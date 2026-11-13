// Package stepwise is a small durable workflow engine backed by PostgreSQL.
//
// A workflow is an ordinary Go function made of steps. The engine saves the
// result of every step before moving on, so if the process dies half-way, a
// different worker re-runs the function, skips the steps that already have a
// saved result, and carries on from where the first one stopped.
package stepwise

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WorkflowFunc is the code of a workflow. It must be deterministic: given the
// same step results it must call the same steps in the same order. Anything
// that is not (time, randomness, I/O) belongs inside a Step.
type WorkflowFunc func(c *Context, input json.RawMessage) (any, error)

// Typed adapts a function with concrete input and output types.
func Typed[In, Out any](fn func(c *Context, in In) (Out, error)) WorkflowFunc {
	return func(c *Context, raw json.RawMessage) (any, error) {
		var in In
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, fmt.Errorf("decode input: %w", err)
		}
		return fn(c, in)
	}
}

type Options struct {
	// Lease is how long a worker owns a workflow without renewing. It is also
	// the worst-case time to notice a dead worker.
	Lease time.Duration
	// PollInterval is the fallback polling period. Workers are normally woken
	// by LISTEN/NOTIFY; polling covers timers, expired leases and lost notifications.
	PollInterval time.Duration
	// Concurrency is how many workflows one worker runs at once.
	Concurrency int
	// InlineSleep: sleeps shorter than this wait in memory instead of parking
	// the workflow in the database.
	InlineSleep time.Duration
}

func (o *Options) defaults() {
	if o.Lease == 0 {
		o.Lease = 10 * time.Second
	}
	if o.PollInterval == 0 {
		o.PollInterval = 250 * time.Millisecond
	}
	if o.Concurrency == 0 {
		o.Concurrency = 16
	}
	if o.InlineSleep == 0 {
		o.InlineSleep = time.Second
	}
}

type Engine struct {
	pool *pgxpool.Pool
	dsn  string
	opts Options

	mu       sync.RWMutex
	registry map[string]WorkflowFunc
}

const notifyChannel = "stepwise_wake"

// New connects to PostgreSQL and creates the tables if they do not exist.
func New(ctx context.Context, dsn string, opts Options) (*Engine, error) {
	opts.defaults()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if min := int32(opts.Concurrency + 8); cfg.MaxConns < min {
		cfg.MaxConns = min
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Two processes may start at once; serialise schema creation.
	if _, err := pool.Exec(ctx, `BEGIN; SELECT pg_advisory_xact_lock(7310); `+schema+`; COMMIT`); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Engine{pool: pool, dsn: dsn, opts: opts, registry: map[string]WorkflowFunc{}}, nil
}

func (e *Engine) Close()              { e.pool.Close() }
func (e *Engine) Pool() *pgxpool.Pool { return e.pool }

// Register makes a workflow runnable by workers created from this engine.
func (e *Engine) Register(name string, fn WorkflowFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.registry[name] = fn
}

func (e *Engine) names() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.registry))
	for n := range e.registry {
		out = append(out, n)
	}
	return out
}

func (e *Engine) lookup(name string) WorkflowFunc {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.registry[name]
}

func newID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Start enqueues a workflow and returns its id.
//
// If id is non-empty it works as an idempotency key: starting the same id
// twice creates one workflow, so a client that retries a timed-out request
// cannot start the same order twice.
func (e *Engine) Start(ctx context.Context, name string, input any, id string) (string, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	if id == "" {
		id = newID()
	}
	_, err = e.pool.Exec(ctx, `
		WITH ins AS (
			INSERT INTO workflows (id, name, input) VALUES ($1, $2, $3::jsonb)
			ON CONFLICT (id) DO NOTHING
		)
		SELECT pg_notify($4, '')`,
		id, name, data, notifyChannel)
	return id, err
}

// Workflow is a snapshot of one workflow's state.
type Workflow struct {
	ID          string
	Name        string
	Status      string
	Input       json.RawMessage
	Output      json.RawMessage
	Error       string
	Epoch       int64 // number of times a worker has claimed it
	CreatedAt   time.Time
	CompletedAt *time.Time
}

func (w *Workflow) Done() bool { return w.Status == "completed" || w.Status == "failed" }

var ErrNotFound = errors.New("stepwise: workflow not found")

func (e *Engine) Get(ctx context.Context, id string) (*Workflow, error) {
	var w Workflow
	var in, out []byte
	var errMsg *string
	err := e.pool.QueryRow(ctx, `
		SELECT id, name, status, input, output, error, epoch, created_at, completed_at
		FROM workflows WHERE id = $1`, id).
		Scan(&w.ID, &w.Name, &w.Status, &in, &out, &errMsg, &w.Epoch, &w.CreatedAt, &w.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	w.Input, w.Output = in, out
	if errMsg != nil {
		w.Error = *errMsg
	}
	return &w, nil
}

// Wait blocks until the workflow has completed or failed.
func (e *Engine) Wait(ctx context.Context, id string) (*Workflow, error) {
	for {
		w, err := e.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if w.Done() {
			return w, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
