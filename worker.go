package stepwise

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// Worker pulls ready workflows from the queue and runs them.
type Worker struct {
	ID string
	e  *Engine

	mu       sync.Mutex
	inflight map[*execution]struct{}
	wake     chan struct{}
	crashed  atomic.Bool // tests only: behave as if the process had been killed
}

func (e *Engine) NewWorker(id string) *Worker {
	if id == "" {
		id = "worker-" + newID()[:6]
	}
	return &Worker{ID: id, e: e, inflight: map[*execution]struct{}{}, wake: make(chan struct{}, 1)}
}

// execution is one attempt by one worker to drive one workflow forward.
type execution struct {
	w       *Worker
	claim   claim
	ctx     context.Context
	cancel  context.CancelFunc
	history map[int]event // what earlier attempts already finished
	seq     int           // index of the next step or timer
}

// Control-flow signals. Workflow code is unwound with panic/recover when it
// must stop without finishing: user code never sees these.
type signal int

const (
	sigAbandon signal = iota + 1 // stop now and write nothing more (shutdown, lost lease)
	sigSuspend                   // parked in the database by Sleep
)

// nondeterminism is raised when a replay does not line up with history.
type nondeterminism struct{ msg string }

func (ex *execution) mustBeAlive() {
	if ex.ctx.Err() != nil || ex.w.crashed.Load() {
		panic(sigAbandon)
	}
}

// mustMatch checks that the code being replayed asks for the same thing that
// history recorded at this position. If someone edited a workflow while
// instances were in flight, this is where it shows up.
func (ex *execution) mustMatch(ev event, kind, name string) {
	if ev.Kind != kind || ev.Name != name {
		panic(nondeterminism{fmt.Sprintf(
			"position %d: history has %s %q but the code asked for %s %q", ev.Seq, ev.Kind, ev.Name, kind, name)})
	}
}

func (ex *execution) record(ev event) {
	if ex.w.crashed.Load() {
		panic(sigAbandon)
	}
	if err := ex.w.e.recordEvent(ex.ctx, ex.claim, ex.w.ID, ev); err != nil {
		// Lease lost, shutting down, or the database is unreachable. In every
		// case the safe move is to stop: whoever claims the workflow next
		// will redo this step.
		panic(sigAbandon)
	}
}

func (ex *execution) wait(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ex.ctx.Done():
		panic(sigAbandon)
	}
}

// Run processes workflows until ctx is cancelled, then shuts down gracefully:
// in-flight workflows are interrupted and handed back to the queue so another
// worker can pick them up at once.
func (w *Worker) Run(ctx context.Context) error {
	execCtx, cancelExecs := context.WithCancel(context.Background())
	defer cancelExecs()
	bg, cancelBg := context.WithCancel(context.Background())
	defer cancelBg()
	go w.heartbeatLoop(bg)
	go w.listenLoop(bg)

	var wg sync.WaitGroup
	ticker := time.NewTicker(w.e.opts.PollInterval)
	defer ticker.Stop()
	names := w.e.names()

	for {
		w.mu.Lock()
		free := w.e.opts.Concurrency - len(w.inflight)
		w.mu.Unlock()

		claimed := 0
		if free > 0 && !w.crashed.Load() {
			claims, err := w.e.claimReady(ctx, w.ID, free, names)
			if err != nil && ctx.Err() == nil {
				slog.Warn("claim failed", "worker", w.ID, "err", err)
			}
			claimed = len(claims)
			for _, c := range claims {
				ex := &execution{w: w, claim: c}
				ex.ctx, ex.cancel = context.WithCancel(execCtx)
				w.mu.Lock()
				w.inflight[ex] = struct{}{}
				w.mu.Unlock()
				wg.Add(1)
				go func() {
					defer wg.Done()
					w.execute(ex)
				}()
			}
		}
		if claimed > 0 && claimed == free {
			continue // the queue may hold more than we had room for; a slot will wake us
		}
		select {
		case <-ctx.Done():
			cancelExecs()
			wg.Wait()
			return nil
		case <-w.wake:
		case <-ticker.C:
		}
	}
}

func (w *Worker) poke() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// execute replays history and then runs the workflow function live.
func (w *Worker) execute(ex *execution) {
	defer func() {
		ex.cancel()
		w.mu.Lock()
		delete(w.inflight, ex)
		w.mu.Unlock()
		w.poke() // a slot is free: go look for more work
	}()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		switch v := r.(type) {
		case signal:
			if v == sigAbandon && !w.crashed.Load() {
				// Best effort. If this fails the lease simply runs out.
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				w.e.release(ctx, ex.claim)
			}
		case nondeterminism:
			w.finish(ex, nil, fmt.Errorf("non-deterministic workflow: %s", v.msg))
		default:
			w.finish(ex, nil, fmt.Errorf("workflow panicked: %v", v))
		}
	}()

	// Epoch 1 means nobody has ever run this workflow, so there is no history to load.
	ex.history = map[int]event{}
	if ex.claim.Epoch > 1 {
		h, err := w.e.loadHistory(ex.ctx, ex.claim.ID)
		if err != nil {
			panic(sigAbandon)
		}
		ex.history = h
	}
	fn := w.e.lookup(ex.claim.Name)
	out, err := fn(&Context{ex: ex}, json.RawMessage(ex.claim.Input))
	ex.mustBeAlive()
	w.finish(ex, out, err)
}

func (w *Worker) finish(ex *execution, out any, wfErr error) {
	if w.crashed.Load() {
		return
	}
	status, data := "completed", []byte(nil)
	var errMsg *string
	if wfErr == nil {
		var err error
		if data, err = json.Marshal(out); err != nil {
			wfErr = fmt.Errorf("output is not JSON-serialisable: %w", err)
		}
	}
	if wfErr != nil {
		msg := wfErr.Error()
		status, data, errMsg = "failed", nil, &msg
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.e.finish(ctx, ex.claim, status, data, errMsg); err != nil && err != ErrLeaseLost {
		slog.Warn("could not record workflow result; it will be retried after the lease expires",
			"workflow", ex.claim.ID, "err", err)
	}
}

// heartbeatLoop renews the leases of everything in flight. A workflow whose
// lease could not be renewed has been taken over, so its execution is cancelled.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.e.opts.Lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if w.crashed.Load() {
			return
		}
		w.mu.Lock()
		execs := make([]*execution, 0, len(w.inflight))
		ids := make([]string, 0, len(w.inflight))
		for ex := range w.inflight {
			execs = append(execs, ex)
			ids = append(ids, ex.claim.ID)
		}
		w.mu.Unlock()
		if len(ids) == 0 {
			continue
		}
		owned, err := w.e.heartbeat(ctx, w.ID, ids)
		if err != nil {
			continue // database hiccup: try again next tick
		}
		for _, ex := range execs {
			if !owned[ex.claim.ID] {
				ex.cancel()
			}
		}
	}
}

// listenLoop wakes the worker the moment a workflow is started, instead of
// waiting for the next poll.
func (w *Worker) listenLoop(ctx context.Context) {
	for ctx.Err() == nil {
		func() {
			conn, err := pgx.Connect(ctx, w.e.dsn)
			if err != nil {
				return
			}
			defer conn.Close(context.Background())
			if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
				return
			}
			for {
				if _, err := conn.WaitForNotification(ctx); err != nil {
					return
				}
				w.poke()
			}
		}()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second): // connection dropped: polling covers us meanwhile
		}
	}
}

// crash makes the worker behave as if its process had been killed: it stops
// claiming, renewing and writing, and does not hand anything back. Tests use
// it; the chaos command kills real processes instead.
func (w *Worker) crash() {
	w.crashed.Store(true)
	w.mu.Lock()
	for ex := range w.inflight {
		ex.cancel()
	}
	w.mu.Unlock()
}
