package stepwise

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Context is handed to workflow code. It is how a workflow runs steps and timers.
type Context struct {
	ex *execution
}

// WorkflowID returns the id of the running workflow.
func (c *Context) WorkflowID() string { return c.ex.claim.ID }

// StepInfo is passed to every step function.
type StepInfo struct {
	WorkflowID string
	Seq        int // position of this step in the workflow, starting at 0
	Attempt    int // 1 on the first try
	// IdempotencyKey is the same on every execution of this step, including
	// re-executions after a crash. Pass it to external systems (payment APIs,
	// email providers) so they can drop duplicates.
	IdempotencyKey string
}

// StepError is what a workflow sees when a step has failed for good. It is
// recorded in history, so a replay returns exactly the same error.
type StepError struct {
	Step    string
	Message string
}

func (e *StepError) Error() string { return fmt.Sprintf("step %q failed: %s", e.Step, e.Message) }

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks an error as not worth retrying (a declined card, bad input).
func Permanent(err error) error { return &permanentError{err} }

type stepConfig struct {
	maxAttempts int
	backoff     time.Duration
}

type StepOption func(*stepConfig)

// Retry sets how many times a step is tried and the first backoff delay,
// which doubles after each failure (capped at 5s).
func Retry(maxAttempts int, backoff time.Duration) StepOption {
	return func(c *stepConfig) { c.maxAttempts, c.backoff = maxAttempts, backoff }
}

// Step runs fn once and durably saves its result.
//
// When the workflow is replayed after a crash, a step that already has a
// saved result does not run again: Step returns the saved value immediately.
//
// The guarantee is "result recorded exactly once, side effect attempted at
// least once". If the process dies after fn's side effect happened but before
// the result was saved, fn runs again on recovery. Use info.IdempotencyKey to
// make that second run harmless.
func Step[T any](c *Context, name string, fn func(ctx context.Context, info StepInfo) (T, error), opts ...StepOption) (T, error) {
	var zero T
	ex := c.ex
	seq := ex.seq
	ex.seq++

	// Replay path: this step finished in an earlier run.
	if ev, ok := ex.history[seq]; ok {
		ex.mustMatch(ev, "step", name)
		if ev.Error != nil {
			return zero, &StepError{Step: name, Message: *ev.Error}
		}
		var out T
		if err := json.Unmarshal(ev.Result, &out); err != nil {
			panic(nondeterminism{fmt.Sprintf("step %d (%s): saved result does not fit the current type: %v", seq, name, err)})
		}
		return out, nil
	}

	// Live path: run it, with retries, then save the outcome.
	cfg := stepConfig{maxAttempts: 3, backoff: 100 * time.Millisecond}
	for _, o := range opts {
		o(&cfg)
	}
	info := StepInfo{WorkflowID: ex.claim.ID, Seq: seq, IdempotencyKey: fmt.Sprintf("%s:%d", ex.claim.ID, seq)}
	for attempt := 1; ; attempt++ {
		ex.mustBeAlive()
		info.Attempt = attempt
		out, err := callStep(ex.ctx, info, fn)
		var data []byte
		if err == nil {
			if data, err = json.Marshal(out); err != nil {
				err = Permanent(fmt.Errorf("result is not JSON-serialisable: %w", err))
			}
		}
		if err == nil {
			ex.record(event{Seq: seq, Kind: "step", Name: name, Attempts: attempt, Result: data})
			// Return what a replay would return, not the in-memory value, so
			// the first run and every replay see identical data.
			var saved T
			if err := json.Unmarshal(data, &saved); err != nil {
				panic(nondeterminism{fmt.Sprintf("step %d (%s): result does not survive a JSON round trip: %v", seq, name, err)})
			}
			return saved, nil
		}
		// The worker is shutting down or lost its lease. That is not the
		// step's fault, so do not count it as a failure.
		ex.mustBeAlive()

		var perm *permanentError
		if attempt >= cfg.maxAttempts || errors.As(err, &perm) {
			msg := err.Error()
			ex.record(event{Seq: seq, Kind: "step", Name: name, Attempts: attempt, Error: &msg})
			return zero, &StepError{Step: name, Message: msg}
		}
		delay := cfg.backoff << (attempt - 1)
		if delay > 5*time.Second || delay <= 0 {
			delay = 5 * time.Second
		}
		ex.wait(delay)
	}
}

// callStep runs fn and turns a panic inside it into an ordinary, retryable error.
func callStep[T any](ctx context.Context, info StepInfo, fn func(context.Context, StepInfo) (T, error)) (out T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx, info)
}

// Sleep is a durable timer. The wake-up time is saved the first time Sleep is
// reached, so a workflow that sleeps for a day and is restarted after 23
// hours waits one more hour, not another day.
//
// Long sleeps do not hold a worker: the workflow is parked in the database
// and the function is unwound. When the time comes, any worker claims it and
// replays up to this point.
func (c *Context) Sleep(d time.Duration) {
	ex := c.ex
	seq := ex.seq
	ex.seq++

	var fireAt time.Time
	if ev, ok := ex.history[seq]; ok {
		ex.mustMatch(ev, "timer", "sleep")
		fireAt = *ev.FireAt
	} else {
		ex.mustBeAlive()
		fireAt = time.Now().Add(d)
		ex.record(event{Seq: seq, Kind: "timer", Name: "sleep", Attempts: 1, FireAt: &fireAt})
	}

	remaining := time.Until(fireAt)
	if remaining <= 0 {
		return
	}
	if remaining <= ex.w.e.opts.InlineSleep {
		ex.wait(remaining)
		return
	}
	if err := ex.w.e.suspend(ex.ctx, ex.claim, fireAt); err != nil {
		panic(sigAbandon)
	}
	panic(sigSuspend)
}
