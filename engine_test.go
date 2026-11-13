package stepwise

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are integration tests against a real PostgreSQL.
// Start one with scripts/dev-db.sh, or point STEPWISE_TEST_DSN at your own.

func testDSN() string {
	if dsn := os.Getenv("STEPWISE_TEST_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://localhost:5433/stepwise_test?sslmode=disable"
}

func newEngine(t *testing.T, opts Options) *Engine {
	t.Helper()
	if opts.Lease == 0 {
		opts.Lease = 600 * time.Millisecond
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 20 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e, err := New(ctx, testDSN(), opts)
	if err != nil {
		t.Skipf("no test database (%v); run scripts/dev-db.sh", err)
	}
	if _, err := e.pool.Exec(ctx, "TRUNCATE workflows CASCADE"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

// runWorker starts a worker and returns it with a function that stops it gracefully.
func runWorker(t *testing.T, e *Engine, id string) (*Worker, func()) {
	t.Helper()
	w := e.NewWorker(id)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return w, stop
}

func wait(t *testing.T, e *Engine, id string) *Workflow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatalf("waiting for %s: %v", id, err)
	}
	return w
}

func eventCount(t *testing.T, e *Engine, id string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), "SELECT count(*) FROM events WHERE workflow_id = $1", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRunsStepsInOrderAndReturnsOutput(t *testing.T) {
	e := newEngine(t, Options{})
	e.Register("sum", Typed(func(c *Context, in []int) (int, error) {
		total := 0
		for i, n := range in {
			v, err := Step(c, fmt.Sprintf("add-%d", i), func(context.Context, StepInfo) (int, error) { return total + n, nil })
			if err != nil {
				return 0, err
			}
			total = v
		}
		return total, nil
	}))
	runWorker(t, e, "w1")
	id, _ := e.Start(context.Background(), "sum", []int{1, 2, 3, 4}, "")
	w := wait(t, e, id)
	if w.Status != "completed" || string(w.Output) != "10" {
		t.Fatalf("status=%s output=%s error=%s", w.Status, w.Output, w.Error)
	}
	if n := eventCount(t, e, id); n != 4 {
		t.Fatalf("want 4 events, got %d", n)
	}
}

func TestStartIsIdempotentByID(t *testing.T) {
	e := newEngine(t, Options{})
	var runs atomic.Int32
	e.Register("once", func(c *Context, _ json.RawMessage) (any, error) {
		runs.Add(1)
		return "ok", nil
	})
	for i := 0; i < 5; i++ {
		if _, err := e.Start(context.Background(), "once", nil, "order-42"); err != nil {
			t.Fatal(err)
		}
	}
	runWorker(t, e, "w1")
	wait(t, e, "order-42")
	time.Sleep(100 * time.Millisecond)
	if runs.Load() != 1 {
		t.Fatalf("workflow ran %d times", runs.Load())
	}
}

// The core promise: kill a worker mid-workflow and another one finishes the
// job without redoing the steps that were already saved.
func TestCrashedWorkerIsReplacedAndFinishedStepsAreNotRerun(t *testing.T) {
	e := newEngine(t, Options{})
	var execs [3]atomic.Int32
	reachedStep2 := make(chan struct{})
	var once sync.Once
	e.Register("order", func(c *Context, _ json.RawMessage) (any, error) {
		a, _ := Step(c, "charge", func(context.Context, StepInfo) (string, error) {
			execs[0].Add(1)
			return "charged", nil
		})
		b, _ := Step(c, "reserve", func(ctx context.Context, _ StepInfo) (string, error) {
			if execs[1].Add(1) == 1 { // first run: hang until the worker dies
				once.Do(func() { close(reachedStep2) })
				<-ctx.Done()
				return "", ctx.Err()
			}
			return "reserved", nil
		})
		d, _ := Step(c, "ship", func(context.Context, StepInfo) (string, error) {
			execs[2].Add(1)
			return "shipped", nil
		})
		return a + "," + b + "," + d, nil
	})

	w1, _ := runWorker(t, e, "w1")
	id, _ := e.Start(context.Background(), "order", nil, "")
	<-reachedStep2
	w1.crash() // w1 now holds a lease it will never renew or release

	runWorker(t, e, "w2")
	w := wait(t, e, id)
	if w.Status != "completed" || string(w.Output) != `"charged,reserved,shipped"` {
		t.Fatalf("status=%s output=%s error=%s", w.Status, w.Output, w.Error)
	}
	if got := [3]int32{execs[0].Load(), execs[1].Load(), execs[2].Load()}; got != [3]int32{1, 2, 1} {
		t.Fatalf("step executions = %v, want [1 2 1] (charge must not run twice)", got)
	}
	if w.Epoch != 2 {
		t.Fatalf("epoch = %d, want 2 (claimed once by each worker)", w.Epoch)
	}
	var from, to string
	if err := e.pool.QueryRow(context.Background(),
		`SELECT from_worker, to_worker FROM takeovers WHERE workflow_id = $1`, id).Scan(&from, &to); err != nil || from != "w1" || to != "w2" {
		t.Fatalf("takeover log = %q -> %q (err %v), want w1 -> w2", from, to, err)
	}
}

func TestGracefulStopHandsWorkBackImmediately(t *testing.T) {
	e := newEngine(t, Options{Lease: 30 * time.Second}) // long lease: only a release can explain a fast takeover
	started := make(chan struct{}, 2)
	var first atomic.Bool
	e.Register("slow", func(c *Context, _ json.RawMessage) (any, error) {
		return Step(c, "work", func(ctx context.Context, _ StepInfo) (string, error) {
			started <- struct{}{}
			if first.CompareAndSwap(false, true) {
				<-ctx.Done()
				return "", ctx.Err()
			}
			return "done", nil
		})
	})
	_, stop := runWorker(t, e, "w1")
	id, _ := e.Start(context.Background(), "slow", nil, "")
	<-started
	stop()
	runWorker(t, e, "w2")
	begin := time.Now()
	if w := wait(t, e, id); w.Status != "completed" {
		t.Fatalf("status=%s error=%s", w.Status, w.Error)
	}
	if took := time.Since(begin); took > 5*time.Second {
		t.Fatalf("takeover took %v; the lease was not released", took)
	}
	var n int
	e.pool.QueryRow(context.Background(), `SELECT count(*) FROM takeovers WHERE workflow_id = $1`, id).Scan(&n)
	if n != 0 {
		t.Fatalf("a graceful hand-back was logged as a takeover (%d rows)", n)
	}
}

// A worker that was replaced must not be able to write, even if it is still running.
func TestFencingRejectsWritesFromAReplacedWorker(t *testing.T) {
	e := newEngine(t, Options{Lease: 50 * time.Millisecond})
	e.Register("wf", func(*Context, json.RawMessage) (any, error) { return nil, nil })
	ctx := context.Background()
	id, _ := e.Start(ctx, "wf", nil, "")

	a, err := e.claimReady(ctx, "A", 1, []string{"wf"})
	if err != nil || len(a) != 1 {
		t.Fatalf("claim A: %v %v", a, err)
	}
	if again, _ := e.claimReady(ctx, "B", 1, []string{"wf"}); len(again) != 0 {
		t.Fatal("B claimed a workflow whose lease is still live")
	}
	time.Sleep(80 * time.Millisecond) // A stalls; its lease runs out
	b, _ := e.claimReady(ctx, "B", 1, []string{"wf"})
	if len(b) != 1 || b[0].Epoch != a[0].Epoch+1 {
		t.Fatalf("claim B: %v", b)
	}

	// A wakes up and tries to carry on.
	if err := e.recordEvent(ctx, a[0], "A", event{Seq: 0, Kind: "step", Name: "x", Attempts: 1, Result: []byte(`1`)}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale recordEvent: got %v, want ErrLeaseLost", err)
	}
	if err := e.finish(ctx, a[0], "completed", []byte(`1`), nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale finish: got %v, want ErrLeaseLost", err)
	}
	if n := eventCount(t, e, id); n != 0 {
		t.Fatalf("stale worker wrote %d events", n)
	}
	// The real owner is unaffected.
	if err := e.recordEvent(ctx, b[0], "B", event{Seq: 0, Kind: "step", Name: "x", Attempts: 1, Result: []byte(`2`)}); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentWorkersNeverRunTheSameWorkflowTwice(t *testing.T) {
	e := newEngine(t, Options{Lease: 5 * time.Second, Concurrency: 8})
	var mu sync.Mutex
	runs := map[string]int{}
	e.Register("count", func(c *Context, _ json.RawMessage) (any, error) {
		return Step(c, "s", func(_ context.Context, info StepInfo) (int, error) {
			mu.Lock()
			runs[info.WorkflowID]++
			mu.Unlock()
			return 1, nil
		})
	})
	const n = 200
	ids := make([]string, n)
	for i := range ids {
		ids[i], _ = e.Start(context.Background(), "count", nil, "")
	}
	for i := 0; i < 4; i++ {
		runWorker(t, e, fmt.Sprintf("w%d", i))
	}
	for _, id := range ids {
		if w := wait(t, e, id); w.Status != "completed" || w.Epoch != 1 {
			t.Fatalf("%s: status=%s epoch=%d", id, w.Status, w.Epoch)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for id, c := range runs {
		if c != 1 {
			t.Fatalf("workflow %s ran its step %d times", id, c)
		}
	}
	if len(runs) != n {
		t.Fatalf("%d workflows ran, want %d", len(runs), n)
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	e := newEngine(t, Options{})
	var calls atomic.Int32
	e.Register("flaky", func(c *Context, _ json.RawMessage) (any, error) {
		return Step(c, "call-api", func(_ context.Context, info StepInfo) (int, error) {
			if calls.Add(1) < 3 {
				return 0, errors.New("timeout")
			}
			return info.Attempt, nil
		}, Retry(5, time.Millisecond))
	})
	runWorker(t, e, "w1")
	id, _ := e.Start(context.Background(), "flaky", nil, "")
	if w := wait(t, e, id); w.Status != "completed" || string(w.Output) != "3" {
		t.Fatalf("status=%s output=%s", w.Status, w.Output)
	}
}

// A step that fails for good surfaces as an error the workflow can handle,
// which is how compensation ("saga") logic is written.
func TestPermanentFailureTriggersCompensation(t *testing.T) {
	e := newEngine(t, Options{})
	var shipCalls, refunds atomic.Int32
	e.Register("order", func(c *Context, _ json.RawMessage) (any, error) {
		Step(c, "charge", func(context.Context, StepInfo) (bool, error) { return true, nil })
		_, err := Step(c, "ship", func(context.Context, StepInfo) (bool, error) {
			shipCalls.Add(1)
			return false, Permanent(errors.New("address invalid"))
		}, Retry(5, time.Millisecond))
		if err != nil {
			Step(c, "refund", func(context.Context, StepInfo) (bool, error) {
				refunds.Add(1)
				return true, nil
			})
			return nil, fmt.Errorf("order cancelled and refunded: %w", err)
		}
		return "shipped", nil
	})
	runWorker(t, e, "w1")
	id, _ := e.Start(context.Background(), "order", nil, "")
	w := wait(t, e, id)
	if w.Status != "failed" || !strings.Contains(w.Error, "address invalid") {
		t.Fatalf("status=%s error=%q", w.Status, w.Error)
	}
	if shipCalls.Load() != 1 || refunds.Load() != 1 {
		t.Fatalf("ship calls=%d (permanent errors must not retry), refunds=%d", shipCalls.Load(), refunds.Load())
	}
}

func TestSleepParksTheWorkflowAndSurvivesAWorkerChange(t *testing.T) {
	e := newEngine(t, Options{InlineSleep: 10 * time.Millisecond})
	var before, after atomic.Int32
	e.Register("nap", func(c *Context, _ json.RawMessage) (any, error) {
		Step(c, "before", func(context.Context, StepInfo) (int, error) { return int(before.Add(1)), nil })
		c.Sleep(400 * time.Millisecond)
		Step(c, "after", func(context.Context, StepInfo) (int, error) { return int(after.Add(1)), nil })
		return "awake", nil
	})
	_, stop := runWorker(t, e, "w1")
	begin := time.Now()
	id, _ := e.Start(context.Background(), "nap", nil, "")

	// Wait until it is parked, then take the worker away.
	deadline := time.Now().Add(3 * time.Second)
	for {
		w, _ := e.Get(context.Background(), id)
		if w.Status == "sleeping" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never went to sleep; status=%s", w.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	runWorker(t, e, "w2")

	w := wait(t, e, id)
	took := time.Since(begin)
	if w.Status != "completed" || before.Load() != 1 || after.Load() != 1 {
		t.Fatalf("status=%s before=%d after=%d", w.Status, before.Load(), after.Load())
	}
	if took < 400*time.Millisecond || took > 3*time.Second {
		t.Fatalf("slept %v, want about 400ms (the timer must not restart)", took)
	}
}

func TestChangedWorkflowCodeIsDetectedOnReplay(t *testing.T) {
	e := newEngine(t, Options{})
	ctx := context.Background()
	e.Register("wf", func(c *Context, _ json.RawMessage) (any, error) {
		return Step(c, "new-name", func(context.Context, StepInfo) (int, error) { return 1, nil })
	})
	// Fake an earlier run by a previous version of the code that called the step "old-name".
	id, _ := e.Start(ctx, "wf", nil, "")
	c, _ := e.claimReady(ctx, "old", 1, []string{"wf"})
	if err := e.recordEvent(ctx, c[0], "old", event{Seq: 0, Kind: "step", Name: "old-name", Attempts: 1, Result: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	e.release(ctx, c[0])

	runWorker(t, e, "w1")
	w := wait(t, e, id)
	if w.Status != "failed" || !strings.Contains(w.Error, "non-deterministic") {
		t.Fatalf("status=%s error=%q", w.Status, w.Error)
	}
}
