package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dolokashi35/stepwise"
)

// The demo workflow: an order that charges a card, reserves stock, ships and
// emails. Every step writes to demo_effects, a stand-in for the outside world
// (a payment provider, a warehouse). That table is what lets the chaos test
// prove no effect was lost or applied twice.

const effectsSchema = `
BEGIN;
SELECT pg_advisory_xact_lock(7311);
CREATE TABLE IF NOT EXISTS demo_effects (
    key         TEXT PRIMARY KEY,           -- the step's idempotency key
    workflow_id TEXT   NOT NULL,
    step        TEXT   NOT NULL,
    executions  INT    NOT NULL DEFAULT 1,  -- how many times the step body actually ran
    n           BIGSERIAL                   -- order in which effects were first applied
);
CREATE INDEX IF NOT EXISTS demo_effects_wf ON demo_effects (workflow_id);
COMMIT;`

// What a cancelled order must look like: charged, stock reserved, shipping
// refused, card refunded. The refund is the compensation step.
var (
	refundHistory = []string{"charge_card", "reserve_stock", "ship_order", "refund_card"}
	refundEffects = []string{"charge_card", "reserve_stock", "refund_card"}
)

// Eight attempts with doubling backoff ride out about 15 seconds of the
// outside world (here: the database) being unreachable. A longer outage makes
// the step fail for good and the order is compensated.
var orderRetry = stepwise.Retry(8, 150*time.Millisecond)

var orderSteps = []string{"charge_card", "reserve_stock", "ship_order", "send_email"}

type OrderInput struct {
	OrderID  string
	DelayMs  int  // how long each step takes
	FlakyPct int  // chance that a step's first attempt fails with a retryable error
	FailShip bool // shipping fails for good, which triggers a refund
}

type OrderOutput struct {
	Receipt string
}

// chain makes each step's result depend on the one before it, so a replay
// that returned the wrong saved value would produce the wrong receipt.
func chain(prev, step string) string {
	sum := sha256.Sum256([]byte(prev + "|" + step))
	return hex.EncodeToString(sum[:6])
}

func expectedReceipt(orderID string) string {
	v := orderID
	for _, s := range orderSteps {
		v = chain(v, s)
	}
	return v
}

// registerOrder registers the order workflow under the given name. Workers
// only claim names they have registered, so the chaos test uses its own name
// to keep a demo running on the same database from picking up its orders.
func registerOrder(e *stepwise.Engine, name string) {
	pool := e.Pool()

	// effect is the "side effect". The idempotency key is the primary key, so
	// running the step a second time after a crash updates a counter instead
	// of charging the card again.
	effect := func(ctx context.Context, info stepwise.StepInfo, step string, in OrderInput) error {
		half := time.Duration(in.DelayMs) * time.Millisecond / 2
		if err := sleepCtx(ctx, half); err != nil {
			return err
		}
		if info.Attempt == 1 && rand.IntN(100) < in.FlakyPct {
			return errors.New("upstream timed out")
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO demo_effects (key, workflow_id, step) VALUES ($1, $2, $3)
			ON CONFLICT (key) DO UPDATE SET executions = demo_effects.executions + 1`,
			info.IdempotencyKey, info.WorkflowID, step)
		if err != nil {
			return err
		}
		// A crash here, after the effect and before the result is saved, is
		// the case that makes idempotency keys necessary.
		return sleepCtx(ctx, half)
	}

	e.Register(name, stepwise.Typed(func(c *stepwise.Context, in OrderInput) (OrderOutput, error) {
		value := in.OrderID
		for _, name := range orderSteps {
			prev := value
			v, err := stepwise.Step(c, name, func(ctx context.Context, info stepwise.StepInfo) (string, error) {
				if name == "ship_order" && in.FailShip {
					sleepCtx(ctx, time.Duration(in.DelayMs)*time.Millisecond)
					return "", stepwise.Permanent(errors.New("carrier rejected the address"))
				}
				if err := effect(ctx, info, name, in); err != nil {
					return "", err
				}
				return chain(prev, name), nil
			}, orderRetry)
			if err != nil {
				// Compensation: undo what already happened, then give up.
				stepwise.Step(c, "refund_card", func(ctx context.Context, info stepwise.StepInfo) (bool, error) {
					return true, effect(ctx, info, "refund_card", in)
				}, orderRetry)
				return OrderOutput{}, fmt.Errorf("order cancelled and refunded: %w", err)
			}
			value = v
		}
		return OrderOutput{Receipt: value}, nil
	}))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runWorker is one worker process. The demo and chaos commands start several
// of these as child processes so that they can be killed for real.
func runWorker(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "PostgreSQL connection string")
	id := fs.String("id", "", "worker id")
	concurrency := fs.Int("concurrency", 8, "workflows to run at once")
	lease := fs.Duration("lease", 3*time.Second, "lease duration")
	queue := fs.String("workflow", "order", "name to register the order workflow under")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	e, err := stepwise.New(ctx, *dsn, stepwise.Options{Lease: *lease, Concurrency: *concurrency})
	if err != nil {
		return err
	}
	defer e.Close()
	if _, err := e.Pool().Exec(ctx, effectsSchema); err != nil {
		return err
	}
	registerOrder(e, *queue)
	registerBench(e)

	// If the supervisor dies without cleaning up, do not linger as an orphan.
	parent := os.Getppid()
	go func() {
		for range time.Tick(500 * time.Millisecond) {
			if os.Getppid() != parent {
				os.Exit(0)
			}
		}
	}()
	return e.NewWorker(*id).Run(ctx)
}
