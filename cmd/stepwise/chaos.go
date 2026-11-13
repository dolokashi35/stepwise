package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dolokashi35/stepwise"
	"github.com/jackc/pgx/v5"
)

const chaosWorkflow = "chaos-order"

// runChaos starts a batch of orders and attacks the system while they run:
//
//   - worker processes are killed with SIGKILL at random,
//   - optionally PostgreSQL itself is crashed and restarted,
//   - a share of the orders fail for good at shipping and must be refunded.
//
// Then it checks from the database that nothing was lost, duplicated,
// reordered or corrupted.
func runChaos(args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "PostgreSQL connection string")
	total := fs.Int("workflows", 1000, "orders to run")
	workers := fs.Int("workers", 4, "worker processes")
	concurrency := fs.Int("concurrency", 16, "workflows per worker")
	killEvery := fs.Duration("kill-every", 400*time.Millisecond, "how often a random worker is killed (0 = never)")
	stallEvery := fs.Duration("stall-every", 0, "how often a random worker is frozen with SIGSTOP (0 = never)")
	stallFor := fs.Duration("stall-for", 3*time.Second, "how long a frozen worker stays frozen (longer than the lease, so it is replaced)")
	dbCrashEvery := fs.Duration("db-crash-every", 0, "how often PostgreSQL is crashed and restarted (0 = never; local server only)")
	dbDown := fs.Duration("db-down", 2*time.Second, "how long PostgreSQL stays down after each crash")
	lease := fs.Duration("lease", time.Second, "lease duration")
	stepDelay := fs.Duration("step-delay", 20*time.Millisecond, "how long each step takes")
	flaky := fs.Int("flaky-pct", 5, "chance a step's first attempt fails")
	refundPct := fs.Int("refund-pct", 5, "share of orders whose shipping fails for good, so they must be refunded")
	timeout := fs.Duration("timeout", 10*time.Minute, "give up after this long")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	e, err := stepwise.New(ctx, *dsn, stepwise.Options{})
	if err != nil {
		return fmt.Errorf("cannot reach PostgreSQL (%w). Start it with scripts/dev-db.sh", err)
	}
	defer e.Close()
	pool := e.Pool()
	if _, err := pool.Exec(ctx, effectsSchema); err != nil {
		return err
	}
	var server *pgServer
	if *dbCrashEvery > 0 {
		if server, err = findPGServer(ctx, pool); err != nil {
			return err
		}
	}

	prefix := fmt.Sprintf("chaos-%d-", time.Now().UnixNano())
	like := prefix + "%"
	mustRefund := func(i int) bool { return i%100 < *refundPct }
	refunds := 0
	for i := 0; i < *total; i++ {
		if mustRefund(i) {
			refunds++
		}
	}

	sup := &Supervisor{
		WorkerArgs:   []string{"--dsn", *dsn, "--concurrency", strconv.Itoa(*concurrency), "--lease", lease.String(), "--workflow", chaosWorkflow},
		RestartDelay: 150 * time.Millisecond,
		Quiet:        true,
	}
	if err := sup.Start(*workers); err != nil {
		return err
	}
	defer sup.Stop()

	fmt.Printf("starting %d orders (%d steps each, %d of them must be refunded) on %d workers\n",
		*total, len(orderSteps), refunds, *workers)
	if *killEvery > 0 {
		fmt.Printf("  killing a random worker every %s\n", *killEvery)
	}
	if *stallEvery > 0 {
		fmt.Printf("  freezing a random worker for %s every %s (lease is %s)\n", *stallFor, *stallEvery, *lease)
	}
	if server != nil {
		fmt.Printf("  crashing PostgreSQL every %s and keeping it down for %s\n", *dbCrashEvery, *dbDown)
	}
	begin := time.Now()

	// Start the orders. If the database is down, keep trying: Start is
	// idempotent by id, so retrying a call that may have gone through is safe.
	var wg sync.WaitGroup
	jobs := make(chan int)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				id := fmt.Sprintf("%s%d", prefix, i)
				in := OrderInput{OrderID: id, DelayMs: int(stepDelay.Milliseconds()), FlakyPct: *flaky, FailShip: mustRefund(i)}
				for ctx.Err() == nil {
					if _, err := e.Start(ctx, chaosWorkflow, in, id); err == nil {
						break
					}
					sleepCtx(ctx, 100*time.Millisecond)
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for i := 0; i < *total; i++ {
			select {
			case jobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	tick := func(d time.Duration) <-chan time.Time {
		if d <= 0 {
			return nil // a nil channel never fires
		}
		return time.NewTicker(d).C
	}
	killer, staller, crasher := tick(*killEvery), tick(*stallEvery), tick(*dbCrashEvery)
	progress := time.NewTicker(time.Second)
	defer progress.Stop()
	deadline := time.After(*timeout)

	var dbCrashes atomic.Int32
	var crashing atomic.Bool
	var crashWG sync.WaitGroup
	crashErr := make(chan error, 1)
	done := 0
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case err := <-crashErr:
			return err
		case <-deadline:
			fmt.Printf("TIMEOUT: only %d of %d orders finished\n", done, *total)
			break loop
		case <-killer:
			sup.KillRandom()
		case <-staller:
			sup.StallRandom(*stallFor)
		case <-crasher:
			if !crashing.CompareAndSwap(false, true) {
				continue
			}
			crashWG.Add(1)
			go func() {
				defer crashWG.Done()
				defer crashing.Store(false)
				dbCrashes.Add(1)
				if err := server.Crash(*dbDown); err != nil {
					select {
					case crashErr <- err:
					default:
					}
				}
			}()
		case <-progress.C:
			qctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			err := pool.QueryRow(qctx,
				`SELECT count(*) FROM workflows WHERE id LIKE $1 AND status IN ('completed', 'failed')`, like).Scan(&done)
			cancel()
			line := fmt.Sprintf("  %5.1fs  %d/%d finished, %d workers killed", time.Since(begin).Seconds(), done, *total, sup.Kills())
			if *stallEvery > 0 {
				line += fmt.Sprintf(", %d frozen", sup.Stalls())
			}
			if server != nil {
				line += fmt.Sprintf(", %d database crashes", dbCrashes.Load())
			}
			if err != nil {
				line += "   (database is down)"
			}
			fmt.Println(line)
			if err == nil && done >= *total {
				break loop
			}
		}
	}
	// Never leave the database down, whatever happened above.
	crashWG.Wait()
	if server != nil {
		pool.Reset() // drop connections that died with the old server
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	wg.Wait()
	elapsed := time.Since(begin)
	kills, stalls := sup.Kills(), sup.Stalls()
	sup.Stop()

	// ---- verification: everything below is read back from the database ----
	var wrongStatus, badHistory, badEffects, applied, executions, claims int
	q := func(dst *int, sql string, args ...any) {
		if err == nil {
			err = pool.QueryRow(ctx, sql, args...).Scan(dst)
		}
	}
	isRefund := `(w.input ->> 'FailShip')::bool`
	okSteps, okFailed := orderSteps, []bool{false, false, false, false}
	rfSteps, rfFailed := refundHistory, []bool{false, false, true, false}

	q(&wrongStatus, `
		SELECT count(*) FROM workflows w WHERE w.id LIKE $1
		AND w.status <> CASE WHEN `+isRefund+` THEN 'failed' ELSE 'completed' END`, like)
	// History: the right steps, in order, each recorded once, and only the
	// shipping step of a refund order recorded as failed.
	q(&badHistory, `
		SELECT count(*) FROM workflows w
		LEFT JOIN LATERAL (
			SELECT array_agg(name ORDER BY seq) AS names, array_agg(error IS NOT NULL ORDER BY seq) AS failed
			FROM events ev WHERE ev.workflow_id = w.id
		) h ON true
		WHERE w.id LIKE $1 AND NOT COALESCE(
			CASE WHEN `+isRefund+` THEN h.names = $4::text[] AND h.failed = $5::bool[]
			     ELSE h.names = $2::text[] AND h.failed = $3::bool[] END, false)`,
		like, okSteps, okFailed, rfSteps, rfFailed)
	// Outside world: the right effects, in order. The primary key on the
	// idempotency key is what makes each of them apply at most once.
	q(&badEffects, `
		SELECT count(*) FROM workflows w
		LEFT JOIN LATERAL (
			SELECT array_agg(step ORDER BY n) AS steps FROM demo_effects d WHERE d.workflow_id = w.id
		) fx ON true
		WHERE w.id LIKE $1 AND NOT COALESCE(
			fx.steps = CASE WHEN `+isRefund+` THEN $3::text[] ELSE $2::text[] END, false)`,
		like, okSteps, refundEffects)
	q(&applied, `SELECT count(*) FROM demo_effects WHERE workflow_id LIKE $1`, like)
	q(&executions, `SELECT COALESCE(sum(executions), 0) FROM demo_effects WHERE workflow_id LIKE $1`, like)
	q(&claims, `SELECT COALESCE(sum(epoch), 0) FROM workflows WHERE id LIKE $1`, like)
	if err != nil {
		return err
	}

	// Output: recompute each receipt from the order id and compare.
	wrongOutput := 0
	rows, err := pool.Query(ctx, `
		SELECT input ->> 'OrderID', COALESCE(output ->> 'Receipt', '') FROM workflows WHERE id LIKE $1 AND status = 'completed'`, like)
	if err != nil {
		return err
	}
	var orderID, receipt string
	if _, err := pgx.ForEachRow(rows, []any{&orderID, &receipt}, func() error {
		if receipt != expectedReceipt(orderID) {
			wrongOutput++
		}
		return nil
	}); err != nil {
		return err
	}

	// Takeovers: the engine logs one row each time a workflow is claimed
	// after its owner's lease expired. Compare with when each worker process
	// actually died to tell real deaths from false alarms.
	takeovers, falseTakeovers := 0, 0
	rows, err = pool.Query(ctx, `SELECT from_worker, at FROM takeovers WHERE workflow_id LIKE $1`, like)
	if err != nil {
		return err
	}
	var from string
	var at time.Time
	if _, err := pgx.ForEachRow(rows, []any{&from, &at}, func() error {
		takeovers++
		if died, dead := sup.DiedAt(from); !dead || died.After(at) {
			falseTakeovers++
		}
		return nil
	}); err != nil {
		return err
	}

	n := *total
	wantEffects := (n-refunds)*len(okSteps) + refunds*len(refundEffects)
	check := func(ok bool, label string) bool {
		mark := "PASS"
		if !ok {
			mark = "FAIL"
		}
		fmt.Printf("  [%s] %s\n", mark, label)
		return ok
	}
	fmt.Printf("\n%d orders in %.1fs\n", n, elapsed.Seconds())
	fmt.Printf("  %d workers killed (SIGKILL)\n", kills)
	if *stallEvery > 0 {
		fmt.Printf("  %d workers frozen for %s (SIGSTOP, then SIGCONT)\n", stalls, *stallFor)
	}
	if server != nil {
		fmt.Printf("  %d PostgreSQL crashes (immediate shutdown, down %s each)\n", dbCrashes.Load(), *dbDown)
	}
	fmt.Printf("  %d takeovers after a lease expired\n", takeovers)
	fmt.Printf("      %d from a worker that was dead\n", takeovers-falseTakeovers)
	fmt.Printf("      %d from a worker that was still alive (false takeovers; fencing stops the old owner writing)\n", falseTakeovers)
	if handed := claims - n - takeovers; handed > 0 {
		fmt.Printf("  %d times a worker gave an order back after a database error\n", handed)
	}
	fmt.Printf("  %d step executions for %d effects: %d repeats, all absorbed by idempotency keys\n\n",
		executions, applied, executions-applied)

	ok := check(wrongStatus == 0, fmt.Sprintf("no order lost or stuck (%d of %d finished in the right state)", n-wrongStatus, n))
	ok = check(badHistory == 0, fmt.Sprintf("every history has the right steps in order, once each (%d bad)", badHistory)) && ok
	ok = check(badEffects == 0 && applied == wantEffects, fmt.Sprintf("every effect applied exactly once, in order (%d applied, want %d)", applied, wantEffects)) && ok
	ok = check(wrongOutput == 0, fmt.Sprintf("every receipt matches the expected value (%d wrong)", wrongOutput)) && ok
	if refunds > 0 {
		ok = check(wrongStatus == 0 && badHistory == 0 && badEffects == 0,
			fmt.Sprintf("every order that could not ship was refunded exactly once and never shipped (%d orders)", refunds)) && ok
	}

	// Leave the tables as we found them.
	pool.Exec(ctx, `DELETE FROM demo_effects WHERE workflow_id LIKE $1`, like)
	pool.Exec(ctx, `DELETE FROM workflows WHERE id LIKE $1`, like)
	if !ok {
		return fmt.Errorf("chaos test failed")
	}
	return nil
}
