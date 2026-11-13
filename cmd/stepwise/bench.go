package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolokashi35/stepwise"
)

// registerBench registers a workflow of n steps that do no work.
func registerBench(e *stepwise.Engine) {
	e.Register("bench", stepwise.Typed(func(c *stepwise.Context, n int) (int, error) {
		sum := 0
		for i := 0; i < n; i++ {
			v, err := stepwise.Step(c, "step", func(context.Context, stepwise.StepInfo) (int, error) { return i, nil })
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	}))
}

// runBench measures two things on this machine:
//
//  1. Throughput: N workflows are queued up front, then one worker drains them.
//  2. Latency: workflows arrive at a fixed rate and we measure, per workflow,
//     the time from Start to completion (both timestamps taken by PostgreSQL).
//
// Steps do no work of their own, so the numbers are the engine's overhead:
// one durable (fsynced) commit per step plus claim and completion.
func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "PostgreSQL connection string")
	total := fs.Int("workflows", 5000, "workflows per throughput run")
	steps := fs.Int("steps", 4, "steps per workflow")
	levels := fs.String("concurrency", "1,4,16,64", "worker concurrency levels to try")
	procs := fs.String("workers", "1,2,4,8", "numbers of worker processes to try in the scaling runs")
	procConc := fs.Int("worker-concurrency", 4, "workflows per worker process in the scaling runs")
	scaleTotal := fs.Int("scale-workflows", 20000, "workflows per scaling run")
	rates := fs.String("rates", "100,400", "arrival rates (workflows/sec) for the latency runs")
	duration := fs.Duration("duration", 10*time.Second, "length of each latency run")
	fs.Parse(args)
	ctx := context.Background()
	run := time.Now().UnixNano()

	engine := func(concurrency int) (*stepwise.Engine, error) {
		e, err := stepwise.New(ctx, *dsn, stepwise.Options{Concurrency: concurrency, Lease: 30 * time.Second})
		if err != nil {
			return nil, fmt.Errorf("cannot reach PostgreSQL (%w). Start it with scripts/dev-db.sh", err)
		}
		registerBench(e)
		// Start every run from the same physical state. Earlier runs (and the
		// chaos test) delete thousands of rows, which leaves the indexes mostly
		// empty but still large. With stale row counts on top, the planner
		// decides a primary-key lookup is dearer than scanning the whole table
		// and every statement slows down in proportion to the queue length.
		for _, q := range []string{
			"VACUUM workflows", "VACUUM events",
			"REINDEX TABLE workflows", "REINDEX TABLE events",
			"ANALYZE workflows", "ANALYZE events",
		} {
			if _, err := e.Pool().Exec(ctx, q); err != nil {
				return nil, err
			}
		}
		return e, nil
	}
	completed := func(e *stepwise.Engine, like string) (n int) {
		e.Pool().QueryRow(ctx, `SELECT count(*) FROM workflows WHERE id LIKE $1 AND status = 'completed'`, like).Scan(&n)
		return
	}
	cleanup := func(e *stepwise.Engine, like string) {
		e.Pool().Exec(ctx, `DELETE FROM workflows WHERE id LIKE $1`, like)
	}

	fmt.Printf("## Throughput: %d workflows x %d steps, queued up front, one worker\n\n", *total, *steps)
	fmt.Println("| Concurrency | Workflows/sec | Steps/sec | Time |")
	fmt.Println("|---:|---:|---:|---:|")
	best := 0
	for _, field := range strings.Split(*levels, ",") {
		c, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return err
		}
		e, err := engine(c)
		if err != nil {
			return err
		}
		prefix := fmt.Sprintf("bench-%d-c%d-", run, c)
		like := prefix + "%"
		if _, err := e.Pool().Exec(ctx, `
			INSERT INTO workflows (id, name, input)
			SELECT $1 || g, 'bench', to_jsonb($3::int) FROM generate_series(1, $2::int) g`, prefix, *total, *steps); err != nil {
			return err
		}
		e.Pool().Exec(ctx, "ANALYZE workflows") // the planner should know the queue is loaded
		wctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		begin := time.Now()
		go func() { e.NewWorker("bench").Run(wctx); close(done) }()
		for completed(e, like) < *total {
			time.Sleep(20 * time.Millisecond)
		}
		elapsed := time.Since(begin)
		stop()
		<-done
		cleanup(e, like)
		e.Close()
		fmt.Printf("| %d | %.0f | %.0f | %.2fs |\n", c,
			float64(*total)/elapsed.Seconds(), float64(*total**steps)/elapsed.Seconds(), elapsed.Seconds())
		best = c
	}

	// Scaling: the same queue drained by 1, 2, 4 and 8 separate worker
	// processes, which is how it would be deployed (one process per machine).
	fmt.Printf("\n## Scaling out: %d workflows x %d steps, N worker processes at concurrency %d each\n\n", *scaleTotal, *steps, *procConc)
	fmt.Println("| Worker processes | Workflows/sec | Steps/sec | Speed-up | Time |")
	fmt.Println("|---:|---:|---:|---:|---:|")
	base := 0.0
	for _, field := range strings.Split(*procs, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return err
		}
		e, err := engine(1)
		if err != nil {
			return err
		}
		sup := &Supervisor{
			WorkerArgs:   []string{"--dsn", *dsn, "--concurrency", strconv.Itoa(*procConc), "--lease", "30s", "--workflow", "bench-unused"},
			RestartDelay: time.Second,
			Quiet:        true,
		}
		if err := sup.Start(n); err != nil {
			return err
		}
		prefix := fmt.Sprintf("bench-%d-w%d-", run, n)
		like := prefix + "%"
		// Queue the work to become ready two seconds from now, so the workers
		// are all connected and the planner has fresh statistics before the
		// clock starts.
		var readyIn float64
		if err := e.Pool().QueryRow(ctx, `
			WITH ins AS (
				INSERT INTO workflows (id, name, input, run_after)
				SELECT $1 || g, 'bench', to_jsonb($3::int), now() + interval '2 seconds' FROM generate_series(1, $2::int) g
				RETURNING run_after
			)
			SELECT extract(epoch FROM max(run_after) - clock_timestamp()) FROM ins`, prefix, *scaleTotal, *steps).Scan(&readyIn); err != nil {
			sup.Stop()
			return err
		}
		e.Pool().Exec(ctx, "ANALYZE workflows")
		begin := time.Now().Add(time.Duration(readyIn * float64(time.Second)))
		time.Sleep(time.Until(begin))
		e.Pool().Exec(ctx, "SELECT pg_notify('stepwise_wake', '')")
		for completed(e, like) < *scaleTotal {
			time.Sleep(20 * time.Millisecond)
		}
		elapsed := time.Since(begin)
		sup.Stop()
		cleanup(e, like)
		e.Close()
		rate := float64(*scaleTotal) / elapsed.Seconds()
		if base == 0 {
			base = rate / float64(n)
		}
		fmt.Printf("| %d | %.0f | %.0f | %.2fx | %.2fs |\n", n, rate, rate*float64(*steps), rate/base, elapsed.Seconds())
	}

	fmt.Printf("\n## Latency: steady arrivals for %s, %d steps each, one worker at concurrency %d\n\n", *duration, *steps, best)
	fmt.Println("| Arrival rate | Completed | p50 | p95 | p99 |")
	fmt.Println("|---:|---:|---:|---:|---:|")
	for _, field := range strings.Split(*rates, ",") {
		rate, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return err
		}
		e, err := engine(best)
		if err != nil {
			return err
		}
		prefix := fmt.Sprintf("bench-%d-r%d-", run, rate)
		like := prefix + "%"
		wctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { e.NewWorker("bench").Run(wctx); close(done) }()
		time.Sleep(300 * time.Millisecond) // let the worker connect and LISTEN

		// Open loop: send on a fixed schedule whether or not earlier ones have finished.
		n := int(duration.Seconds() * float64(rate))
		interval := time.Second / time.Duration(rate)
		begin := time.Now()
		sem := make(chan struct{}, 64)
		for i := 0; i < n; i++ {
			time.Sleep(time.Until(begin.Add(time.Duration(i) * interval)))
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()
				e.Start(ctx, "bench", *steps, prefix+strconv.Itoa(i))
			}()
		}
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
		deadline := time.Now().Add(60 * time.Second)
		for completed(e, like) < n && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		var p []float64
		got := completed(e, like)
		if err := e.Pool().QueryRow(ctx, `
			SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (
				ORDER BY extract(epoch FROM completed_at - created_at) * 1000)
			FROM workflows WHERE id LIKE $1 AND status = 'completed'`, like).Scan(&p); err != nil {
			stop()
			return err
		}
		stop()
		<-done
		cleanup(e, like)
		e.Close()
		fmt.Printf("| %d/sec | %d of %d | %.1f ms | %.1f ms | %.1f ms |\n", rate, got, n, p[0], p[1], p[2])
	}
	return nil
}
