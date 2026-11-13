package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dolokashi35/stepwise"
	"github.com/jackc/pgx/v5"
)

//go:embed dashboard.html
var dashboardHTML []byte

const demoPrefix = "ord-"

// runDemo serves the dashboard and supervises worker processes. The web
// server runs no workflows itself, so it stays up while workers are killed.
func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	dsn := fs.String("dsn", defaultDSN(), "PostgreSQL connection string")
	addr := fs.String("addr", "127.0.0.1:8765", "address for the dashboard")
	workers := fs.Int("workers", 3, "worker processes")
	concurrency := fs.Int("concurrency", 4, "workflows per worker")
	lease := fs.Duration("lease", 3*time.Second, "lease duration (time to notice a dead worker)")
	stepDelay := fs.Duration("step-delay", 1200*time.Millisecond, "how long each demo step takes")
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

	sup := &Supervisor{
		WorkerArgs:   []string{"--dsn", *dsn, "--concurrency", strconv.Itoa(*concurrency), "--lease", lease.String()},
		RestartDelay: 2 * time.Second,
	}
	if err := sup.Start(*workers); err != nil {
		return err
	}
	defer sup.Stop()

	var chaos atomic.Bool
	go func() {
		for range time.Tick(1500 * time.Millisecond) {
			if chaos.Load() {
				sup.KillRandom()
			}
		}
	}()

	var orderSeq atomic.Int64
	orderSeq.Store(time.Now().Unix() % 100000 * 100)

	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}
	fail := func(w http.ResponseWriter, err error) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	})

	mux.HandleFunc("POST /api/start", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Count    int  `json:"count"`
			FailShip bool `json:"fail_ship"`
			FlakyPct int  `json:"flaky_pct"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.Count < 1 {
			body.Count = 1
		}
		if body.Count > 200 {
			body.Count = 200
		}
		for i := 0; i < body.Count; i++ {
			id := fmt.Sprintf("%s%d", demoPrefix, orderSeq.Add(1))
			in := OrderInput{OrderID: id, DelayMs: int(stepDelay.Milliseconds()), FlakyPct: body.FlakyPct, FailShip: body.FailShip}
			if _, err := e.Start(r.Context(), "order", in, id); err != nil {
				fail(w, err)
				return
			}
		}
		writeJSON(w, map[string]any{"started": body.Count})
	})

	mux.HandleFunc("POST /api/kill", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Slot int `json:"slot"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		writeJSON(w, map[string]any{"killed": sup.Kill(body.Slot)})
	})

	mux.HandleFunc("POST /api/chaos", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			On bool `json:"on"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		chaos.Store(body.On)
		writeJSON(w, map[string]any{"chaos": body.On})
	})

	mux.HandleFunc("POST /api/clear", func(w http.ResponseWriter, r *http.Request) {
		// Only finished demo orders: in-flight ones are left alone.
		_, err := pool.Exec(r.Context(), `
			WITH gone AS (
				DELETE FROM workflows WHERE id LIKE $1 AND status IN ('completed', 'failed') RETURNING id
			)
			DELETE FROM demo_effects WHERE workflow_id IN (SELECT id FROM gone)`, demoPrefix+"%")
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	})

	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		like := demoPrefix + "%"

		counts := map[string]int{"pending": 0, "running": 0, "sleeping": 0, "completed": 0, "failed": 0}
		rows, err := pool.Query(ctx, `SELECT status, count(*) FROM workflows WHERE id LIKE $1 GROUP BY 1`, like)
		if err != nil {
			fail(w, err)
			return
		}
		var status string
		var n int
		if _, err := pgx.ForEachRow(rows, []any{&status, &n}, func() error { counts[status] = n; return nil }); err != nil {
			fail(w, err)
			return
		}

		var applied, executions, takeovers int
		pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(executions), 0) FROM demo_effects WHERE workflow_id LIKE $1`, like).Scan(&applied, &executions)
		pool.QueryRow(ctx, `SELECT COALESCE(sum(GREATEST(epoch - 1, 0)), 0) FROM workflows WHERE id LIKE $1`, like).Scan(&takeovers)

		var workflows json.RawMessage
		err = pool.QueryRow(ctx, `
			SELECT COALESCE(json_agg(t ORDER BY t.created_at DESC), '[]') FROM (
				SELECT w.id, w.status, w.locked_by AS worker, w.epoch, w.error, w.created_at,
				       (w.input ->> 'FailShip')::bool AS fail_ship,
				       round(extract(epoch FROM COALESCE(w.completed_at, now()) - w.created_at)::numeric, 1) AS seconds,
				       GREATEST(0, round(extract(epoch FROM w.lease_until - now())::numeric, 1)) AS lease_left,
				       COALESCE((
				           SELECT json_agg(json_build_object(
				               'name', ev.name, 'attempts', ev.attempts, 'error', ev.error, 'worker', ev.worker) ORDER BY ev.seq)
				           FROM events ev WHERE ev.workflow_id = w.id), '[]') AS events
				FROM workflows w
				WHERE w.id LIKE $1
				ORDER BY w.created_at DESC
				LIMIT 40
			) t`, like).Scan(&workflows)
		if err != nil {
			fail(w, err)
			return
		}

		running := map[string]int{}
		rows, err = pool.Query(ctx, `
			SELECT locked_by, count(*) FROM workflows
			WHERE status = 'running' AND locked_by IS NOT NULL AND id LIKE $1 GROUP BY 1`, like)
		if err == nil {
			var who string
			pgx.ForEachRow(rows, []any{&who, &n}, func() error { running[who] = n; return nil })
		}
		type workerInfo struct {
			SlotInfo
			Running int `json:"running"`
		}
		var ws []workerInfo
		for _, s := range sup.Snapshot() {
			ws = append(ws, workerInfo{s, running[s.ID]})
		}

		writeJSON(w, map[string]any{
			"workers":   ws,
			"counts":    counts,
			"effects":   map[string]int{"applied": applied, "executions": executions, "duplicates_absorbed": executions - applied},
			"takeovers": takeovers,
			"kills":     sup.Kills(),
			"chaos":     chaos.Load(),
			"lease":     lease.Seconds(),
			"steps":     orderSteps,
			"workflows": workflows,
		})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	fmt.Printf("dashboard: http://%s   (%d workers, lease %s)\n", *addr, *workers, *lease)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
