package stepwise

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Everything the engine knows lives in two tables.
//
// workflows is both the record of each workflow and the task queue: a row is
// "ready" when it is not finished, its run_after time has passed, and nobody
// holds a live lease on it.
//
// events is the append-only history of one workflow: one row per finished
// step or timer, keyed by (workflow_id, seq). Recovery is nothing more than
// reading this table back.
const schema = `
CREATE TABLE IF NOT EXISTS workflows (
    id           TEXT PRIMARY KEY,
    name         TEXT        NOT NULL,
    input        JSONB       NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending', -- pending | running | sleeping | completed | failed
    output       JSONB,
    error        TEXT,
    run_after    TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by    TEXT,
    lease_until  TIMESTAMPTZ,
    epoch        BIGINT      NOT NULL DEFAULT 0,          -- fencing token: +1 on every claim
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS workflows_ready ON workflows (run_after)
    WHERE status IN ('pending', 'running', 'sleeping');

CREATE TABLE IF NOT EXISTS events (
    workflow_id TEXT        NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    seq         INT         NOT NULL,
    kind        TEXT        NOT NULL,                     -- step | timer
    name        TEXT        NOT NULL,
    attempts    INT         NOT NULL DEFAULT 1,
    result      JSONB,
    error       TEXT,
    fire_at     TIMESTAMPTZ,
    worker      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, seq)
);

-- Audit log of ownership changes: one row each time a workflow is claimed
-- while another worker's name is still on it, which only happens when that
-- worker's lease ran out. Written in the same statement as the claim.
CREATE TABLE IF NOT EXISTS takeovers (
    workflow_id TEXT        NOT NULL REFERENCES workflows (id) ON DELETE CASCADE,
    epoch       BIGINT      NOT NULL,
    from_worker TEXT        NOT NULL,
    to_worker   TEXT        NOT NULL,
    at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, epoch)
);

-- A queue table churns: every workflow is inserted, updated a few times and
-- eventually deleted. Vacuum and re-analyse it far more eagerly than the
-- defaults (20% / 10% of the table) so dead rows and stale row counts do not
-- pile up and push the planner off the indexes.
ALTER TABLE workflows SET (
    autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 200,
    autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 200
);`

// ErrLeaseLost means another worker has taken over this workflow. The caller
// must stop working on it immediately and write nothing else.
var ErrLeaseLost = errors.New("stepwise: lease lost")

type event struct {
	Seq      int
	Kind     string // "step" or "timer"
	Name     string
	Attempts int
	Result   []byte
	Error    *string
	FireAt   *time.Time
}

type claim struct {
	ID    string
	Name  string
	Input []byte
	Epoch int64
}

func seconds(d time.Duration) float64 { return d.Seconds() }

// claimReady atomically takes up to n ready workflows for this worker.
//
// FOR UPDATE SKIP LOCKED is what lets many workers poll the same table
// without blocking each other or grabbing the same row: each one locks the
// rows it picks and silently skips rows another worker has already locked.
//
// A 'running' row whose lease has expired is ready too. That is the whole
// crash-recovery mechanism: a dead worker stops renewing, and its workflows
// become claimable again.
func (e *Engine) claimReady(ctx context.Context, worker string, n int, names []string) ([]claim, error) {
	rows, err := e.pool.Query(ctx, `
		WITH picked AS (
			SELECT id, locked_by AS prev FROM workflows
			WHERE status IN ('pending', 'running', 'sleeping')
			  AND run_after <= now()
			  AND (lease_until IS NULL OR lease_until < now())
			  AND name = ANY($4)
			ORDER BY run_after
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE workflows w
			SET status      = 'running',
			    locked_by   = $2,
			    lease_until = now() + make_interval(secs => $3),
			    epoch       = w.epoch + 1,
			    started_at  = COALESCE(w.started_at, now())
			FROM picked
			WHERE w.id = picked.id
			RETURNING w.id, w.name, w.input, w.epoch, picked.prev
		), logged AS (
			-- prev is only set when the last owner neither finished nor handed
			-- the workflow back, i.e. its lease expired under it.
			INSERT INTO takeovers (workflow_id, epoch, from_worker, to_worker)
			SELECT id, epoch, prev, $2 FROM claimed WHERE prev IS NOT NULL
		)
		SELECT id, name, input, epoch FROM claimed`,
		n, worker, seconds(e.opts.Lease), names)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (claim, error) {
		var c claim
		return c, r.Scan(&c.ID, &c.Name, &c.Input, &c.Epoch)
	})
}

func (e *Engine) loadHistory(ctx context.Context, id string) (map[int]event, error) {
	rows, err := e.pool.Query(ctx,
		`SELECT seq, kind, name, attempts, result, error, fire_at FROM events WHERE workflow_id = $1`, id)
	if err != nil {
		return nil, err
	}
	history := map[int]event{}
	var ev event
	_, err = pgx.ForEachRow(rows, []any{&ev.Seq, &ev.Kind, &ev.Name, &ev.Attempts, &ev.Result, &ev.Error, &ev.FireAt}, func() error {
		history[ev.Seq] = ev
		return nil
	})
	return history, err
}

// recordEvent appends one event to a workflow's history, but only if the
// caller still owns the workflow.
//
// The epoch check is a fencing token. Suppose worker A stalls (GC pause,
// network partition), its lease expires, worker B claims the workflow (epoch
// 1 -> 2) and carries on. When A wakes up it still believes it is the owner.
// Its write carries epoch 1, matches no row, inserts nothing, and A finds out
// it has been replaced. Without this, A and B could both append history.
func (e *Engine) recordEvent(ctx context.Context, c claim, worker string, ev event) error {
	tag, err := e.pool.Exec(ctx, `
		WITH owner AS (
			SELECT 1 FROM workflows WHERE id = $1 AND epoch = $2 FOR UPDATE
		)
		INSERT INTO events (workflow_id, seq, kind, name, attempts, result, error, fire_at, worker)
		SELECT $1, $3, $4, $5, $6, $7::jsonb, $8, $9, $10 FROM owner`,
		c.ID, c.Epoch, ev.Seq, ev.Kind, ev.Name, ev.Attempts, ev.Result, ev.Error, ev.FireAt, worker)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// finish marks a workflow completed or failed (fenced, like every write).
func (e *Engine) finish(ctx context.Context, c claim, status string, output []byte, errMsg *string) error {
	tag, err := e.pool.Exec(ctx, `
		UPDATE workflows
		SET status = $3, output = $4::jsonb, error = $5, completed_at = now(), locked_by = NULL, lease_until = NULL
		WHERE id = $1 AND epoch = $2`,
		c.ID, c.Epoch, status, output, errMsg)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// suspend parks a workflow until wakeAt and gives up the lease, so a long
// sleep holds no worker, no goroutine and no connection.
func (e *Engine) suspend(ctx context.Context, c claim, wakeAt time.Time) error {
	tag, err := e.pool.Exec(ctx, `
		UPDATE workflows
		SET status = 'sleeping', run_after = $3, locked_by = NULL, lease_until = NULL
		WHERE id = $1 AND epoch = $2`,
		c.ID, c.Epoch, wakeAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// release hands a workflow back to the queue right away (graceful shutdown),
// instead of making everyone wait for the lease to run out.
func (e *Engine) release(ctx context.Context, c claim) error {
	_, err := e.pool.Exec(ctx, `
		UPDATE workflows SET locked_by = NULL, lease_until = NULL
		WHERE id = $1 AND epoch = $2 AND status = 'running'`,
		c.ID, c.Epoch)
	return err
}

// heartbeat extends the lease on every workflow this worker is still running
// and returns the ids it still owns. Anything missing has been taken over.
func (e *Engine) heartbeat(ctx context.Context, worker string, ids []string) (map[string]bool, error) {
	rows, err := e.pool.Query(ctx, `
		UPDATE workflows
		SET lease_until = now() + make_interval(secs => $3)
		WHERE id = ANY($1) AND locked_by = $2 AND status = 'running'
		RETURNING id`,
		ids, worker, seconds(e.opts.Lease))
	if err != nil {
		return nil, err
	}
	owned := map[string]bool{}
	var id string
	_, err = pgx.ForEachRow(rows, []any{&id}, func() error {
		owned[id] = true
		return nil
	})
	return owned, err
}
