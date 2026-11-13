# Design notes

This file explains the decisions behind the engine: what problem each piece
solves, what was traded away, and what is deliberately missing.

## The problem

A job has several steps with side effects: charge a card, reserve stock, ship,
email. The process running it can die between any two instructions. After a
restart you need to know which steps happened, and you must not redo them.

## The core idea: record, then replay

Every workflow has an append-only history in the `events` table, keyed by
`(workflow_id, seq)`. A step is "done" when its result row is committed.

To recover, the engine does not restore a saved stack. It runs the workflow
function again from the top. Each `Step` call takes the next `seq`. If history
has a row at that position, `Step` returns the saved result without running
anything. The first `Step` with no row is where live execution resumes.

This is why workflow code must be deterministic: given the same step results,
it must ask for the same steps in the same order. Time, randomness and I/O
belong inside steps. If the code is changed while workflows are in flight and
a replay asks for a different step than history holds, the engine fails that
workflow with a "non-deterministic" error instead of guessing.

## What "exactly once" means here

There are two different things a step can do exactly once:

1. **Have its result recorded.** Guaranteed. The primary key on
   `(workflow_id, seq)` and the fencing check make a second record impossible.
2. **Run its side effect.** Not guaranteed, and it cannot be. If a worker dies
   after the card was charged but before the result row committed, nobody can
   tell from the database that the charge happened, so the step runs again.

So the real guarantee is: side effects are attempted **at least once**, results
are recorded **exactly once**, and every step gets a stable **idempotency key**
(`workflow_id:seq`) to pass to the outside system so it can drop the repeat.
The chaos test measures this: in a 10,000-order run with only worker kills,
about 300 steps ran twice; with frozen workers and database crashes added,
about 500 did. Every effect was still applied once.

If the side effect is a write to the same PostgreSQL database, it could be made
truly exactly-once by committing it in the same transaction as the event row.
The engine does not offer that yet.

## The queue is a table

`workflows` is both the record of each workflow and the task queue. A row is
ready when it is unfinished, its `run_after` has passed, and no live lease
covers it. Workers claim with:

```sql
SELECT id FROM workflows WHERE <ready> ORDER BY run_after LIMIT n
FOR UPDATE SKIP LOCKED
```

`SKIP LOCKED` is what makes this work with many workers: a row another worker
is in the middle of claiming is skipped rather than waited for, so workers do
not queue up behind each other and cannot claim the same row.

**Why PostgreSQL and not a custom log or Kafka/Redis:** the queue, the history
and the workflow state change together in single transactions, so there is no
"enqueued but state not saved" window to reason about. The cost is throughput:
every step is a database commit.

## Leases: noticing a dead worker

A claim sets `lease_until = now() + lease`. The worker renews every `lease / 3`.
A dead worker stops renewing, and once the lease passes, the row is ready
again. Nothing has to detect the death; it falls out of the claim query.

The lease length is a trade-off. Short leases recover faster but a slow
heartbeat (GC pause, loaded database) can lose a healthy worker its workflows.
The demo uses 3 s, the chaos test 1 s.

On graceful shutdown the worker hands its workflows back immediately instead
of letting leases run out.

## Fencing: the worker that is not dead, only slow

Leases alone are unsafe. Worker A stalls for five seconds. Its lease expires, B
claims the workflow and continues. A wakes up, still believing it is the owner,
and writes a step result. Now two workers are appending to one history.

Every claim increments `epoch`. Every write (`recordEvent`, `finish`,
`suspend`) is conditional on `epoch` still matching, inside the same statement
that does the write. A's write carries the old epoch, changes nothing, and A
abandons the workflow. `TestFencingRejectsWritesFromAReplacedWorker` covers this.

**Measuring it.** A takeover of a worker that was not dead is a false
takeover. To count them, the claim statement writes a row to `takeovers`
whenever it takes a workflow that still has another worker's name on it, in
the same statement as the claim, so the log cannot disagree with what
happened. The chaos test compares that log with the time each worker process
actually died. With only `SIGKILL` faults and a 1 s lease there were none in
10,000 orders. Freezing workers with `SIGSTOP` for 3 s, or taking the database
down for 2 s, produces hundreds, and all checks still pass.

Fencing protects the engine's own tables. It does not stop a stalled worker
from finishing a call to an outside system that it had already started; that is
again what idempotency keys are for.

## Retries and failure

Retries happen in memory with exponential backoff while the worker keeps
renewing the lease. Only the final outcome is recorded (with the attempt
count). A crash during retries restarts the count, which errs on the side of
trying more. A step that exhausts its attempts, or returns `Permanent(err)`,
is recorded as failed, and `Step` returns an error the workflow can act on,
typically by running compensation steps.

## When the database goes away

Workers treat a database error like any other reason to stop: a step result
that could not be saved is not counted as done, and the execution is
abandoned. Nothing else is needed for correctness, because "done" is defined
as "the row is committed". After PostgreSQL comes back, connections are
re-established, leases have expired, and the workflows are claimed and
replayed like after any crash.

Two things follow from that. First, an outage longer than the lease causes
false takeovers when it ends, because nobody could heartbeat; fencing sorts
them out. Second, a step that is in the middle of retrying keeps retrying
through the outage, and if the outage outlasts its retry budget the step
fails for good and the workflow compensates. That is a policy choice: the
alternative is to retry forever.

The chaos test crashes PostgreSQL with an immediate shutdown (no checkpoint,
so it must replay its write-ahead log) every few seconds. It cannot test
losing the disk; surviving that needs a replica.

## Timers

`Sleep` records the absolute wake-up time on first execution. Short sleeps
wait in memory. Long ones set the row to `sleeping` with `run_after` at the
wake-up time, release the lease, and unwind the function. A million sleeping
workflows cost a million rows and nothing else. When the time comes, any
worker claims the row and replays to the `Sleep`, which now returns at once.

Unwinding uses `panic`/`recover` internally. Temporal's Go SDK instead runs
workflow code on coroutines it can block. Panics were the simpler choice and
user code never sees them, but a workflow that uses a bare `recover()` would
interfere.

## Things learned from measuring

**Stale statistics on a churning table.** The first benchmarks were erratic,
from 250 to 4,800 workflows/sec for the same run. Query plans captured with
`auto_explain` showed PostgreSQL scanning the whole `workflows` table for
`WHERE id = $1`, a primary-key lookup. The chaos test had deleted 10,000 rows,
leaving a primary-key index of 386 pages for a table of 5 rows. With those
numbers the planner estimated an index lookup as more expensive than a
sequential scan, and every statement slowed in proportion to the queue length.
The fix has two parts: the schema now sets aggressive per-table autovacuum
thresholds on `workflows`, and the benchmark reindexes and analyses before
each run. In production the same risk exists after any bulk delete (for
example a retention job), which should delete in small batches.

**Dead index entries in the ready index.** Finished workflows leave dead
entries at the front of the partial index the claim query scans, until vacuum
removes them. Claim time creeps up between vacuums. A dedicated queue table
partitioned by time, or moving finished rows to an archive table, would fix
this properly.

## What is missing

- **One writer per workflow.** No signals, queries, child workflows or
  parallel steps within a workflow.
- **History is loaded whole.** A workflow with 100,000 steps replays 100,000
  rows on every recovery. Real engines snapshot or "continue as new".
- **No workflow versioning.** A code change that reorders steps fails in-flight
  workflows (detected, not handled).
- **Clock skew.** Timer wake-up times come from the worker's clock and are
  compared with the database's clock.
- **Single PostgreSQL.** The database is the bottleneck and the single point
  of failure. The engine survives it restarting, not it being lost. Adding
  worker processes on one machine gave 1.6x at four and less at eight.
  Scaling further would mean sharding workflows across databases.
- **No retention.** Finished workflows stay in the table forever.
