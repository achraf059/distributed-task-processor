# Failure Model

What can fail, what the system does about it, and what it honestly guarantees.

## Guarantees in one paragraph

Task execution is **at-least-once**: every job either reaches `COMPLETED`, or
reaches `FAILED` after its attempt budget is spent, as long as the coordinator
(eventually) runs and some worker (eventually) has capacity. A job may execute
**more than once** across failures — never because of a scheduler race, only
because a previous attempt's outcome was unknowable. **Attempt leases**
guarantee that whatever duplicate or late execution happens, coordinator state
records exactly one outcome per job.

## Worker crash (process dies)

The OS closes the TCP connection → the coordinator's reader thread gets EOF →
`onWorkerDisconnected` fires immediately: the session is removed, and each of
its RUNNING jobs is moved to `RETRY_WAIT` with exponential backoff (or
`FAILED` if the budget is gone). Detection here is near-instant; no heartbeat
timeout is involved.

## Worker hang / network partition (connection stays open)

Heartbeats stop arriving. After `heartbeat-timeout-millis` (default 6 s ≈
three missed 2 s heartbeats) the sweep declares the worker **suspected dead**
and takes the same invalidation path. Two honest caveats:

- A timeout is *suspicion*, not proof — an asynchronous network cannot
  distinguish a dead peer from a slow one. A GC-paused or partitioned worker
  may be declared dead while still computing.
- That is precisely why leases exist: when the "dead" worker finishes and
  reports, its `attemptId` no longer matches and the result is logged
  (`Stale result rejected`) and dropped.

## Stuck task on a healthy worker (execution deadline)

A worker being alive does **not** mean every task on it is making progress: a
task can wedge (an infinite loop, a stuck I/O call) while the worker keeps
heartbeating normally. Heartbeat detection cannot catch this — the worker is
not dead. Execution deadlines close the gap.

Each attempt is leased with a deadline of `assignedAt + executionTimeout`
(`--task-timeout-millis`, default 10 min), measured **only on the
coordinator's clock** — worker clocks are never consulted, so no clock
synchronization is required. When the sweep finds a RUNNING job past its
deadline it:

1. revokes the lease (so any later result is stale-rejected);
2. sends a best-effort `TASK_CANCEL` to the owning worker and frees its slot;
3. retries the job (backoff) or fails it if the attempt budget is spent.

Honest caveats, mirroring heartbeat detection:

- A deadline is a *contract*, and expiry is *suspicion* that the task exceeded
  it — not proof the task is broken. Set the timeout above legitimate task
  durations; too low means healthy long tasks get retried forever and end
  FAILED with an "execution deadline exceeded" error.
- Cancellation is cooperative (thread interruption). A task that ignores
  interruption keeps consuming CPU until it finishes, but its lease is already
  gone, so it cannot affect job state.

## The classic at-least-once window

Worker finishes the task → dies before `TASK_RESULT` is read by the
coordinator. The work *happened*, but the coordinator cannot know. The job is
retried and executes twice. Consequences:

- built-in tasks are pure functions, so duplicate execution is harmless here;
- users running externally side-effecting tasks under at-least-once semantics
  must make them idempotent — this is a property of the semantics, not a bug;
- exactly-once *execution* would require the task itself to participate in a
  transaction/dedup protocol; no system can bolt it on from the outside, so
  this project does not claim it.

## Stale and duplicate results

Rejected whenever `(job is RUNNING) ∧ (result.attemptId == current lease)`
fails to hold. Covered cases (all integration-tested):

- result for an attempt superseded by a retry (zombie worker returns);
- duplicate result for an attempt that already completed;
- result for a job the coordinator no longer considers running;
- result for an unknown job id;
- result from an attempt whose deadline expired and was reassigned;
- result from an attempt whose job was cancelled.

The last two are the same mechanism reached through the new triggers, and are
integration-tested.

## Client cancellation

A client may cancel any non-terminal job. QUEUED and RETRY_WAIT jobs move
straight to the terminal `CANCELLED` state. A RUNNING job additionally has its
lease revoked and a best-effort `TASK_CANCEL` sent to the worker, then becomes
CANCELLED. Cancelling an already-terminal job is a no-op that reports the job's
real final state; cancelling an unknown id is an error. Because cancellation
revokes the lease, a worker that keeps running the cancelled task and later
reports success is stale-rejected — a cancelled job never comes back.

## Worker re-registration

If a worker id registers while a session under that id is live, the old
session is treated as lost first (assignments invalidated), then the new
session is accepted. Registration is a fresh start — a reconnecting worker's
old leases are never resurrected. The real worker mirrors this by interrupting
its in-flight tasks when its connection drops.

## Coordinator crash and restart

Job state is written through to SQLite before it is client-visible, so:

- terminal jobs keep their result/error;
- `QUEUED`/`RETRY_WAIT` jobs resume as-is;
- `RUNNING` jobs are the interesting case: their attempt's outcome is unknown
  (the worker may have finished, died, or still be computing). Recovery
  requeues them — the crashed attempt still counts against the budget — or
  fails them if it was the final attempt, with an explicit error saying so.
  This is the at-least-once trade-off again, applied across restarts.

The persisted `deadline` is a coordinator-clock instant from the **dead**
process, so it means nothing to the new one. Recovery never treats a loaded
`RUNNING` job as still-leased: it requeues (discarding the stale deadline
along with the lease), and the fresh assignment gets a fresh deadline. A stale
deadline can therefore never "instantly expire" a recovered job — verified by
`CoordinatorRestartIT.recoveryIgnoresStaleDeadlineAndRequeuesInsteadOfExpiring`.

Workers are not persisted (their sessions cannot survive anyway); they
reconnect and re-register on their own retry loop. A worker that finished a
task during the outage will have its result rejected as stale, because
recovery discarded the old lease.

While the coordinator is down the system is unavailable — see below.

## What is *not* tolerated

- **Coordinator permanent loss**: single coordinator by design. The database
  provides durability, not availability. High availability would need
  replicated coordinators and leader election (Raft-shaped work), which is
  intentionally out of scope; the interesting single-node semantics come
  first.
- **SQLite file corruption/loss**: no replication of the store itself.
- **Byzantine peers**: a worker that *lies* (wrong results, forged ids) is
  trusted. Leases protect against staleness, not malice; there is no
  authentication on the plaintext TCP links.
- **Clock jumps**: failure detection uses wall-clock deltas on the
  coordinator only (workers' clocks are never compared), so it tolerates skew
  between machines but not a large backwards jump on the coordinator host.

## Tuning

| Setting | Default | Meaning |
|---|---|---|
| worker `--heartbeat-interval-millis` | 2000 | heartbeat period |
| coordinator `--heartbeat-timeout-millis` | 6000 | silence before suspected dead |
| coordinator `--sweep-interval-millis` | 500 | failure scan + deadline scan + retry promotion + scheduling period |
| coordinator `--task-timeout-millis` | 600000 | per-attempt execution deadline (coordinator clock) |
| coordinator `--max-attempts` | 3 | default attempt budget per job |
| coordinator `--retry-base-delay-millis` / `--retry-max-delay-millis` | 1000 / 30000 | exponential backoff bounds |

Lower timeouts detect failures faster but raise false positives; the 3×
interval default is a conventional compromise. Integration tests shrink all of
these to hundreds of milliseconds, which is also a demonstration that the
timing model is configuration, not hard-coded behavior.
