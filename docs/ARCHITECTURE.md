# Architecture

## Modules

| Module | Responsibility |
|---|---|
| `common` | Wire protocol messages, frame codec, job state machine, payload validation, shared DTOs |
| `coordinator` | Job scheduling authority: registry, core event loop, failure detection, retries, persistence, TCP servers |
| `worker` | Task execution: registration, heartbeats, bounded thread pool, task implementations |
| `client` | CLI and a small blocking client library |
| `integration-tests` | End-to-end tests running the real components in-process |

Dependencies flow strictly downward: everything depends on `common`; nothing
depends on the coordinator except the tests.

## Coordinator

### The single-writer core

`CoordinatorCore` owns *all* mutable coordinator state: the `WorkerRegistry`
and every `Job`. It runs on one dedicated thread (a single-thread
`ScheduledExecutorService`). Network threads never touch state directly:

- worker connection readers turn frames into events
  (`onWorkerRegistered`, `onHeartbeat`, `onTaskResult`, `onWorkerDisconnected`)
  and submit them to the core;
- client connection handlers submit queries and block for the answer
  (`askCore`);
- the periodic **sweep** (default every 500 ms) runs on the core thread
  itself: detect dead workers → detect expired execution deadlines → promote
  due retries → schedule queued jobs.

**Invariant:** because scheduling, result handling, failure detection, and
recovery are all serialized on one thread, no interleaving can assign one job
to two workers or apply a result twice. This is verified by
`SchedulingSafetyIT`, which fires 60 submissions from 4 threads at 5 workers
and asserts exactly one assignment per job.

The cost is that the core thread must never block for long. Everything it does
is in-memory work plus SQLite upserts of tiny rows and small non-blocking-in-
practice socket writes. A per-connection outbound queue with writer threads
would remove the theoretical stall on a full TCP send buffer; see
DESIGN_DECISIONS.

### Components on the core thread

- **WorkerRegistry** — live sessions by worker id, heartbeat timestamps,
  capacity accounting (`capacity - activeJobs`). Plain `HashMap`s, safe by
  thread confinement.
- **Scheduler** (`scheduleQueuedJobs`) — oldest `QUEUED` job first, assigned
  to the least-loaded worker with free capacity. Runs after every event that
  could create work or capacity, plus each sweep.
- **Failure detector** (`detectDeadWorkers`) — a session whose last heartbeat
  is older than `heartbeat-timeout-millis` (default 6000) is suspected dead:
  session removed and closed, each of its RUNNING jobs retried or failed.
  A dropped connection triggers the same path immediately via the reader
  thread's disconnect event.
- **Deadline enforcer** (`detectExpiredDeadlines`) — a RUNNING job past its
  per-attempt deadline (`assignedAt + task-timeout-millis`, coordinator clock)
  has its lease revoked, a best-effort `TASK_CANCEL` sent to the owning worker
  (`revokeAttemptOnWorker`), and is retried or failed. This is the only
  detector that catches a stuck task on a *live* worker; it shares the retry
  and stale-result machinery with worker-loss handling.
- **RetryPolicy** — delay = `base × 2^(attempts-1)`, capped (defaults 1 s
  base, 30 s cap). The attempt *budget* lives on the job (`maxAttempts`,
  default 3, settable per job).
- **Attempt leases** — `Job.assignTo` issues a fresh UUID per assignment;
  `applyTaskResult` accepts a result only if the job is RUNNING *and* the
  lease matches. Everything else is logged as stale and dropped.
- **Admission control** — an O(1) `activeJobCount` (non-terminal jobs) kept in
  the core state. `submitJob` admits a new job only if the count is below
  `max-active-jobs`, otherwise returns a typed `SUBMIT_REJECTED`; the count is
  incremented on admission and decremented through the single `persistRetired`
  chokepoint on any active→terminal transition. Because it lives on the
  single-writer thread, the check-and-increment is atomic with no extra locking
  — the same property that makes scheduling race-free.

### Persistence and recovery

`JobRepository` is write-through: every state transition is upserted into
SQLite (one row per job) before it is observable by clients. The working set
stays in memory; the database exists purely for recovery.

On startup (`recoverFromRepository`), before any network listener opens:

- all rows load into memory;
- `RUNNING` jobs (their worker sessions are gone, attempt outcome unknown)
  are requeued if budget remains, else failed with an explicit
  "restarted during final attempt" error;
- `RETRY_WAIT`/`QUEUED`/terminal jobs load as-is.

`Coordinator.close()` shuts the core down *first*, freezing durable state,
then closes sockets. A graceful stop therefore leaves exactly the same
database shape as a crash — recovery has one code path, not two.

### Thread inventory (coordinator process)

| Thread | Count | Role |
|---|---|---|
| `coordinator-core` | 1 | all state mutation, sweep |
| `worker-acceptor`, `client-acceptor` | 2 | `accept()` loops |
| `worker-conn-*` | 1 per worker | blocking frame reads → core events |
| `client-conn-*` | 1 per client | request/response, blocks on `askCore` |

Blocking I/O with a thread per connection is deliberate: at tens of
connections it is simpler and easier to reason about than NIO selectors.

## Worker

Three executors, three concerns:

- **connection thread** — connect, register, then a blocking read loop for
  `TASK_ASSIGN`; on any connection loss it interrupts in-flight tasks (their
  leases are already doomed) and reconnects after a delay;
- **heartbeat thread** — a `ScheduledExecutorService` sending `HEARTBEAT`
  every 2 s (default), deliberately independent of the task pool so a
  saturated or slow task can never cause a false death sentence;
- **task pool** — `newFixedThreadPool(capacity)`; the coordinator never
  assigns beyond the advertised capacity, so the pool never queues meaningfully.

All frame writes (heartbeats, results) synchronize on the connection's output
stream, because interleaved frames from two threads would corrupt the stream.

Payloads are validated again on the worker (`TaskPayloads`) — the worker does
not trust the wire, even though the coordinator validates at submission.

## Client

`CoordinatorClient` is a blocking request/response wrapper over one socket;
`wait` is client-side polling of `status`. The CLI prints human-readable
output and exits nonzero on errors.

## Data ownership summary

| Data | Owner | Others see it via |
|---|---|---|
| Job state + leases | CoordinatorCore (core thread) | snapshots (immutable records) |
| Worker liveness/capacity | CoordinatorCore | snapshots |
| Durable job rows | SQLite, written only by core thread | recovery on startup |
| In-flight task futures | Worker process | never leaves the worker |
