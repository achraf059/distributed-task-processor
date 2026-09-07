# Design Decisions

Lightweight ADR-style notes: each records the decision, the reasoning, and
what was consciously given up.

## 1. Java 21

Strong standard-library concurrency (executors, scheduled pools, concurrent
collections), real threads with blocking I/O that map directly onto the
concepts being demonstrated, static types for a protocol-heavy codebase, and
modern language features that pay off here (records for messages, sealed
interfaces for exhaustive protocol dispatch, switch patterns). Trade-off:
more ceremony than Python/Go; accepted, since the JVM concurrency model is
part of what the project is meant to exercise.

## 2. Raw TCP with length-prefixed JSON frames

- **Why not an RPC framework (gRPC etc.)?** It would erase exactly the layer
  this project exists to demonstrate: framing, partial reads, disconnect
  semantics, and message design.
- **Why length-prefix?** Delimiters require escaping and scanning;
  fixed-length headers are the standard, simplest correct answer, and make
  the "TCP is a byte stream" lesson explicit in code (`readFully`).
- **Why JSON, not a binary encoding?** Debuggability (logs, tests, `nc`)
  outweighs bytes-on-wire at this scale; the codec boundary (`MessageIO`)
  means a binary encoding could be swapped in behind two functions.
- Max frame 1 MiB: bounds memory per connection against malicious or buggy
  lengths.

## 3. Single-writer coordinator core (event loop) instead of locks

All coordinator state is confined to one thread; I/O threads submit events.
Alternatives: fine-grained locks (deadlock-prone, hard to review for a
codebase meant to be explainable) or `synchronized` on a monolith (a lock in
disguise with worse contention). The single-writer design makes the crucial
invariant — one job never assigned twice concurrently — structural rather than
something each code path must re-earn. Cost: the core must never block; the
one theoretical violation (a socket write into a full TCP buffer) is accepted
at this scale and documented, with per-session writer queues as the known fix.

## 4. Thread-per-connection blocking I/O

At tens of connections, `accept()`-and-spawn with blocking reads is the
clearest correct design, and the failure semantics (EOF, `IOException`) fall
out naturally. NIO/selectors or virtual-thread pools would matter at
thousands of connections — a scaling problem this project does not claim to
solve.

## 5. At-least-once semantics with attempt leases

Exactly-once *execution* is impossible for opaque tasks when workers can die
between doing the work and reporting it (two-generals-shaped uncertainty). The
honest choices are at-most-once (lose work on every failure — useless for a
fault-tolerance project) or at-least-once with duplicate protection at the
state layer. Each assignment gets a UUID lease; a result is applied only if it
matches the current lease of a RUNNING job. This converts "duplicates can
happen" from a correctness bug into a documented property, and it is cheap:
one string comparison.

## 6. Heartbeats + timeout as the failure detector

Simplest failure detector that works in practice, and its known weakness
(cannot distinguish slow from dead) is a feature for teaching purposes because
the lease mechanism must then handle resurrections. Detection is fail-fast
where TCP already tells us (EOF ⇒ immediate invalidation) and timeout-based
otherwise. Phi-accrual or gossip detectors are strictly out of scope for one
coordinator. Heartbeats run on their own worker thread specifically so task
saturation cannot look like death.

## 7. SQLite behind a repository interface

The persistence *lesson* here is write-through durability plus recovery
semantics, not building a storage engine. SQLite is embedded (no service to
run), transactional, and one dependency. The `JobRepository` interface keeps
SQL out of the domain and let unit tests run in memory. One connection, used
only from the core thread — no pool, no locking. Trade-off: SQLite fsync per
transition caps throughput; irrelevant at portfolio scale and swappable behind
the interface.

## 8. No `TASK_ACCEPTED` handshake

An assignment is only "in doubt" if the connection breaks around it — and a
broken connection already invalidates every lease on that session, whether or
not the worker had acknowledged. The coordinator also tracks capacity itself,
so it never over-assigns. An accept round-trip would add a protocol state
without changing any outcome.

## 9. Single coordinator, durable rather than highly available

Replicated coordination is a consensus problem (Raft/Paxos); doing it
credibly is a project of its own, and doing it naively would be worse than
not doing it. The chosen scope — one coordinator whose restart provably
recovers all job state, with workers that reconnect themselves — keeps every
line explainable while still exercising real distributed-systems failure
reasoning. The limitation is stated wherever guarantees are described.

## 10. No Kafka/RabbitMQ/Celery/Kubernetes

Off-the-shelf brokers and orchestrators would provide the queue, the retries,
and the failure detection — i.e. they would *be* the project. They are the
right tool in production and the wrong tool for demonstrating that one
understands what they do internally.

## 11. Deterministic, bounded, built-in tasks only

Tasks are pure computations with validated, bounded inputs; there is
deliberately no "run this shell command" task. This keeps the demo remotely
safe (no RCE surface), keeps duplicate execution harmless, and keeps tests
deterministic. `FAIL --fail-until-attempt N` exists purely to make retry
behavior observable end to end.

## 12. Timing as configuration, tests at millisecond scale

No magic numbers in code paths: heartbeat interval, timeout, sweep period,
backoff, and attempt budgets are all flags with documented defaults.
Integration tests shrink them (100 ms heartbeats, 600 ms timeout) so the full
failure suite runs in seconds without fake clocks — the same code paths run in
tests and in the live demo, just faster.

## 13. Time-bounded attempt leases (execution deadlines + cooperative cancellation)

The attempt lease started as an identity (a UUID that fences stale results).
It is now also a *time-bounded contract*: each assignment carries a deadline,
and an expired lease is revoked exactly like a lost worker. This is the
canonical distributed-systems lease (Gray & Cheriton) — the identity and the
expiry are two faces of one primitive — so it deepened the system's strongest
existing idea rather than adding a separate subsystem.

Why it was needed: heartbeat detection is worker-granular. A worker can be
perfectly alive and heartbeating while one task wedges, holding a capacity slot
forever. Nothing else in the system could observe that. The deadline is the
per-task progress signal heartbeats can't provide.

Deliberate choices and their trade-offs:

- **Coordinator clock only.** Expiry is judged against `System.currentTimeMillis`
  on the coordinator; worker clocks are never read or compared. This avoids a
  clock-synchronization dependency entirely, at the cost of the deadline
  measuring wall-clock-since-assignment (including transit and queueing), not
  pure on-worker execution time. For a timeout whose job is to catch "stuck",
  that over-approximation is the safe direction.
- **Reuse, not new machinery.** Deadline expiry funnels into the existing
  `retryOrFail`, and a revoked lease is stale-rejected by the existing result
  check. The only genuinely new code is a sweep predicate, a `TASK_CANCEL`
  message, and worker-side interruption. No new rejection path, no new retry
  path — fewer states to reason about.
- **Cooperative cancellation, not preemption.** `TASK_CANCEL` interrupts the
  task's thread; a task that ignores interruption keeps running. Hard
  preemption (`Thread.stop`) is unsafe and was rejected. Correctness never
  depends on the worker obeying — the lease is revoked coordinator-side first,
  so an uncooperative task can only produce a stale, rejected result. This is
  why the cancel can be honestly documented as best-effort without weakening
  any guarantee.
- **Recovery discards deadlines.** A persisted deadline is a dead process's
  clock reading; recovery requeues RUNNING jobs and lets the fresh assignment
  set a fresh deadline, so a stale deadline can never instant-expire a
  recovered job.

What this explicitly does **not** buy: it is not exactly-once (a timed-out task
that actually finished still causes a retry — the at-least-once window is
unchanged), and it is not a real-time guarantee (detection latency is bounded
only by the sweep interval).

## 14. Bounded admission (reject-new over buffer-forever)

Before this decision the coordinator accepted every well-formed submission. The
[measured baseline](MEASURED_BEHAVIOR.md) showed the consequence: under a
sustained overload the submit acknowledgement stayed ~4 ms while end-to-end
latency climbed to ~10 s, because excess work was silently buffered in an
in-memory map and a growing SQLite table. The coordinator had no *defined*
overload behavior. `--max-active-jobs` gives it one: at the limit, new
submissions are rejected.

- **Reject new, don't buffer forever.** An unbounded queue converts overload
  into unbounded latency and unbounded memory (Little's law: with arrival rate
  above service rate, the backlog grows without limit). Shedding the excess
  keeps admitted work's latency bounded and the coordinator's footprint bounded.
  The measured result is a ~10× lower overload tail on admitted jobs, with the
  overflow surfaced as explicit rejections instead of hidden delay.
- **Why not block the client instead?** Blocking the submit call until a slot
  frees just moves the unbounded queue into the clients' threads and hides the
  saturation behind apparent slowness. An explicit, typed, retryable rejection
  puts the backpressure decision where the client can see and act on it (retry
  with back-off, shed, or route elsewhere).
- **Why not drop already-accepted work?** Admission bounds *intake*; it never
  discards work the system already promised to run. Dropping accepted jobs would
  break the durability and at-least-once guarantees the rest of the design is
  built on. So only new submissions are gated, and retries — which are
  already-accepted work — never re-enter admission control.
- **A typed message, not `ERROR`.** Overload rejection is retryable; a
  malformed request is not. Collapsing them into one `ERROR` would force clients
  to parse strings to tell "back off and retry" from "your request is broken".
  `SUBMIT_REJECTED` carries `activeCount`/`limit`/`retryable` so the distinction
  is structural.
- **O(1), and race-free for free.** The active-job count is a single integer
  maintained in the core's state, incremented when a job enters the active set
  and decremented through one `persistRetired` chokepoint when it leaves — never
  recomputed by scanning the job map, which would make admission most expensive
  exactly under the load it is meant to protect against. Because every state
  mutation already runs on the single-writer core thread (decision 3), the
  check-and-increment needs no lock and cannot race: the same design that
  prevents double-assignment also makes "admit iff below the limit" atomic
  without any new machinery. The 8-client boundary test admits *exactly* the
  limit for this reason.

What this explicitly does **not** buy: it is not fairness (a single global limit
with no per-client quota or priority in what gets shed) and it does not raise
throughput (the service rate is still the workers' slot ceiling) — the point is
*defined* overload behavior, not a faster system.

## 15. Optional client-side submission retry (opt-in, jittered, rejection-only)

Decision 14 gives the client a typed, retryable `SUBMIT_REJECTED` and leaves the
back-off decision to the caller. This adds an *optional* client policy that
performs that back-off, without changing the default behavior or the wire.

- **Off by default; a client policy, not a protocol change.** With no flag the
  client makes exactly one attempt and surfaces the rejection, exactly as
  before. `--submit-retries N` enables up to `N` retries *after* the first
  attempt (`N + 1` total). The coordinator, the protocol, and the benchmark are
  untouched — this lives entirely in `CoordinatorClient` and a small
  `ClientRetryPolicy` / `SubmitRetrier` pair.
- **Retry the rejection, never an ambiguous failure.** The loop retries *only* a
  `SubmitRejectedException` whose wire `retryable` flag is true. A malformed
  request, a protocol violation, or any transport `IOException` is propagated on
  first sight. The reason is correctness, not caution: a rejection provably
  creates and persists nothing (asserted by the admission-control tests), so
  resending is safe; a connection that dropped *after* the coordinator accepted
  the job is ambiguous, and a blind resend would risk a duplicate submission.
  This is why the exception now reads the `retryable` field off the wire instead
  of assuming it — the client only retries when the server actually said it was
  safe.
- **This is not the coordinator's execution retry.** Decision 5's retries re-run
  a job that was *accepted* and whose attempt failed; those never re-enter
  admission control. This retries *admission itself* for a job that was never
  accepted. Keeping them in separate classes (`RetryPolicy` on the coordinator,
  `ClientRetryPolicy` on the client) keeps the two from being conflated.
- **Full jitter, bounded.** Back-off is `random in [0, min(cap, base·2^(n-1))]`.
  Full jitter (rather than a fixed exponential schedule) desynchronizes clients
  that were all rejected in the same overload spike, so they do not retry in
  lockstep and re-create it. A hard attempt cap bounds the worst-case wait to
  `(maxAttempts − 1) · cap`. Honestly, retry adds no capacity — under sustained
  overload it only shifts where load is shed; jitter and the cap keep it from
  amplifying the overload.
- **Back-off holds no lock.** The client synchronizes each wire exchange so one
  connection serves one request at a time, but the retry loop sleeps *outside*
  that critical section, so a back-off never blocks another thread's exchange on
  the same client. Sleep is injectable, so the retry decision and delay bounds
  are unit-tested deterministically with no real waiting.
- **Benchmark stays one-shot.** The load generator deliberately does not retry,
  so its accepted/rejected counts keep measuring raw admission shedding and stay
  comparable across runs.
