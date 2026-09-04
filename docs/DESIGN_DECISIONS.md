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
