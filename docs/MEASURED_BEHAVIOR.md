# Measured Behavior

This document records what the system actually does under load, measured with
the built-in `client bench` load generator. It is written in two parts: the
**baseline** (the unbounded system, before admission control) and the
**post-change** results (after `--max-active-jobs` was added). The engineering
point of the exercise is not a big throughput number — it is turning *undefined*
overload behavior into *defined* overload behavior, and being able to show the
before/after.

## What the benchmark is, honestly

`client bench` is a **local engineering benchmark**, not a rigorous performance
benchmark. Read the numbers as "this system compared against itself under
different configurations", never as absolute capacity claims. Specifically:

- The load generator and the system under test share one machine, so they
  compete for the same cores.
- Submit-acknowledgement latency is a genuine request/response round-trip.
  End-to-end latency (submit → observed terminal) is measured by **polling job
  status every 20 ms**, so it is an *upper bound* quantized by that interval,
  not a precise completion timestamp.
- Warm-up is approximated by an untimed batch of jobs before the timed phase,
  so the timed phase is not measuring a stone-cold JIT. It is not a rigorous
  steady-state warm-up.
- Percentiles are nearest-rank over the run's samples. No attempt is made to
  correct for coordinated omission.

Every number below was produced on the machine described next. Nothing is
fabricated or extrapolated.

## Environment

| | |
|---|---|
| Machine | Apple M1 Pro, 8 cores, 16 GB RAM |
| OS | macOS 15.7.2 |
| JDK | 23 (Temurin), project compiled `--release 21` |
| Coordinator | 1 process, default timings |
| Workers | 3 processes, capacity 4 each → **12 concurrent execution slots** |
| Client | `client bench`, connections = `--concurrency` |
| Persistence | SQLite file (durable) and `:memory:` (non-durable), as noted per run |

Reproduce with (three workers of `--capacity 4`, then):

```bash
client bench --task sha256   --jobs 2000 --concurrency 4 --warmup 50
client bench --task sleep --sleep-millis 500 --jobs 240 --concurrency 8 --warmup 5
```

## Baseline: the unbounded system

### 1. Cost of durable write-through (SHA256, 2000 jobs, concurrency 4)

`sha256` is a cheap, CPU-bound, deterministic task, so this run is dominated by
coordinator bookkeeping and persistence rather than task execution.

| Persistence | Throughput | Submit ack p50 / p99 | End-to-end p50 / p99 |
|---|---|---|---|
| SQLite file (durable) | **896 jobs/s** | 4.2 / 6.4 ms | 1110 / 2142 ms |
| `:memory:` (non-durable) | **4843 jobs/s** | 0.5 / 2.2 ms | 165 / 341 ms |

**Finding.** Durable write-through costs roughly **5.4× throughput** here (896
vs 4843 jobs/s). Every job state transition is written through to SQLite
synchronously on the single coordinator core thread (QUEUED → RUNNING →
COMPLETED is several writes), and those synchronous writes — not the SHA-256
work — are the bottleneck for cheap tasks. This is the price of the durability
guarantee that makes coordinator restart recovery possible, and it is a
deliberate, documented trade-off (see
[DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) ADR 7). The comparison is valid
because *only* the repository implementation changes between the two runs;
everything else is identical.

### 2. Overload behavior (sleep 500 ms, 240 jobs, concurrency 8)

With 12 execution slots and 500 ms tasks, the system's sustained service rate is
about `12 / 0.5 s = 24 jobs/s`. Submitting 240 such jobs deliberately exceeds
what the workers can service concurrently, so the excess must go *somewhere*.

| | Value |
|---|---|
| Accepted | 240 / 240 (nothing rejected) |
| Throughput | 23.6 jobs/s (≈ the 24 jobs/s slot ceiling, as expected) |
| **Submit ack** p50 / p99 | **3.9 / 19.1 ms** (fast and flat) |
| **End-to-end** p50 / p99 / max | **5040 / 10028 / 10039 ms** |

**Finding — this is the limitation the milestone fixes.** The coordinator
accepts *all* 240 jobs almost instantly (submit ack stays around 4 ms), while
end-to-end latency climbs to ~5 s at the median and ~10 s at the tail. The two
numbers diverge because the excess work is silently buffered in the coordinator:

- **The client gets no backpressure signal.** A fast "accepted" is returned
  regardless of how deep the queue already is, so a client (or a retry storm, or
  a burst of clients) can pile on unboundedly. The in-memory `jobs` map and the
  SQLite table both grow with every accepted job.
- **End-to-end latency grows with queue depth**, not with the task itself. A
  500 ms task takes 10 s to finish once it is 20 waves deep. This is Little's
  law in miniature: with arrival rate above service rate, the queue — and
  therefore the wait — grows without bound until submission stops.
- **Nothing pushes back and nothing sheds load.** The only reason the run ended
  at all is that the benchmark submitted a finite batch. A truly open-loop
  source would drive latency and memory up indefinitely.

The pathology is not "the system is slow" — 24 jobs/s is exactly the honest slot
ceiling. The pathology is that **overload has no defined behavior**: the system
neither refuses work nor signals that it is saturated. That is what admission
control changes.

## Admission control: the change

`--max-active-jobs N` caps the number of active (non-terminal) jobs the
coordinator will hold. A submission that arrives when the active-job count is
already at the limit is refused with a typed `SUBMIT_REJECTED` reply
(`retryable = true`) instead of being queued. The count is an O(1) running
counter kept inside the single-writer core — it is never recomputed by scanning
the job map, so the admission check does not itself get more expensive as load
rises. See [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) ADR 14 for the reject-new
rationale.

## Post-change: the same overload, now bounded

Same offered load as the baseline overload run — 240× 500 ms sleep jobs from 8
connections, 12 execution slots — but with `--max-active-jobs 24` (twice the
slot count, so a full complement can run with an equal number queued):

| | Baseline (unbounded) | Bounded (`--max-active-jobs 24`) |
|---|---|---|
| Accepted | 240 | **24** |
| Rejected | 0 | **216** |
| Completed throughput | 23.6 jobs/s | 22.8 jobs/s |
| Submit ack p50 / p99 | 3.9 / 19.1 ms | 7.7 / 13.1 ms |
| **End-to-end p50 / p99 / max** | **5040 / 10028 / 10039 ms** | **537 / 1033 / 1033 ms** |

**Interpretation.**

- **Overload latency is now bounded.** End-to-end p99 for admitted work fell
  from ~10 s to ~1 s — roughly a 10× reduction — and, more importantly, it is
  now *bounded by design*: with at most 24 active jobs over 12 slots, no job
  waits behind more than about one extra wave, so the tail sits near
  `(24 / 12) × 500 ms ≈ 1 s`, exactly as measured. In the unbounded run the tail
  grew with the size of the submitted burst; here it cannot.
- **The system did not get faster, and that is the point.** Completed
  throughput is essentially unchanged (22.8 vs 23.6 jobs/s) — it was always the
  honest 12-slot ceiling. What changed is that excess load is now *shed
  explicitly* (216 typed rejections a client can act on) instead of being
  silently absorbed into an ever-growing queue. The intended result of this
  milestone is **defined overload behavior**, not higher peak throughput.
- **Backpressure reaches the client.** In the baseline the client had no way to
  know the system was saturated — every submit returned "accepted" in ~4 ms.
  Now a saturated coordinator says so, and says it in a way the client can
  distinguish from a malformed-request error and safely retry after a back-off.

### Admission control is free when the limit is not hit

Re-running the healthy SHA256 throughput scenarios with the default limit
(10 000, far above the 2000-job workload) shows the O(1) counter check adds no
measurable cost:

| Persistence | Baseline throughput | Post-change throughput |
|---|---|---|
| SQLite file (durable) | 896 jobs/s | 897 jobs/s |
| `:memory:` (non-durable) | 4843 jobs/s | 4751 jobs/s |

The differences are within run-to-run noise (the small in-memory dip is not
reproducible in direction across runs). Admission control costs one integer
comparison per submission on a thread that is already serializing all state
changes, so below the limit it is effectively invisible.

## Benchmark limitations (recap)

- Local, single-machine, generator and system co-resident — treat all absolute
  numbers as this-machine-only.
- End-to-end latency is polled at 20 ms granularity, so it is an upper bound.
- Percentiles are nearest-rank with no coordinated-omission correction.
- Warm-up is approximate. Runs are short (hundreds to a few thousand jobs).
- The overload demonstration deliberately floods a small, fixed batch; a
  true open-loop load source would make the *unbounded* case worse (unbounded
  latency and memory), which only sharpens the contrast — it does not change the
  bounded case.

The value here is the **before/after contrast under identical offered load**,
produced by the same tool against the same system, not the raw magnitudes.
