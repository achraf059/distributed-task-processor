package io.github.achrafaittayeb.dtp.client;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Concurrent load generator for the coordinator: submits a batch of
 * deterministic jobs from several client connections at once, waits for every
 * job to reach a terminal state, and reports throughput plus latency
 * percentiles.
 *
 * <p>This is a local engineering benchmark, not a rigorous performance
 * benchmark: generator and system share one machine, latencies are measured
 * by request/response round-trips and status polling (end-to-end numbers are
 * an upper bound, quantized by the poll interval), and JIT warm-up is only
 * approximated by an optional warm-up batch. Its purpose is comparing this
 * system against itself under different configurations, not producing
 * absolute numbers.
 *
 * <p>Each generator thread owns a private {@link CoordinatorClient}
 * connection — the client is a blocking one-request-in-flight protocol and is
 * not meant to be shared across threads.
 */
public final class Bench {

    /** One benchmark run's parameters. */
    public record Config(
            String host,
            int port,
            int jobs,
            int concurrency,
            int warmupJobs,
            TaskType taskType,
            ObjectNode payload,
            long waitTimeoutMillis) {

        public Config {
            if (jobs < 1) {
                throw new IllegalArgumentException("jobs must be >= 1");
            }
            if (concurrency < 1 || concurrency > jobs) {
                throw new IllegalArgumentException("concurrency must be in 1..jobs");
            }
            if (warmupJobs < 0) {
                throw new IllegalArgumentException("warmup must be >= 0");
            }
        }
    }

    /** Nearest-rank latency percentiles over one measured phase, in milliseconds. */
    public record LatencyStats(long count, double p50, double p95, double p99, double max) {

        static LatencyStats of(List<Long> nanos) {
            if (nanos.isEmpty()) {
                return new LatencyStats(0, 0, 0, 0, 0);
            }
            long[] sorted = nanos.stream().mapToLong(Long::longValue).sorted().toArray();
            return new LatencyStats(sorted.length,
                    millis(percentile(sorted, 50)),
                    millis(percentile(sorted, 95)),
                    millis(percentile(sorted, 99)),
                    millis(sorted[sorted.length - 1]));
        }

        /** Nearest-rank percentile: the smallest value with at least p% of samples at or below it. */
        private static long percentile(long[] sorted, int p) {
            int rank = (int) Math.ceil(p / 100.0 * sorted.length);
            return sorted[Math.max(0, rank - 1)];
        }

        private static double millis(long nanos) {
            return nanos / 1_000_000.0;
        }
    }

    /** Aggregated outcome of a benchmark run. */
    public record Report(
            Config config,
            int accepted,
            int rejected,
            int completed,
            int failed,
            long elapsedMillis,
            LatencyStats submitAck,
            LatencyStats endToEnd) {

        /** Completed jobs per second over the whole timed phase (submit of first to finish of last). */
        public double throughputPerSecond() {
            return elapsedMillis == 0 ? 0 : completed * 1000.0 / elapsedMillis;
        }

        public void print(PrintStream out) {
            out.printf("Benchmark finished in %.1f s%n", elapsedMillis / 1000.0);
            out.printf("  Jobs:        %d requested, %d accepted, %d rejected%n",
                    config.jobs(), accepted, rejected);
            out.printf("  Outcomes:    %d completed, %d failed%n", completed, failed);
            out.printf("  Throughput:  %.1f completed jobs/s%n", throughputPerSecond());
            out.printf("  Submit ack:  p50 %.1f ms   p95 %.1f ms   p99 %.1f ms   max %.1f ms%n",
                    submitAck.p50(), submitAck.p95(), submitAck.p99(), submitAck.max());
            out.printf("  End-to-end:  p50 %.1f ms   p95 %.1f ms   p99 %.1f ms   max %.1f ms%n",
                    endToEnd.p50(), endToEnd.p95(), endToEnd.p99(), endToEnd.max());
            out.println("  (end-to-end = submit to observed-terminal via polling; upper bound)");
        }
    }

    private final Config config;

    public Bench(Config config) {
        this.config = config;
    }

    /**
     * Runs warm-up (untimed, sequential, one connection) and then the timed
     * phase: {@code concurrency} threads submit their share of the jobs and
     * poll them to terminal state.
     */
    public Report run() throws Exception {
        if (config.warmupJobs() > 0) {
            warmUp();
        }

        List<GeneratorThread> generators = new ArrayList<>();
        CountDownLatch startGate = new CountDownLatch(1);
        int perThread = config.jobs() / config.concurrency();
        int remainder = config.jobs() % config.concurrency();
        for (int i = 0; i < config.concurrency(); i++) {
            int share = perThread + (i < remainder ? 1 : 0);
            generators.add(new GeneratorThread(i, share, startGate));
        }
        for (GeneratorThread generator : generators) {
            generator.thread.start();
        }

        long startedAt = System.nanoTime();
        startGate.countDown();
        for (GeneratorThread generator : generators) {
            generator.thread.join();
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        int accepted = 0;
        int rejected = 0;
        int completed = 0;
        int failed = 0;
        List<Long> submitAckNanos = new ArrayList<>();
        List<Long> endToEndNanos = new ArrayList<>();
        Exception firstFailure = null;
        for (GeneratorThread generator : generators) {
            accepted += generator.accepted;
            rejected += generator.rejected;
            completed += generator.completed;
            failed += generator.failed;
            submitAckNanos.addAll(generator.submitAckNanos);
            endToEndNanos.addAll(generator.endToEndNanos);
            if (firstFailure == null && generator.failure != null) {
                firstFailure = generator.failure;
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
        return new Report(config, accepted, rejected, completed, failed, elapsedMillis,
                LatencyStats.of(submitAckNanos), LatencyStats.of(endToEndNanos));
    }

    /** Submits and awaits a small untimed batch so JIT and connections are not stone cold. */
    private void warmUp() throws Exception {
        try (CoordinatorClient client = new CoordinatorClient(config.host(), config.port())) {
            List<String> jobIds = new ArrayList<>();
            for (int i = 0; i < config.warmupJobs(); i++) {
                jobIds.add(client.submit(config.taskType(), config.payload(), 0));
            }
            for (String jobId : jobIds) {
                client.awaitTerminal(jobId, Duration.ofMillis(20),
                        Duration.ofMillis(config.waitTimeoutMillis()));
            }
        }
    }

    /**
     * One generator: submits its share of jobs on a private connection,
     * recording per-job submit-acknowledgement latency, then polls its own
     * jobs until all are terminal, recording submit-to-observed-terminal
     * latency.
     */
    private final class GeneratorThread {

        final Thread thread;
        final List<Long> submitAckNanos = new ArrayList<>();
        final List<Long> endToEndNanos = new ArrayList<>();
        int accepted;
        int rejected;
        int completed;
        int failed;
        Exception failure;

        private final int jobsToSubmit;
        private final CountDownLatch startGate;

        GeneratorThread(int index, int jobsToSubmit, CountDownLatch startGate) {
            this.jobsToSubmit = jobsToSubmit;
            this.startGate = startGate;
            this.thread = new Thread(this::generate, "bench-" + index);
        }

        private void generate() {
            try (CoordinatorClient client = new CoordinatorClient(config.host(), config.port())) {
                startGate.await();
                Deque<PendingJob> pending = submitAll(client);
                awaitAll(client, pending);
            } catch (Exception e) {
                failure = e;
            }
        }

        private Deque<PendingJob> submitAll(CoordinatorClient client) throws IOException {
            Deque<PendingJob> pending = new ArrayDeque<>();
            for (int i = 0; i < jobsToSubmit; i++) {
                long before = System.nanoTime();
                String jobId = client.submit(config.taskType(), config.payload(), 0);
                submitAckNanos.add(System.nanoTime() - before);
                accepted++;
                pending.add(new PendingJob(jobId, before));
            }
            return pending;
        }

        /** Round-robin polls this thread's jobs until every one is terminal. */
        private void awaitAll(CoordinatorClient client, Deque<PendingJob> pending)
                throws IOException, InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(config.waitTimeoutMillis());
            while (!pending.isEmpty()) {
                if (System.nanoTime() > deadline) {
                    throw new IOException(pending.size() + " jobs still not terminal after "
                            + config.waitTimeoutMillis() + " ms");
                }
                int stillPending = pending.size();
                for (int i = 0; i < stillPending; i++) {
                    PendingJob job = pending.removeFirst();
                    JobSnapshot snapshot = client.status(job.jobId());
                    if (snapshot.state().isTerminal()) {
                        endToEndNanos.add(System.nanoTime() - job.submittedAtNanos());
                        if (snapshot.state() == JobState.COMPLETED) {
                            completed++;
                        } else {
                            failed++;
                        }
                    } else {
                        pending.addLast(job);
                    }
                }
                if (!pending.isEmpty()) {
                    Thread.sleep(20);
                }
            }
        }
    }

    private record PendingJob(String jobId, long submittedAtNanos) {
    }

    /** Builds the deterministic payload for a benchmark task type. */
    public static ObjectNode payloadFor(TaskType taskType, long sleepMillis, long primeLimit) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        switch (taskType) {
            case SHA256 -> payload.put("text", "distributed-task-processor-bench");
            case SLEEP -> payload.put("durationMillis", sleepMillis);
            case PRIME_COUNT -> payload.put("limit", primeLimit);
            default -> throw new IllegalArgumentException(
                    "Unsupported bench task type: " + taskType
                            + " (use one of: sha256, sleep, prime-count)");
        }
        return payload;
    }

    /** Maps the CLI task name to a benchable {@link TaskType}. */
    public static TaskType taskTypeFor(String name) {
        return switch (name) {
            case "sha256" -> TaskType.SHA256;
            case "sleep" -> TaskType.SLEEP;
            case "prime-count" -> TaskType.PRIME_COUNT;
            default -> throw new IllegalArgumentException(
                    "Unsupported bench task type: " + name
                            + " (use one of: sha256, sleep, prime-count)");
        };
    }
}
