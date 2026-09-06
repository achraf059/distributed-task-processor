package io.github.achrafaittayeb.dtp.it;

import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import io.github.achrafaittayeb.dtp.worker.WorkerConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Shared helpers for integration tests. Everything runs in-process on
 * ephemeral ports with deliberately tiny timeouts: workers heartbeat every
 * 100 ms and are suspected dead after 600 ms, so failure scenarios settle in
 * well under a second without long sleeps.
 */
final class Testbed {

    static final long HEARTBEAT_TIMEOUT_MILLIS = 600;
    static final long SWEEP_INTERVAL_MILLIS = 50;
    /** Generous default so execution deadlines never fire in tests that aren't about them. */
    static final long TASK_TIMEOUT_MILLIS = 60_000;
    static final long WORKER_HEARTBEAT_MILLIS = 100;
    static final Duration POLL = Duration.ofMillis(50);
    static final Duration TERMINAL_TIMEOUT = Duration.ofSeconds(15);

    private Testbed() {
    }

    /** Coordinator on ephemeral ports; {@code database} may be {@code :memory:} or a temp file. */
    /** Large enough that admission control never fires unless a test asks it to. */
    static final int UNLIMITED_ACTIVE_JOBS = 1_000_000;

    static Coordinator startCoordinator(String database, int maxAttempts) throws IOException {
        return startCoordinator(database, maxAttempts, TASK_TIMEOUT_MILLIS);
    }

    static Coordinator startCoordinator(String database, int maxAttempts, long taskTimeoutMillis)
            throws IOException {
        return startCoordinator(database, maxAttempts, taskTimeoutMillis, UNLIMITED_ACTIVE_JOBS);
    }

    static Coordinator startCoordinator(String database, int maxAttempts, long taskTimeoutMillis,
                                        int maxActiveJobs) throws IOException {
        Coordinator coordinator = new Coordinator(new CoordinatorConfig(
                0, 0,
                HEARTBEAT_TIMEOUT_MILLIS, SWEEP_INTERVAL_MILLIS,
                taskTimeoutMillis,
                maxAttempts,
                50, 200,
                maxActiveJobs,
                database));
        coordinator.start();
        return coordinator;
    }

    static Worker startWorker(Coordinator coordinator, String workerId, int capacity) {
        Worker worker = new Worker(new WorkerConfig(
                "localhost", coordinator.workerPort(), workerId, capacity,
                WORKER_HEARTBEAT_MILLIS, 200));
        worker.start();
        return worker;
    }

    static CoordinatorClient connectClient(Coordinator coordinator) throws IOException {
        return new CoordinatorClient("localhost", coordinator.clientPort());
    }

    /** Polls {@code condition} every 20 ms until true or {@code timeout} elapses. */
    static void waitUntil(String description, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for: " + description);
            }
            Thread.sleep(20);
        }
    }
}
