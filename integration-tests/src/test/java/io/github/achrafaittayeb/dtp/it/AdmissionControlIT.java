package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.client.SubmitRejectedException;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Scenario G: admission control. The coordinator holds at most
 * {@code --max-active-jobs} active (non-terminal) jobs; further submissions are
 * rejected with a typed {@code SUBMIT_REJECTED}, and the O(1) active-job counter
 * stays exact across completion, failure, cancellation, retries, and recovery.
 *
 * <p>Most tests run with <em>no workers</em>, so submitted jobs sit in QUEUED
 * (active) indefinitely — that pins the active set to exactly what was submitted
 * and removes execution timing from the assertions.
 */
class AdmissionControlIT {

    @TempDir
    Path tempDir;

    private static String submitSha(CoordinatorClient client) throws IOException {
        return client.submit(TaskType.SHA256,
                JsonNodeFactory.instance.objectNode().put("text", "x"), 0);
    }

    /** Attempts one submission; returns true if it was rejected as overloaded. */
    private static boolean rejected(CoordinatorClient client) throws IOException {
        try {
            submitSha(client);
            return false;
        } catch (SubmitRejectedException overloaded) {
            return true;
        }
    }

    @Test
    void acceptsUpToLimitThenRejectsWithTypedReply() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 5);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            for (int i = 0; i < 5; i++) {
                submitSha(client); // no worker → stays QUEUED (active)
            }
            assertThat(coordinator.activeJobCount()).isEqualTo(5);

            // The 6th submission is a valid request refused under overload.
            assertThatThrownBy(() -> submitSha(client))
                    .isInstanceOf(SubmitRejectedException.class)
                    .satisfies(thrown -> {
                        SubmitRejectedException e = (SubmitRejectedException) thrown;
                        assertThat(e.activeCount()).isEqualTo(5);
                        assertThat(e.limit()).isEqualTo(5);
                        assertThat(e.retryable()).isTrue();
                    });

            // Rejection must not have created or persisted anything.
            assertThat(coordinator.activeJobCount()).isEqualTo(5);
            assertThat(client.listJobs()).hasSize(5);
        }
    }

    @Test
    void completionFreesCapacity() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String first = submitSha(client);
            assertThat(client.awaitTerminal(first, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.COMPLETED);
            assertThat(coordinator.activeJobCount()).isZero();

            // Slot freed → a new submission is accepted.
            String second = submitSha(client);
            assertThat(client.awaitTerminal(second, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.COMPLETED);
        }
    }

    @Test
    void permanentFailureFreesCapacity() throws Exception {
        // maxAttempts 1 so the first failure is terminal.
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 1, Testbed.TASK_TIMEOUT_MILLIS, 1);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String failing = client.submit(TaskType.FAIL,
                    JsonNodeFactory.instance.objectNode().put("failUntilAttempt", 999), 0);
            assertThat(client.awaitTerminal(failing, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.FAILED);
            assertThat(coordinator.activeJobCount()).isZero();

            assertThat(rejected(client)).isFalse(); // capacity is free again
        }
    }

    @Test
    void cancellationFreesCapacity() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String queued = submitSha(client); // no worker → QUEUED (active)
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
            assertThat(rejected(client)).isTrue(); // at limit

            client.cancel(queued); // synchronous → CANCELLED, counter freed
            assertThat(coordinator.activeJobCount()).isZero();

            assertThat(rejected(client)).isFalse(); // capacity available again
        }
    }

    @Test
    void retryDoesNotFreeOrConsumeAnExtraSlot() throws Exception {
        // A ScriptedWorker gives exact control over when the attempt fails and
        // when the retry succeeds, so the counter can be sampled at known points.
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             ScriptedWorker worker = ScriptedWorker.register(coordinator.workerPort(), "w1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String jobId = submitSha(client);
            TaskAssign attempt1 = worker.awaitAssignment(); // RUNNING, 1 active
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
            assertThat(rejected(client)).isTrue(); // single slot held while RUNNING

            worker.sendFailure(attempt1, "boom"); // RUNNING → RETRY_WAIT → requeued

            // awaitAssignment blocks until the coordinator has processed the
            // failure and reassigned the retry, so no transient state needs to be
            // caught. The active count is 1 in every non-terminal state, so the
            // retry neither freed the slot (would drop to 0) nor consumed an extra
            // one (would rise to 2).
            TaskAssign attempt2 = worker.awaitAssignment();
            assertThat(attempt2.attemptNumber()).isEqualTo(2);
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
            assertThat(rejected(client)).isTrue(); // still the same single slot

            worker.sendSuccess(attempt2, "ok"); // → COMPLETED
            Testbed.waitUntil("job completed", Testbed.TERMINAL_TIMEOUT,
                    () -> uncheckedState(client, jobId) == JobState.COMPLETED);
            assertThat(coordinator.activeJobCount()).isZero(); // freed exactly once
        }
    }

    @Test
    void concurrentClientsAtBoundaryAdmitExactlyTheLimit() throws Exception {
        int limit = 20;
        int clients = 8;
        int perClient = 10; // 80 attempts against 20 slots, no workers
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger refused = new AtomicInteger();
        List<Throwable> unexpected = new CopyOnWriteArrayList<>();

        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, limit)) {

            CountDownLatch startGate = new CountDownLatch(1);
            List<Thread> threads = new java.util.ArrayList<>();
            for (int c = 0; c < clients; c++) {
                Thread thread = new Thread(() -> {
                    try (CoordinatorClient client = Testbed.connectClient(coordinator)) {
                        startGate.await();
                        for (int i = 0; i < perClient; i++) {
                            if (rejected(client)) {
                                refused.incrementAndGet();
                            } else {
                                accepted.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        unexpected.add(e);
                    }
                }, "submitter-" + c);
                threads.add(thread);
                thread.start();
            }
            startGate.countDown();
            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(unexpected).isEmpty();
            assertThat(accepted).hasValue(limit);
            assertThat(refused).hasValue(clients * perClient - limit);
            assertThat(coordinator.activeJobCount()).isEqualTo(limit);
        }
    }

    @Test
    void recoveryReconstructsActiveCountIgnoringTerminalJobs() throws Exception {
        String database = tempDir.resolve("admission-recovery.db").toString();

        // First lifetime: 2 jobs completed (terminal), 3 left QUEUED (active).
        try (Coordinator first = Testbed.startCoordinator(database, 3, Testbed.TASK_TIMEOUT_MILLIS, 100)) {
            try (Worker worker = Testbed.startWorker(first, "worker-1", 2);
                 CoordinatorClient client = Testbed.connectClient(first)) {
                for (int i = 0; i < 2; i++) {
                    String id = submitSha(client);
                    assertThat(client.awaitTerminal(id, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                            .isEqualTo(JobState.COMPLETED);
                }
            }
            // Worker is now gone; these three stay QUEUED (active).
            try (CoordinatorClient client = Testbed.connectClient(first)) {
                for (int i = 0; i < 3; i++) {
                    submitSha(client);
                }
                assertThat(first.activeJobCount()).isEqualTo(3);
            }
            first.close();
        }

        // Second lifetime on the same database: only the 3 non-terminal jobs count.
        try (Coordinator second = Testbed.startCoordinator(database, 3, Testbed.TASK_TIMEOUT_MILLIS, 100)) {
            assertThat(second.activeJobCount()).isEqualTo(3);
        }
    }

    @Test
    void recoveryAboveLoweredLimitRejectsNewWorkUntilDrained() throws Exception {
        String database = tempDir.resolve("admission-overage.db").toString();

        // First lifetime: 5 active jobs under a generous limit, no workers.
        try (Coordinator first = Testbed.startCoordinator(database, 3, Testbed.TASK_TIMEOUT_MILLIS, 5)) {
            try (CoordinatorClient client = Testbed.connectClient(first)) {
                for (int i = 0; i < 5; i++) {
                    submitSha(client);
                }
                assertThat(first.activeJobCount()).isEqualTo(5);
            }
            first.close();
        }

        // Restart with a lowered limit of 2: the 5 recovered jobs remain, but new
        // submissions are rejected until enough recovered jobs drain below 2.
        try (Coordinator second = Testbed.startCoordinator(database, 3, Testbed.TASK_TIMEOUT_MILLIS, 2)) {
            assertThat(second.activeJobCount()).isEqualTo(5); // above the new limit

            try (CoordinatorClient client = Testbed.connectClient(second)) {
                assertThat(client.listJobs()).hasSize(5); // recovered work kept
                assertThat(rejected(client)).isTrue();     // new work refused (5 >= 2)
            }

            // Attaching a worker drains the recovered jobs; once the active count
            // drops below the limit, submissions are accepted again.
            try (Worker worker = Testbed.startWorker(second, "worker-1", 4)) {
                Testbed.waitUntil("recovered jobs drained below the lowered limit",
                        Testbed.TERMINAL_TIMEOUT, () -> second.activeJobCount() < 2);
                try (CoordinatorClient client = Testbed.connectClient(second)) {
                    assertThat(rejected(client)).isFalse();
                }
            }
        }
    }

    private static JobState uncheckedState(CoordinatorClient client, String jobId) {
        try {
            return client.status(jobId).state();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
