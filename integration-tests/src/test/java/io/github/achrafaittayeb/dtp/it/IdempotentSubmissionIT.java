package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.client.SubmitRejectedException;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Durable idempotent submission: a client-supplied key deduplicates logical
 * submissions. Deduplication is checked before admission control (a known key
 * creates no job, so it bypasses the active-job limit), the mapping survives a
 * coordinator restart, and a key stays bound to its job through terminal states.
 * This dedups <em>submission</em> only — execution stays at-least-once.
 */
class IdempotentSubmissionIT {

    @TempDir
    Path tempDir;

    private static String submit(CoordinatorClient client, String text, int maxAttempts, String key)
            throws IOException {
        return client.submit(TaskType.SHA256,
                JsonNodeFactory.instance.objectNode().put("text", text), maxAttempts, key);
    }

    private static String submitShaKeyed(CoordinatorClient client, String key) throws IOException {
        return submit(client, "x", 0, key);
    }

    @Test
    void sameKeyTwiceReturnsSameJobId() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String first = submitShaKeyed(client, "dup-key");
            String second = submitShaKeyed(client, "dup-key");

            assertThat(second).isEqualTo(first);
            assertThat(client.listJobs()).hasSize(1);
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
        }
    }

    @Test
    void submissionsWithoutKeyCreateDistinctJobs() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String first = submitShaKeyed(client, null);
            String second = submitShaKeyed(client, null);

            assertThat(second).isNotEqualTo(first);
            assertThat(client.listJobs()).hasSize(2);
            assertThat(coordinator.activeJobCount()).isEqualTo(2);
        }
    }

    @Test
    void knownKeyAtAdmissionLimitReturnsOriginalIdWithoutRejection() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String original = submitShaKeyed(client, "at-limit"); // fills the only slot
            assertThat(coordinator.activeJobCount()).isEqualTo(1);

            // Resubmitting the known key bypasses admission (creates no new job).
            String again = submitShaKeyed(client, "at-limit");
            assertThat(again).isEqualTo(original);
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void newKeyAtAdmissionLimitIsRejected() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            submitShaKeyed(client, "first"); // fills the only slot
            assertThat(coordinator.activeJobCount()).isEqualTo(1);

            // A genuinely new key is new work and still passes through admission.
            assertThatThrownBy(() -> submitShaKeyed(client, "second"))
                    .isInstanceOf(SubmitRejectedException.class);
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void sameKeyDifferentPayloadIsConflictAndLeavesOriginalUnchanged() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String original = submit(client, "original-text", 0, "conflict-key");

            assertThatThrownBy(() -> submit(client, "different-text", 0, "conflict-key"))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SubmitRejectedException.class)
                    .hasMessageContaining("Idempotency key");

            // The original job is untouched: same id, still the only job, unchanged payload.
            assertThat(client.listJobs()).hasSize(1);
            assertThat(client.status(original).state()).isEqualTo(JobState.QUEUED);
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
        }
    }

    @Test
    void sameKeyDifferentTaskTypeIsConflict() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            client.submit(TaskType.SHA256,
                    JsonNodeFactory.instance.objectNode().put("text", "x"), 0, "type-key");

            assertThatThrownBy(() -> client.submit(TaskType.WORD_COUNT,
                    JsonNodeFactory.instance.objectNode().put("text", "x"), 0, "type-key"))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SubmitRejectedException.class)
                    .hasMessageContaining("Idempotency key");
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void sameKeyDifferentMaxAttemptsIsConflict() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            submit(client, "x", 2, "attempts-key");

            assertThatThrownBy(() -> submit(client, "x", 5, "attempts-key"))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SubmitRejectedException.class)
                    .hasMessageContaining("Idempotency key");
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void keyMappingSurvivesRestartAndRecovery() throws Exception {
        String database = tempDir.resolve("idempotency-recovery.db").toString();

        String originalId;
        try (Coordinator first = Testbed.startCoordinator(database, 3, Testbed.TASK_TIMEOUT_MILLIS, 100)) {
            try (CoordinatorClient client = Testbed.connectClient(first)) {
                originalId = submitShaKeyed(client, "persist-key"); // no worker → stays QUEUED
            }
            first.close();
        }

        // Same database, fresh coordinator: the dedup map is rebuilt from storage.
        try (Coordinator second = Testbed.startCoordinator(database, 3, Testbed.TASK_TIMEOUT_MILLIS, 100)) {
            try (CoordinatorClient client = Testbed.connectClient(second)) {
                String afterRestart = submitShaKeyed(client, "persist-key");
                assertThat(afterRestart).isEqualTo(originalId); // deduped to the pre-restart job
                assertThat(client.listJobs()).hasSize(1);
            }
        }
    }

    @Test
    void completedJobStillDeduplicates() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String id = submitShaKeyed(client, "done-key");
            assertThat(client.awaitTerminal(id, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.COMPLETED);
            assertThat(coordinator.activeJobCount()).isZero();

            // Resubmitting the key returns the completed job — it does NOT re-run.
            String again = submitShaKeyed(client, "done-key");
            assertThat(again).isEqualTo(id);
            assertThat(client.status(again).state()).isEqualTo(JobState.COMPLETED);
            assertThat(coordinator.activeJobCount()).isZero();
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void cancelledJobStillDeduplicates() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String id = submitShaKeyed(client, "cancel-key"); // no worker → QUEUED
            client.cancel(id);
            assertThat(client.status(id).state()).isEqualTo(JobState.CANCELLED);

            String again = submitShaKeyed(client, "cancel-key");
            assertThat(again).isEqualTo(id);
            assertThat(client.status(again).state()).isEqualTo(JobState.CANCELLED);
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void failedJobStillDeduplicates() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 1, Testbed.TASK_TIMEOUT_MILLIS, 100);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String id = client.submit(TaskType.FAIL,
                    JsonNodeFactory.instance.objectNode().put("failUntilAttempt", 999), 0, "fail-key");
            assertThat(client.awaitTerminal(id, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.FAILED);

            String again = client.submit(TaskType.FAIL,
                    JsonNodeFactory.instance.objectNode().put("failUntilAttempt", 999), 0, "fail-key");
            assertThat(again).isEqualTo(id);
            assertThat(client.status(again).state()).isEqualTo(JobState.FAILED);
            assertThat(client.listJobs()).hasSize(1);
        }
    }

    @Test
    void concurrentDuplicateSubmissionsCreateExactlyOneJob() throws Exception {
        int clients = 8;
        Set<String> returnedIds = new CopyOnWriteArraySet<>();
        Set<Throwable> failures = new CopyOnWriteArraySet<>();

        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 100)) {

            CountDownLatch startGate = new CountDownLatch(1);
            java.util.List<Thread> threads = new java.util.ArrayList<>();
            for (int i = 0; i < clients; i++) {
                Thread thread = new Thread(() -> {
                    try (CoordinatorClient client = Testbed.connectClient(coordinator)) {
                        startGate.await();
                        returnedIds.add(submitShaKeyed(client, "race-key"));
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                }, "submitter-" + i);
                threads.add(thread);
                thread.start();
            }
            startGate.countDown();
            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(failures).isEmpty();
            assertThat(returnedIds).hasSize(1); // every client saw the same single job
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
        }
    }
}
