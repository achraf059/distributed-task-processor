package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.worker.Worker;
import io.github.achrafaittayeb.dtp.worker.WorkerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario F: durability. The coordinator process dies and restarts on the
 * same SQLite file; completed jobs keep their results and an in-flight job is
 * requeued under the documented recovery rule and finishes.
 */
class CoordinatorRestartIT {

    @TempDir
    Path tempDir;

    @Test
    void jobsSurviveCoordinatorRestartAndInFlightWorkResumes() throws Exception {
        String database = tempDir.resolve("restart.db").toString();
        String completedJobId;
        String inFlightJobId;

        // --- First coordinator lifetime -------------------------------------
        // The scripted worker must still be connected when the coordinator goes
        // down, otherwise the disconnect would already have invalidated the
        // in-flight attempt — this test is about dying with a RUNNING job.
        Coordinator first = Testbed.startCoordinator(database, 3);
        ScriptedWorker worker = ScriptedWorker.register(first.workerPort(), "w1", 2);
        try (CoordinatorClient client = Testbed.connectClient(first)) {
            final String submittedId = client.submit(TaskType.SHA256,
                    JsonNodeFactory.instance.objectNode().put("text", "abc"), 0);
            completedJobId = submittedId;
            worker.sendSuccess(worker.awaitAssignment(), "hash-of-abc");
            Testbed.waitUntil("first job completed", Testbed.TERMINAL_TIMEOUT,
                    () -> uncheckedState(client, submittedId) == JobState.COMPLETED);

            // Second job is RUNNING (assigned, never finished) when the
            // coordinator "crashes".
            inFlightJobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 100), 0);
            worker.awaitAssignment();
            assertThat(client.status(inFlightJobId).state()).isEqualTo(JobState.RUNNING);
        } finally {
            first.close();  // "crash" while the job is RUNNING
            worker.close();
        }

        // --- Second coordinator lifetime on the same database ---------------
        try (Coordinator second = Testbed.startCoordinator(database, 3);
             CoordinatorClient client = Testbed.connectClient(second)) {

            // Completed work is fully recovered.
            JobSnapshot recovered = client.status(completedJobId);
            assertThat(recovered.state()).isEqualTo(JobState.COMPLETED);
            assertThat(recovered.result()).isEqualTo("hash-of-abc");

            // The interrupted job was requeued (attempt 1 spent, lease discarded).
            JobSnapshot requeued = client.status(inFlightJobId);
            assertThat(requeued.state()).isEqualTo(JobState.QUEUED);
            assertThat(requeued.attempts()).isEqualTo(1);
            assertThat(requeued.workerId()).isNull();

            // A real worker joins the new coordinator and the job completes.
            try (Worker realWorker = Testbed.startWorker(second, "w2", 1)) {
                JobSnapshot done = client.awaitTerminal(
                        inFlightJobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
                assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                assertThat(done.result()).isEqualTo("slept 100 ms");
                assertThat(done.attempts()).isEqualTo(2);
            }
        }
    }

    @Test
    void recoveryFailsJobWhoseFinalAttemptWasInterrupted() throws Exception {
        String database = tempDir.resolve("final-attempt.db").toString();
        String jobId;

        Coordinator first = Testbed.startCoordinator(database, 1);
        ScriptedWorker worker = ScriptedWorker.register(first.workerPort(), "w1", 1);
        try (CoordinatorClient client = Testbed.connectClient(first)) {
            jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 100), 1);
            worker.awaitAssignment();
        } finally {
            first.close();  // "crash" during the job's only attempt
            worker.close();
        }

        try (Coordinator second = Testbed.startCoordinator(database, 1);
             CoordinatorClient client = Testbed.connectClient(second)) {
            JobSnapshot job = client.status(jobId);
            assertThat(job.state()).isEqualTo(JobState.FAILED);
            assertThat(job.error()).contains("coordinator restarted during final attempt");
        }
    }

    @Test
    void recoveryIgnoresStaleDeadlineAndRequeuesInsteadOfExpiring() throws Exception {
        // A RUNNING job's deadline is a coordinator-clock instant from the old
        // process. Recovery must NOT resurrect that assignment and let its
        // deadline "expire" it — it requeues (lease discarded) like any other
        // in-flight job, and the fresh assignment gets a fresh deadline.
        String database = tempDir.resolve("stale-deadline.db").toString();
        String jobId;

        // Tiny task timeout: if recovery wrongly kept the old deadline, the job
        // would already be "expired" the instant it loads.
        Coordinator first = Testbed.startCoordinator(database, 3, 200);
        ScriptedWorker worker = ScriptedWorker.register(first.workerPort(), "w1", 1);
        try (CoordinatorClient client = Testbed.connectClient(first)) {
            jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 3);
            worker.awaitAssignment();
            assertThat(client.status(jobId).state()).isEqualTo(JobState.RUNNING);
        } finally {
            first.close();
            worker.close();
        }

        // Let real time pass the old deadline before the new coordinator starts.
        Thread.sleep(400);

        try (Coordinator second = Testbed.startCoordinator(database, 3, 200);
             CoordinatorClient client = Testbed.connectClient(second)) {
            // Requeued by the recovery rule, not failed by a stale deadline.
            JobSnapshot recovered = client.status(jobId);
            assertThat(recovered.state()).isEqualTo(JobState.QUEUED);
            assertThat(recovered.attempts()).isEqualTo(1);

            try (Worker realWorker = Testbed.startWorker(second, "w2", 1)) {
                JobSnapshot done = client.awaitTerminal(
                        jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
                assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                assertThat(done.attempts()).isEqualTo(2);
            }
        }
    }

    @Test
    void workerAutomaticallyReconnectsToRestartedCoordinatorOnSamePorts() throws Exception {
        String database = tempDir.resolve("reconnect.db").toString();

        // Fixed ports so the restarted coordinator is reachable at the same address.
        int workerPort;
        int clientPort;
        Coordinator first = Testbed.startCoordinator(database, 3);
        workerPort = first.workerPort();
        clientPort = first.clientPort();

        Worker worker = new Worker(new WorkerConfig("localhost", workerPort, "persistent-worker",
                1, Testbed.WORKER_HEARTBEAT_MILLIS, 100));
        worker.start();
        try {
            try (CoordinatorClient client = new CoordinatorClient("localhost", clientPort)) {
                Testbed.waitUntil("worker registered with first coordinator", Testbed.TERMINAL_TIMEOUT,
                        () -> uncheckedWorkerCount(client) == 1);
            }
            first.close();

            try (Coordinator second = new Coordinator(
                    new io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig(
                            workerPort, clientPort,
                            Testbed.HEARTBEAT_TIMEOUT_MILLIS, Testbed.SWEEP_INTERVAL_MILLIS,
                            Testbed.TASK_TIMEOUT_MILLIS, 3, 50, 200,
                            Testbed.UNLIMITED_ACTIVE_JOBS, database))) {
                second.start();
                try (CoordinatorClient client = new CoordinatorClient("localhost", clientPort)) {
                    Testbed.waitUntil("worker re-registered after restart", Testbed.TERMINAL_TIMEOUT,
                            () -> uncheckedWorkerCount(client) == 1);

                    String jobId = client.submit(TaskType.WORD_COUNT,
                            JsonNodeFactory.instance.objectNode().put("text", "back online"), 0);
                    JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
                    assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                    assertThat(done.result()).isEqualTo("2");
                }
            }
        } finally {
            worker.close();
        }
    }

    private static JobState uncheckedState(CoordinatorClient client, String jobId) {
        try {
            return client.status(jobId).state();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static int uncheckedWorkerCount(CoordinatorClient client) {
        try {
            return client.listWorkers().size();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
