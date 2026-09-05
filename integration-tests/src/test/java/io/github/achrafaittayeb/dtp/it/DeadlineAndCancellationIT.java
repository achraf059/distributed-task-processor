package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.JobCancelReply;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.common.protocol.TaskCancel;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Time-bounded leases: execution deadlines close the gap where a task wedges on
 * a worker that stays alive and heartbeating, and client cancellation lets a
 * job be revoked on demand. Both reuse the existing attempt-lease machinery, so
 * these tests also re-assert stale-result rejection under the new triggers.
 */
class DeadlineAndCancellationIT {

    // Short execution timeout so deadline expiry is fast; still well above the
    // sweep interval so the timing is deterministic, not a race.
    private static final long SHORT_TASK_TIMEOUT = 400;

    // ---- A. Execution deadlines --------------------------------------------

    @Test
    void taskExceedingDeadlineOnHealthyWorkerIsReassignedAndCompletes() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, SHORT_TASK_TIMEOUT);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            // A scripted worker that keeps heartbeating (stays "alive") but never
            // reports a result: exactly a wedged task on a healthy worker.
            ScriptedWorker stuckButAlive = ScriptedWorker.register(
                    coordinator.workerPort(), "stuck-worker", 1);
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 0);
            TaskAssign firstAttempt = stuckButAlive.awaitAssignment();
            assertThat(client.status(jobId).state()).isEqualTo(JobState.RUNNING);

            // The deadline expires while the worker is still heartbeating; the
            // coordinator must send TASK_CANCEL and reassign.
            TaskCancel cancel = stuckButAlive.awaitCancel();
            assertThat(cancel.jobId()).isEqualTo(jobId);
            assertThat(cancel.attemptId()).isEqualTo(firstAttempt.attemptId());

            try (Worker healthy = Testbed.startWorker(coordinator, "healthy-worker", 1)) {
                JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
                assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                assertThat(done.result()).isEqualTo("slept 50 ms");
                assertThat(done.attempts()).isEqualTo(2);
                assertThat(done.error()).contains("execution deadline exceeded");
            }
            stuckButAlive.close();
        }
    }

    @Test
    void lateResultFromTimedOutAttemptIsRejectedAsStale() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, SHORT_TASK_TIMEOUT);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            ScriptedWorker slow = ScriptedWorker.register(coordinator.workerPort(), "slow", 1);
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 0);
            TaskAssign timedOut = slow.awaitAssignment();
            slow.awaitCancel(); // deadline fired

            // A healthy worker takes attempt 2 and completes it.
            try (Worker healthy = Testbed.startWorker(coordinator, "healthy", 1)) {
                JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
                assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                assertThat(done.attempts()).isEqualTo(2);

                // Now the timed-out attempt finally reports success — must be rejected.
                slow.sendSuccess(timedOut, "late result from the wedged attempt");
                Thread.sleep(300);

                JobSnapshot after = client.status(jobId);
                assertThat(after.state()).isEqualTo(JobState.COMPLETED);
                assertThat(after.result()).isEqualTo("slept 50 ms");
                assertThat(after.attempts()).isEqualTo(2);
            }
            slow.close();
        }
    }

    @Test
    void deadlineExpiryOnFinalAttemptFailsJobWithTimeoutError() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 1, SHORT_TASK_TIMEOUT);
             CoordinatorClient client = Testbed.connectClient(coordinator);
             ScriptedWorker stuck = ScriptedWorker.register(coordinator.workerPort(), "stuck", 1)) {

            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 1);
            stuck.awaitAssignment();

            JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
            assertThat(done.state()).isEqualTo(JobState.FAILED);
            assertThat(done.attempts()).isEqualTo(1);
            assertThat(done.error())
                    .contains("execution deadline exceeded")
                    .contains("all 1 attempts used");
        }
    }

    // ---- B. Client-requested cancellation ----------------------------------

    @Test
    void cancellingQueuedJobPreventsExecution() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            // No workers online, so the job stays QUEUED.
            String jobId = client.submit(TaskType.SHA256,
                    JsonNodeFactory.instance.objectNode().put("text", "never runs"), 0);
            assertThat(client.status(jobId).state()).isEqualTo(JobState.QUEUED);

            JobCancelReply reply = client.cancel(jobId);
            assertThat(reply.cancelled()).isTrue();
            assertThat(reply.job().state()).isEqualTo(JobState.CANCELLED);

            // Even after a worker joins, a CANCELLED job is never scheduled.
            try (Worker worker = Testbed.startWorker(coordinator, "worker-1", 1)) {
                Thread.sleep(300);
                assertThat(client.status(jobId).state()).isEqualTo(JobState.CANCELLED);
            }
        }
    }

    @Test
    void cancellingRunningJobInterruptsRealWorkerAndRevokesLease() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 1)) {

            Testbed.waitUntil("worker online", Testbed.TERMINAL_TIMEOUT,
                    () -> uncheckedWorkers(client) == 1);

            // A long SLEEP that would otherwise run for 30 s.
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 30_000), 0);
            Testbed.waitUntil("job running", Testbed.TERMINAL_TIMEOUT,
                    () -> uncheckedStatus(client, jobId).state() == JobState.RUNNING);

            long before = System.nanoTime();
            JobCancelReply reply = client.cancel(jobId);
            assertThat(reply.cancelled()).isTrue();
            assertThat(reply.job().state()).isEqualTo(JobState.CANCELLED);

            // The real worker's SLEEP was interrupted well before its 30 s runtime,
            // and its capacity was freed (proven by a follow-up job completing).
            long elapsedMillis = (System.nanoTime() - before) / 1_000_000;
            assertThat(elapsedMillis).isLessThan(5_000);

            String followUp = client.submit(TaskType.WORD_COUNT,
                    JsonNodeFactory.instance.objectNode().put("text", "one two"), 0);
            assertThat(client.awaitTerminal(followUp, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.COMPLETED);

            assertThat(client.status(jobId).state()).isEqualTo(JobState.CANCELLED);
        }
    }

    @Test
    void staleResultCannotResurrectACancelledJob() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator);
             ScriptedWorker worker = ScriptedWorker.register(coordinator.workerPort(), "worker-1", 1)) {

            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 0);
            TaskAssign assignment = worker.awaitAssignment();

            JobCancelReply reply = client.cancel(jobId);
            assertThat(reply.cancelled()).isTrue();
            worker.awaitCancel();

            // The worker ignores the cancel and reports success anyway.
            worker.sendSuccess(assignment, "result after cancel");
            Thread.sleep(300);

            JobSnapshot after = client.status(jobId);
            assertThat(after.state()).isEqualTo(JobState.CANCELLED);
            assertThat(after.result()).isNull();
        }
    }

    @Test
    void cancellingTerminalJobsIsReportedNotApplied() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 1)) {

            String jobId = client.submit(TaskType.SHA256,
                    JsonNodeFactory.instance.objectNode().put("text", "abc"), 0);
            client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);

            // Cancelling a COMPLETED job is a no-op that reports the real state.
            JobCancelReply reply = client.cancel(jobId);
            assertThat(reply.cancelled()).isFalse();
            assertThat(reply.job().state()).isEqualTo(JobState.COMPLETED);

            // Cancelling an unknown job is an error.
            assertThatThrownBy(() -> client.cancel("no-such-job"))
                    .hasMessageContaining("Unknown job");
        }
    }

    private static int uncheckedWorkers(CoordinatorClient client) {
        try {
            return client.listWorkers().size();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static JobSnapshot uncheckedStatus(CoordinatorClient client, String jobId) {
        try {
            return client.status(jobId);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
