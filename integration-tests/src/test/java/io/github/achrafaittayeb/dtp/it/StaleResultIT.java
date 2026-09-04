package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.common.protocol.TaskResult;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario E: attempt-lease protection. A worker that was declared dead comes
 * back and reports a result for an attempt whose lease has been superseded;
 * the coordinator must reject it without disturbing the job's real outcome.
 */
class StaleResultIT {

    @Test
    void resultFromSupersededAttemptIsRejected() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            // Zombie worker takes the job, then goes silent.
            ScriptedWorker zombie = ScriptedWorker.register(coordinator.workerPort(), "zombie", 1);
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 0);
            TaskAssign staleAssignment = zombie.awaitAssignment();
            zombie.stopHeartbeats();

            // A second scripted worker legitimately completes the second attempt.
            try (ScriptedWorker successor = ScriptedWorker.register(
                    coordinator.workerPort(), "successor", 1)) {
                TaskAssign freshAssignment = successor.awaitAssignment();
                assertThat(freshAssignment.jobId()).isEqualTo(jobId);
                assertThat(freshAssignment.attemptId()).isNotEqualTo(staleAssignment.attemptId());
                assertThat(freshAssignment.attemptNumber()).isEqualTo(2);
                successor.sendSuccess(freshAssignment, "legitimate result");

                Testbed.waitUntil("job completed by successor", Testbed.TERMINAL_TIMEOUT,
                        () -> statusOf(client, jobId).state() == JobState.COMPLETED);

                // The zombie revives: reconnects and reports its obsolete attempt.
                try (ScriptedWorker revived = ScriptedWorker.register(
                        coordinator.workerPort(), "zombie", 1)) {
                    revived.sendResult(TaskResult.success("zombie", jobId,
                            staleAssignment.attemptId(), "stale result that must be ignored"));

                    // Give the coordinator time to process, then verify nothing changed.
                    Thread.sleep(300);
                    JobSnapshot job = client.status(jobId);
                    assertThat(job.state()).isEqualTo(JobState.COMPLETED);
                    assertThat(job.result()).isEqualTo("legitimate result");
                    assertThat(job.attempts()).isEqualTo(2);

                    // And the coordinator is still fully operational afterwards.
                    assertThat(client.listWorkers()).isNotEmpty();
                }
            }
        }
    }

    @Test
    void staleResultCannotResurrectAJobMidRetry() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            ScriptedWorker zombie = ScriptedWorker.register(coordinator.workerPort(), "zombie", 1);
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 50), 0);
            TaskAssign staleAssignment = zombie.awaitAssignment();
            zombie.stopHeartbeats();

            // Wait until the failure detector has invalidated the first attempt
            // (job leaves RUNNING) but no worker is around to take attempt two.
            Testbed.waitUntil("first attempt invalidated", Testbed.TERMINAL_TIMEOUT,
                    () -> statusOf(client, jobId).state() != JobState.RUNNING);

            try (ScriptedWorker revived = ScriptedWorker.register(
                    coordinator.workerPort(), "zombie-revived", 1)) {
                revived.sendResult(TaskResult.success("zombie-revived", jobId,
                        staleAssignment.attemptId(), "stale"));
                Thread.sleep(300);

                // The job must not be COMPLETED by the stale lease; it either waits
                // for retry or is already running its second attempt on `revived`.
                JobSnapshot job = client.status(jobId);
                assertThat(job.state()).isIn(JobState.RETRY_WAIT, JobState.QUEUED, JobState.RUNNING);
                assertThat(job.result()).isNull();
            }
        }
    }

    private static JobSnapshot statusOf(CoordinatorClient client, String jobId) {
        try {
            return client.status(jobId);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void completedJobSurvivesDuplicateResultFromSameAttempt() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator);
             ScriptedWorker worker = ScriptedWorker.register(coordinator.workerPort(), "dupe", 1)) {

            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 10), 0);
            TaskAssign assignment = worker.awaitAssignment();
            worker.sendSuccess(assignment, "first");
            Testbed.waitUntil("job completed", Testbed.TERMINAL_TIMEOUT,
                    () -> statusOf(client, jobId).state() == JobState.COMPLETED);

            // At-least-once delivery can duplicate the result message itself.
            worker.sendSuccess(assignment, "second");
            Thread.sleep(200);

            JobSnapshot job = client.status(jobId);
            assertThat(job.result()).isEqualTo("first");
            assertThat(job.attempts()).isEqualTo(1);
        }
    }
}
