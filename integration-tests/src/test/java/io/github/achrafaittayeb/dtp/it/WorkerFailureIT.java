package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario C: the flagship failure story. A job is assigned to a worker that
 * stops heartbeating (simulating a hang/crash); the failure detector fires,
 * the attempt is invalidated, and a healthy worker completes the job.
 */
class WorkerFailureIT {

    @Test
    void jobIsReassignedWhenItsWorkerStopsHeartbeating() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             ScriptedWorker dyingWorker = ScriptedWorker.register(
                     coordinator.workerPort(), "dying-worker", 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            // Only the scripted worker is online, so the job must land on it.
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 100), 0);
            TaskAssign assignment = dyingWorker.awaitAssignment();
            assertThat(assignment.jobId()).isEqualTo(jobId);
            assertThat(client.status(jobId).state()).isEqualTo(JobState.RUNNING);
            assertThat(client.status(jobId).workerId()).isEqualTo("dying-worker");

            // The worker "hangs": its TCP connection stays open, heartbeats stop.
            dyingWorker.stopHeartbeats();

            try (Worker healthyWorker = Testbed.startWorker(coordinator, "healthy-worker", 1)) {
                JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);

                assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                assertThat(done.result()).isEqualTo("slept 100 ms");
                assertThat(done.attempts())
                        .as("first attempt lost to the dead worker, second succeeded")
                        .isEqualTo(2);
                assertThat(done.error()).contains("dying-worker");
            }
        }
    }

    @Test
    void abruptDisconnectAlsoTriggersReassignment() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            ScriptedWorker crashingWorker = ScriptedWorker.register(
                    coordinator.workerPort(), "crashing-worker", 1);
            String jobId = client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 100), 0);
            crashingWorker.awaitAssignment();

            // Simulate a killed process: the socket closes without any goodbye.
            crashingWorker.close();

            try (Worker healthyWorker = Testbed.startWorker(coordinator, "healthy-worker", 1)) {
                JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
                assertThat(done.state()).isEqualTo(JobState.COMPLETED);
                assertThat(done.attempts()).isEqualTo(2);
            }
        }
    }
}
