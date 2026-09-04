package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Scenario D: retry policy — bounded retries, then FAILED; or success on a later attempt. */
class RetryIT {

    @Test
    void alwaysFailingTaskEndsFailedAfterMaxAttempts() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 2);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String jobId = client.submit(TaskType.FAIL,
                    JsonNodeFactory.instance.objectNode().put("failUntilAttempt", 999), 0);

            JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
            assertThat(done.state()).isEqualTo(JobState.FAILED);
            assertThat(done.attempts()).isEqualTo(2); // coordinator default from config above
            assertThat(done.error())
                    .contains("deliberate failure")
                    .contains("all 2 attempts used");
            assertThat(done.result()).isNull();
        }
    }

    @Test
    void flakyTaskSucceedsOnRetry() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String jobId = client.submit(TaskType.FAIL,
                    JsonNodeFactory.instance.objectNode().put("failUntilAttempt", 2), 0);

            JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
            assertThat(done.state()).isEqualTo(JobState.COMPLETED);
            assertThat(done.attempts()).isEqualTo(2);
            assertThat(done.result()).isEqualTo("succeeded on attempt 2");
        }
    }

    @Test
    void perJobMaxAttemptsOverridesCoordinatorDefault() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            String jobId = client.submit(TaskType.FAIL,
                    JsonNodeFactory.instance.objectNode().put("failUntilAttempt", 999), 1);

            JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
            assertThat(done.state()).isEqualTo(JobState.FAILED);
            assertThat(done.attempts()).isEqualTo(1);
        }
    }
}
