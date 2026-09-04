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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Scenario A: one coordinator, one worker, one job — QUEUED → RUNNING → COMPLETED. */
class BasicExecutionIT {

    @Test
    void jobRunsToCompletion() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            Testbed.waitUntil("worker registered", Testbed.TERMINAL_TIMEOUT,
                    () -> !uncheckedListWorkers(client).isEmpty());

            String jobId = client.submit(TaskType.WORD_COUNT,
                    JsonNodeFactory.instance.objectNode().put("text", "alpha beta gamma"), 0);

            JobSnapshot done = client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
            assertThat(done.state()).isEqualTo(JobState.COMPLETED);
            assertThat(done.result()).isEqualTo("3");
            assertThat(done.attempts()).isEqualTo(1);
            assertThat(done.error()).isNull();
        }
    }

    @Test
    void invalidSubmissionsAreRejectedWithClearErrors() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            assertThatThrownBy(() -> client.submit(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode(), 0))
                    .hasMessageContaining("durationMillis");

            assertThatThrownBy(() -> client.status("no-such-job"))
                    .hasMessageContaining("Unknown job");
        }
    }

    private static java.util.List<?> uncheckedListWorkers(CoordinatorClient client) {
        try {
            return client.listWorkers();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
