package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.ClientRetryPolicy;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.client.SubmitRejectedException;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage for optional client-side submission retry against a real
 * coordinator over the wire (extends the admission-control scenario). All runs
 * use a limit of 1 and no worker, so a single queued job pins the coordinator
 * at capacity and every further submission is a genuine overload rejection.
 */
class SubmitRetryIT {

    private static String submitSha(CoordinatorClient client) throws IOException {
        return client.submit(TaskType.SHA256,
                JsonNodeFactory.instance.objectNode().put("text", "x"), 0);
    }

    private static CoordinatorClient retryingClient(Coordinator coordinator, ClientRetryPolicy policy)
            throws IOException {
        return new CoordinatorClient("localhost", coordinator.clientPort(), policy);
    }

    @Test
    void retryingClientEventuallySucceedsWhenCapacityFrees() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             CoordinatorClient blocker = Testbed.connectClient(coordinator)) {

            String queued = submitSha(blocker);          // fills the only slot (QUEUED)
            assertThat(coordinator.activeJobCount()).isEqualTo(1);

            // A retrying client submits while the coordinator is at capacity; it
            // keeps re-attempting with back-off long enough for the slot to free.
            AtomicReference<String> submittedId = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread submitter = new Thread(() -> {
                try (CoordinatorClient retrying = retryingClient(coordinator,
                        ClientRetryPolicy.ofRetries(100, 30, 120))) {
                    submittedId.set(submitSha(retrying));
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "retrying-submitter");
            submitter.start();

            // Give the submitter time to hit at least one rejection and back off,
            // then free the slot by cancelling the queued job.
            Thread.sleep(100);
            blocker.cancel(queued);

            submitter.join(Testbed.TERMINAL_TIMEOUT.toMillis());
            assertThat(failure.get()).isNull();
            assertThat(submittedId.get()).isNotBlank();
            assertThat(coordinator.activeJobCount()).isEqualTo(1); // the retried job now holds the slot
        }
    }

    @Test
    void retryingClientThrowsTypedRejectionWhenCapacityNeverFrees() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             CoordinatorClient blocker = Testbed.connectClient(coordinator);
             CoordinatorClient retrying = retryingClient(coordinator,
                     ClientRetryPolicy.ofRetries(2, 10, 40))) {

            submitSha(blocker); // fills the only slot, never freed
            assertThat(coordinator.activeJobCount()).isEqualTo(1);

            // Budget is bounded: after the retries are spent the caller still
            // sees the typed rejection, and nothing extra was admitted.
            assertThatThrownBy(() -> submitSha(retrying))
                    .isInstanceOf(SubmitRejectedException.class)
                    .satisfies(t -> assertThat(((SubmitRejectedException) t).retryable()).isTrue());
            assertThat(coordinator.activeJobCount()).isEqualTo(1);
        }
    }

    @Test
    void defaultClientStillThrowsOnFirstRejection() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS, 1);
             CoordinatorClient blocker = Testbed.connectClient(coordinator);
             CoordinatorClient defaultClient = Testbed.connectClient(coordinator)) {

            submitSha(blocker);
            // No retry policy configured → unchanged one-shot behavior.
            assertThatThrownBy(() -> submitSha(defaultClient))
                    .isInstanceOf(SubmitRejectedException.class);
        }
    }
}
