package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end priority scheduling. A single scripted worker with capacity 1 is
 * kept busy by a blocker job so several submissions pile up in QUEUED; the order
 * in which they are assigned once the slot frees is fully determined by the
 * scheduler, which lets these tests assert priority, FIFO, and aging behavior
 * without racing real task execution.
 */
class PrioritySchedulingIT {

    private static String submit(CoordinatorClient client, TaskPriority priority, String text)
            throws IOException {
        return client.submit(TaskType.SHA256,
                JsonNodeFactory.instance.objectNode().put("text", text), 0, null, priority);
    }

    /** Occupies the worker's single slot and returns the still-open blocker assignment. */
    private static TaskAssign occupyWorker(CoordinatorClient client, ScriptedWorker worker)
            throws Exception {
        submit(client, TaskPriority.NORMAL, "blocker");
        return worker.awaitAssignment(); // now RUNNING; the one capacity slot is busy
    }

    private static void awaitQueued(CoordinatorClient client, String jobId) throws Exception {
        Testbed.waitUntil("job " + jobId + " queued", Testbed.TERMINAL_TIMEOUT,
                () -> {
                    try {
                        return client.status(jobId).state() == JobState.QUEUED;
                    } catch (IOException e) {
                        return false;
                    }
                });
    }

    @Test
    void higherPriorityIsAssignedFirst() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             ScriptedWorker worker = ScriptedWorker.register(coordinator.workerPort(), "w1", 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            TaskAssign blocker = occupyWorker(client, worker);

            // Submit in ascending priority order so submission order can't explain
            // the result — only priority can.
            String low = submit(client, TaskPriority.LOW, "low");
            String normal = submit(client, TaskPriority.NORMAL, "normal");
            String high = submit(client, TaskPriority.HIGH, "high");
            awaitQueued(client, low);
            awaitQueued(client, normal);
            awaitQueued(client, high);

            // Free the slot; the scheduler now drains the backlog in priority order.
            worker.sendSuccess(blocker, "done");

            assertThat(nextAssignedJobId(worker)).isEqualTo(high);
            assertThat(nextAssignedJobId(worker)).isEqualTo(normal);
            assertThat(nextAssignedJobId(worker)).isEqualTo(low);
        }
    }

    @Test
    void fifoIsPreservedAmongEqualPriority() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             ScriptedWorker worker = ScriptedWorker.register(coordinator.workerPort(), "w1", 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            TaskAssign blocker = occupyWorker(client, worker);

            String first = submit(client, TaskPriority.NORMAL, "first");
            String second = submit(client, TaskPriority.NORMAL, "second");
            String third = submit(client, TaskPriority.NORMAL, "third");
            awaitQueued(client, first);
            awaitQueued(client, second);
            awaitQueued(client, third);

            worker.sendSuccess(blocker, "done");

            // Equal priority: submission order (sequence) decides.
            assertThat(nextAssignedJobId(worker)).isEqualTo(first);
            assertThat(nextAssignedJobId(worker)).isEqualTo(second);
            assertThat(nextAssignedJobId(worker)).isEqualTo(third);
        }
    }

    @Test
    void agedLowPriorityJobBeatsNewerHighPriorityJobs() throws Exception {
        // Deterministic aging via an injected clock: time only moves when this
        // test advances it, so the result never depends on real elapsed time or a
        // Thread.sleep. The aging step is tiny and the advance is a handful of
        // milliseconds — far below the heartbeat and deadline timeouts — so the
        // fake advance promotes the LOW job without disturbing worker liveness.
        long agingStep = 10;
        MutableClock clock = new MutableClock(1_000_000);
        try (Coordinator coordinator = Testbed.startCoordinator(
                CoordinatorConfig.IN_MEMORY_DATABASE, 3, Testbed.TASK_TIMEOUT_MILLIS,
                Testbed.UNLIMITED_ACTIVE_JOBS, agingStep, clock);
             ScriptedWorker worker = ScriptedWorker.register(coordinator.workerPort(), "w1", 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            TaskAssign blocker = occupyWorker(client, worker);

            // Submit the LOW job first (created at the current fake time), then
            // advance the clock past two promotion intervals. LOW starts at level 0,
            // so two promotions raise its effective level to HIGH (2).
            String agedLow = submit(client, TaskPriority.LOW, "aged-low");
            awaitQueued(client, agedLow);
            clock.advance(2 * agingStep + 5);

            // Newer HIGH submissions: created "now" (no aging), higher base priority
            // but higher sequence — so the aged LOW must still win the tie.
            String newHigh1 = submit(client, TaskPriority.HIGH, "new-high-1");
            String newHigh2 = submit(client, TaskPriority.HIGH, "new-high-2");
            awaitQueued(client, newHigh1);
            awaitQueued(client, newHigh2);

            worker.sendSuccess(blocker, "done");

            // Starvation prevented: the long-waiting LOW job runs before the newer
            // HIGH jobs; the HIGH jobs then follow in submission order.
            assertThat(nextAssignedJobId(worker)).isEqualTo(agedLow);
            assertThat(nextAssignedJobId(worker)).isEqualTo(newHigh1);
            assertThat(nextAssignedJobId(worker)).isEqualTo(newHigh2);
        }
    }

    /** Captures the next assignment and immediately completes it so the next can flow. */
    private static String nextAssignedJobId(ScriptedWorker worker) throws Exception {
        TaskAssign assign = worker.awaitAssignment();
        worker.sendSuccess(assign, "ok");
        return assign.jobId();
    }
}
