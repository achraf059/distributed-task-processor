package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Durable priority scheduling. Priorities and submission sequences persisted by
 * one coordinator lifetime must drive scheduling correctly after a restart, and
 * the sequence counter must resume strictly above every persisted value so new
 * submissions still sort after recovered ones.
 */
class PriorityRecoveryIT {

    @TempDir
    Path tempDir;

    private static String submit(CoordinatorClient client, TaskPriority priority, String text)
            throws IOException {
        return client.submit(TaskType.SHA256,
                JsonNodeFactory.instance.objectNode().put("text", text), 0, null, priority);
    }

    @Test
    void priorityAndSequenceSurviveRestartAndDriveOrdering() throws Exception {
        String database = tempDir.resolve("priority-restart.db").toString();
        String low;
        String normal;
        String high;

        // --- First lifetime: no workers, so everything stays QUEUED and persists.
        try (Coordinator first = Testbed.startCoordinator(database, 3);
             CoordinatorClient client = Testbed.connectClient(first)) {
            low = submit(client, TaskPriority.LOW, "low");        // sequence 1
            normal = submit(client, TaskPriority.NORMAL, "normal"); // sequence 2
            high = submit(client, TaskPriority.HIGH, "high");     // sequence 3
            assertThat(client.status(low).state()).isEqualTo(JobState.QUEUED);
        }

        // --- Second lifetime on the same database ---------------------------
        try (Coordinator second = Testbed.startCoordinator(database, 3);
             CoordinatorClient client = Testbed.connectClient(second)) {

            // Base priority survived the restart and is visible in the snapshot.
            assertThat(client.status(low).priorityOrDefault()).isEqualTo(TaskPriority.LOW);
            assertThat(client.status(normal).priorityOrDefault()).isEqualTo(TaskPriority.NORMAL);
            assertThat(client.status(high).priorityOrDefault()).isEqualTo(TaskPriority.HIGH);

            // A brand-new submission after restart must receive a sequence strictly
            // above every persisted one, so among equal HIGH priority it sorts
            // AFTER the recovered HIGH job.
            String newHigh = submit(client, TaskPriority.HIGH, "new-high"); // sequence 4

            // Register a capacity-1 worker only now, so the recovered + new backlog
            // drains through the scheduler in a single deterministic order.
            try (ScriptedWorker worker = ScriptedWorker.register(second.workerPort(), "w1", 1)) {
                assertThat(nextAssignedJobId(worker)).isEqualTo(high);     // HIGH, seq 3
                assertThat(nextAssignedJobId(worker)).isEqualTo(newHigh);  // HIGH, seq 4 (post-restart)
                assertThat(nextAssignedJobId(worker)).isEqualTo(normal);   // NORMAL, seq 2
                assertThat(nextAssignedJobId(worker)).isEqualTo(low);      // LOW, seq 1
            }
        }
    }

    private static String nextAssignedJobId(ScriptedWorker worker) throws Exception {
        TaskAssign assign = worker.awaitAssignment();
        worker.sendSuccess(assign, "ok");
        return assign.jobId();
    }
}
