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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Scenario B: several workers execute a batch of jobs concurrently. */
class MultiWorkerIT {

    @Test
    void batchOfJobsSpreadsAcrossWorkersAndAllComplete() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             Worker worker1 = Testbed.startWorker(coordinator, "worker-1", 2);
             Worker worker2 = Testbed.startWorker(coordinator, "worker-2", 2);
             Worker worker3 = Testbed.startWorker(coordinator, "worker-3", 2);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            Testbed.waitUntil("all three workers registered", Testbed.TERMINAL_TIMEOUT,
                    () -> workersOnline(client) == 3);

            // Each job sleeps long enough that 8 jobs cannot be handled by one
            // worker alone before others pick some up (total capacity is 6).
            List<String> jobIds = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                jobIds.add(client.submit(TaskType.SLEEP,
                        JsonNodeFactory.instance.objectNode().put("durationMillis", 300), 0));
            }

            // Sample which workers are executing while the batch drains; the
            // assigned worker id is only visible while a job is RUNNING.
            Set<String> executingWorkers = new HashSet<>();
            Testbed.waitUntil("all jobs completed", Testbed.TERMINAL_TIMEOUT, () -> {
                boolean allDone = true;
                for (JobSnapshot job : uncheckedListJobs(client)) {
                    if (job.state() == JobState.RUNNING && job.workerId() != null) {
                        executingWorkers.add(job.workerId());
                    }
                    allDone &= job.state().isTerminal();
                }
                return allDone;
            });

            for (JobSnapshot job : client.listJobs()) {
                assertThat(job.state()).isEqualTo(JobState.COMPLETED);
                assertThat(job.result()).isEqualTo("slept 300 ms");
            }
            assertThat(client.listJobs()).hasSize(8);
            assertThat(executingWorkers.size())
                    .as("jobs should have been spread over several workers, saw %s", executingWorkers)
                    .isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void jobsRunConcurrentlyUpToCapacity() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 4);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            Testbed.waitUntil("worker registered", Testbed.TERMINAL_TIMEOUT,
                    () -> workersOnline(client) == 1);

            long start = System.nanoTime();
            List<String> jobIds = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                jobIds.add(client.submit(TaskType.SLEEP,
                        JsonNodeFactory.instance.objectNode().put("durationMillis", 500), 0));
            }
            for (String jobId : jobIds) {
                client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT);
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            // Serial execution would need >= 2000 ms; concurrent execution on a
            // capacity-4 pool finishes in roughly one task duration.
            assertThat(elapsedMillis).isLessThan(1_500);
        }
    }

    private static int workersOnline(CoordinatorClient client) {
        try {
            return client.listWorkers().size();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static List<JobSnapshot> uncheckedListJobs(CoordinatorClient client) {
        try {
            return client.listJobs();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
