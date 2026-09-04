package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario G: concurrency safety. Many clients submit simultaneously while
 * several workers offer capacity; every job must be assigned exactly once —
 * duplicate execution may only ever come from failure handling, never from a
 * scheduler race.
 */
class SchedulingSafetyIT {

    private static final int CLIENT_THREADS = 4;
    private static final int JOBS_PER_CLIENT = 15;
    private static final int WORKERS = 5;

    @Test
    void concurrentSubmissionsNeverProduceDuplicateAssignments() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3)) {

            List<ScriptedWorker> workers = new ArrayList<>();
            ExecutorService submitters = Executors.newFixedThreadPool(CLIENT_THREADS);
            try {
                // Scripted workers accept assignments but never answer them, so
                // every assignment stays observable in their queues.
                for (int i = 0; i < WORKERS; i++) {
                    workers.add(ScriptedWorker.register(
                            coordinator.workerPort(), "worker-" + i, JOBS_PER_CLIENT));
                }

                Set<String> submittedJobIds = ConcurrentHashMap.newKeySet();
                CountDownLatch startTogether = new CountDownLatch(1);
                List<Future<?>> submissions = new ArrayList<>();
                for (int t = 0; t < CLIENT_THREADS; t++) {
                    submissions.add(submitters.submit(() -> {
                        try (CoordinatorClient client = Testbed.connectClient(coordinator)) {
                            startTogether.await();
                            for (int j = 0; j < JOBS_PER_CLIENT; j++) {
                                submittedJobIds.add(client.submit(TaskType.SLEEP,
                                        JsonNodeFactory.instance.objectNode()
                                                .put("durationMillis", 60_000), 0));
                            }
                            return null;
                        }
                    }));
                }
                startTogether.countDown();
                for (Future<?> submission : submissions) {
                    submission.get(30, TimeUnit.SECONDS);
                }

                int totalJobs = CLIENT_THREADS * JOBS_PER_CLIENT;
                assertThat(submittedJobIds).hasSize(totalJobs);

                // Total worker capacity (5 * 15 = 75) exceeds the job count, so
                // every job must eventually be assigned somewhere.
                List<TaskAssign> allAssignments = new ArrayList<>();
                Testbed.waitUntil("all jobs assigned", Testbed.TERMINAL_TIMEOUT, () -> {
                    for (ScriptedWorker worker : workers) {
                        allAssignments.addAll(worker.drainAssignments());
                    }
                    return allAssignments.size() >= totalJobs;
                });

                Set<String> assignedJobIds = new HashSet<>();
                for (TaskAssign assignment : allAssignments) {
                    assertThat(assignedJobIds.add(assignment.jobId()))
                            .as("job %s was assigned twice", assignment.jobId())
                            .isTrue();
                    assertThat(assignment.attemptNumber()).isEqualTo(1);
                }
                assertThat(assignedJobIds).isEqualTo(submittedJobIds);
            } finally {
                submitters.shutdownNow();
                workers.forEach(ScriptedWorker::close);
            }
        }
    }
}
