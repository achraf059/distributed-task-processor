package io.github.achrafaittayeb.dtp.coordinator.workers;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerRegistryTest {

    private static WorkerSession session(String workerId, int capacity, long heartbeatAt) {
        return new WorkerSession(workerId, capacity,
                new ByteArrayOutputStream(), () -> { }, heartbeatAt);
    }

    @Test
    void registerReturnsReplacedSessionForDuplicateWorkerId() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerSession original = session("worker-1", 2, 0);
        WorkerSession replacement = session("worker-1", 2, 0);

        assertThat(registry.register(original)).isEmpty();
        assertThat(registry.register(replacement)).contains(original);
        assertThat(registry.find("worker-1")).contains(replacement);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void removeIsANoOpForSupersededSessions() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerSession old = session("worker-1", 2, 0);
        WorkerSession current = session("worker-1", 2, 0);
        registry.register(old);
        registry.register(current);

        registry.remove(old); // late disconnect event from the replaced connection
        assertThat(registry.find("worker-1")).contains(current);
    }

    @Test
    void availableWorkersAreLeastLoadedFirstAndExcludeFullOnes() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerSession busy = session("busy", 2, 0);
        busy.addActiveJob("j1");
        WorkerSession idle = session("idle", 2, 0);
        WorkerSession full = session("full", 1, 0);
        full.addActiveJob("j2");
        registry.register(busy);
        registry.register(idle);
        registry.register(full);

        assertThat(registry.availableWorkers())
                .extracting(WorkerSession::workerId)
                .containsExactly("idle", "busy");
    }

    @Test
    void capacityAccountingTracksActiveJobs() {
        WorkerSession worker = session("w", 3, 0);
        worker.addActiveJob("a");
        worker.addActiveJob("b");
        assertThat(worker.freeCapacity()).isEqualTo(1);
        worker.removeActiveJob("a");
        worker.removeActiveJob("not-there"); // idempotent
        assertThat(worker.freeCapacity()).isEqualTo(2);
    }

    @Test
    void detectsSessionsWithoutRecentHeartbeats() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerSession fresh = session("fresh", 1, 10_000);
        WorkerSession silent = session("silent", 1, 1_000);
        registry.register(fresh);
        registry.register(silent);

        assertThat(registry.sessionsSuspectedDead(10_500, 3_000))
                .extracting(WorkerSession::workerId)
                .containsExactly("silent");

        fresh.recordHeartbeat(20_000);
        assertThat(registry.sessionsSuspectedDead(20_100, 3_000))
                .extracting(WorkerSession::workerId)
                .containsExactlyInAnyOrder("silent");
    }
}
