package io.github.achrafaittayeb.dtp.coordinator.workers;

import io.github.achrafaittayeb.dtp.common.model.WorkerSnapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The coordinator's view of currently registered workers.
 * Confined to the coordinator core thread, hence plain collections.
 */
public final class WorkerRegistry {

    private final Map<String, WorkerSession> sessions = new HashMap<>();

    /**
     * Registers a session, returning the previous session for the same worker id
     * if one existed (the caller must treat that old session as lost first).
     */
    public Optional<WorkerSession> register(WorkerSession session) {
        return Optional.ofNullable(sessions.put(session.workerId(), session));
    }

    /** Removes a session, but only if it is still the current one for that worker id. */
    public void remove(WorkerSession session) {
        sessions.remove(session.workerId(), session);
    }

    public Optional<WorkerSession> find(String workerId) {
        return Optional.ofNullable(sessions.get(workerId));
    }

    /** Workers with free capacity, least-loaded first, so load spreads evenly. */
    public List<WorkerSession> availableWorkers() {
        return sessions.values().stream()
                .filter(session -> session.freeCapacity() > 0)
                .sorted(Comparator.comparingInt(WorkerSession::freeCapacity).reversed()
                        .thenComparing(WorkerSession::workerId))
                .toList();
    }

    /** Sessions whose last heartbeat is older than {@code timeoutMillis}. */
    public List<WorkerSession> sessionsSuspectedDead(long now, long timeoutMillis) {
        return sessions.values().stream()
                .filter(session -> now - session.lastHeartbeatMillis() > timeoutMillis)
                .toList();
    }

    public List<WorkerSnapshot> snapshots() {
        List<WorkerSnapshot> result = new ArrayList<>();
        for (WorkerSession session : sessions.values()) {
            result.add(new WorkerSnapshot(session.workerId(), session.capacity(),
                    session.capacity() - session.freeCapacity(), session.lastHeartbeatMillis()));
        }
        result.sort(Comparator.comparing(WorkerSnapshot::workerId));
        return result;
    }

    public int size() {
        return sessions.size();
    }
}
