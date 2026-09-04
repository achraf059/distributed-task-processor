package io.github.achrafaittayeb.dtp.common.model;

/** Immutable, client-facing view of a registered worker. */
public record WorkerSnapshot(
        String workerId,
        int capacity,
        int activeTasks,
        long lastHeartbeatMillis) {
}
