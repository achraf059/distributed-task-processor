package io.github.achrafaittayeb.dtp.worker;

import io.github.achrafaittayeb.dtp.common.util.Args;

import java.util.UUID;

/**
 * Worker settings. The worker id is stable for the process lifetime; if none is
 * given a random one is generated so accidental id collisions cannot occur.
 */
public record WorkerConfig(
        String coordinatorHost,
        int coordinatorPort,
        String workerId,
        int capacity,
        long heartbeatIntervalMillis,
        long reconnectDelayMillis) {

    public static final int DEFAULT_CAPACITY = 4;
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 2_000;
    public static final long DEFAULT_RECONNECT_DELAY_MILLIS = 2_000;

    public static WorkerConfig fromArgs(String[] args) {
        Args parsed = Args.parse(args);
        return new WorkerConfig(
                parsed.get("coordinator-host", "localhost"),
                parsed.getInt("coordinator-port", 7070),
                parsed.get("id", "worker-" + UUID.randomUUID().toString().substring(0, 8)),
                parsed.getInt("capacity", DEFAULT_CAPACITY),
                parsed.getLong("heartbeat-interval-millis", DEFAULT_HEARTBEAT_INTERVAL_MILLIS),
                parsed.getLong("reconnect-delay-millis", DEFAULT_RECONNECT_DELAY_MILLIS));
    }

    public WorkerConfig {
        if (capacity < 1 || capacity > 1024) {
            throw new IllegalArgumentException("capacity must be in 1..1024");
        }
        if (heartbeatIntervalMillis <= 0 || reconnectDelayMillis <= 0) {
            throw new IllegalArgumentException("Timing intervals must be positive");
        }
    }
}
