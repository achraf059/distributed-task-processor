package io.github.achrafaittayeb.dtp.coordinator;

import io.github.achrafaittayeb.dtp.common.util.Args;

/**
 * Coordinator settings. Every value has a documented default and can be set via
 * {@code --flag value} or the {@code DTP_*} environment variable of the same name.
 *
 * <p>Timing defaults: workers heartbeat every 2 s (worker-side default) and are
 * suspected dead after 6 s of silence — three missed heartbeats, so one delayed
 * packet does not kill a healthy worker. Integration tests shrink all of these
 * to keep the suite fast and deterministic.
 */
public record CoordinatorConfig(
        int workerPort,
        int clientPort,
        long heartbeatTimeoutMillis,
        long sweepIntervalMillis,
        int defaultMaxAttempts,
        long retryBaseDelayMillis,
        long retryMaxDelayMillis,
        String databasePath) {

    public static final int DEFAULT_WORKER_PORT = 7070;
    public static final int DEFAULT_CLIENT_PORT = 7071;
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_MILLIS = 6_000;
    public static final long DEFAULT_SWEEP_INTERVAL_MILLIS = 500;
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_RETRY_BASE_DELAY_MILLIS = 1_000;
    public static final long DEFAULT_RETRY_MAX_DELAY_MILLIS = 30_000;
    public static final String DEFAULT_DATABASE_PATH = "data/coordinator.db";

    /** Path {@code :memory:} selects the non-durable in-memory repository. */
    public static final String IN_MEMORY_DATABASE = ":memory:";

    public static CoordinatorConfig fromArgs(String[] args) {
        Args parsed = Args.parse(args);
        return new CoordinatorConfig(
                parsed.getInt("worker-port", DEFAULT_WORKER_PORT),
                parsed.getInt("client-port", DEFAULT_CLIENT_PORT),
                parsed.getLong("heartbeat-timeout-millis", DEFAULT_HEARTBEAT_TIMEOUT_MILLIS),
                parsed.getLong("sweep-interval-millis", DEFAULT_SWEEP_INTERVAL_MILLIS),
                parsed.getInt("max-attempts", DEFAULT_MAX_ATTEMPTS),
                parsed.getLong("retry-base-delay-millis", DEFAULT_RETRY_BASE_DELAY_MILLIS),
                parsed.getLong("retry-max-delay-millis", DEFAULT_RETRY_MAX_DELAY_MILLIS),
                parsed.get("database", DEFAULT_DATABASE_PATH));
    }

    public CoordinatorConfig {
        if (heartbeatTimeoutMillis <= 0 || sweepIntervalMillis <= 0) {
            throw new IllegalArgumentException("Timing intervals must be positive");
        }
        if (defaultMaxAttempts < 1) {
            throw new IllegalArgumentException("max-attempts must be >= 1");
        }
    }
}
