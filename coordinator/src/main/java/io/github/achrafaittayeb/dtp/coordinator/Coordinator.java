package io.github.achrafaittayeb.dtp.coordinator;

import io.github.achrafaittayeb.dtp.coordinator.job.InMemoryJobRepository;
import io.github.achrafaittayeb.dtp.coordinator.job.JobRepository;
import io.github.achrafaittayeb.dtp.coordinator.job.SqliteJobRepository;
import io.github.achrafaittayeb.dtp.coordinator.net.ClientServer;
import io.github.achrafaittayeb.dtp.coordinator.net.WorkerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wires the coordinator together and owns its lifecycle:
 * repository → core (recovery + sweep) → worker server → client server.
 * Also the embedding point for integration tests, which construct one of these
 * on ephemeral ports (port 0) with shrunken timeouts.
 */
public final class Coordinator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final CoordinatorCore core;
    private final WorkerServer workerServer;
    private final ClientServer clientServer;

    public Coordinator(CoordinatorConfig config) throws IOException {
        JobRepository repository = openRepository(config.databasePath());
        this.core = new CoordinatorCore(config, repository);
        this.workerServer = new WorkerServer(config.workerPort(), core);
        this.clientServer = new ClientServer(config.clientPort(), core);
    }

    private static JobRepository openRepository(String databasePath) {
        if (CoordinatorConfig.IN_MEMORY_DATABASE.equals(databasePath)) {
            log.warn("Using non-durable in-memory job storage (--database :memory:)");
            return new InMemoryJobRepository();
        }
        return new SqliteJobRepository(databasePath);
    }

    public void start() {
        core.start();
        workerServer.start();
        clientServer.start();
        log.info("Coordinator started (worker port {}, client port {})",
                workerServer.port(), clientServer.port());
    }

    public int workerPort() {
        return workerServer.port();
    }

    public int clientPort() {
        return clientServer.port();
    }

    @Override
    public void close() {
        // Stop the brain first: after this, disconnect events from closing
        // sockets are ignored and durable job state is frozen as-is, so a
        // graceful stop is indistinguishable from a crash at recovery time.
        core.close();
        clientServer.close();
        workerServer.close();
        log.info("Coordinator stopped");
    }
}
