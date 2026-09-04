package io.github.achrafaittayeb.dtp.coordinator.job;

import java.util.List;

/**
 * Durable storage for job state. The coordinator keeps the working set in
 * memory and writes through on every state change, so after a crash the
 * repository is the source of truth for recovery.
 *
 * <p>Implementations are called only from the coordinator core thread.
 */
public interface JobRepository extends AutoCloseable {

    /** Inserts or fully replaces the stored row for this job. */
    void save(Job job);

    /** Loads every stored job, in creation order. */
    List<Job> loadAll();

    @Override
    void close();
}
