package io.github.achrafaittayeb.dtp.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Tracks the tasks a worker is currently executing, keyed by job id, each
 * tagged with the attempt id it belongs to. The attempt tag is what lets
 * cancellation target one specific attempt: a TASK_CANCEL for a superseded
 * attempt must never interrupt a newer assignment of the same job.
 *
 * <p>Thread-safe: the connection thread inserts, task-pool threads remove on
 * completion, and the connection thread also cancels. A {@link ConcurrentHashMap}
 * with attempt-matched compute operations keeps these races correct.
 */
final class InFlightTasks {

    private record Entry(String attemptId, Future<?> future) {
    }

    private final ConcurrentHashMap<String, Entry> byJobId = new ConcurrentHashMap<>();

    void put(String jobId, String attemptId, Future<?> future) {
        byJobId.put(jobId, new Entry(attemptId, future));
    }

    /** Removes the entry only if it still belongs to {@code attemptId}. */
    void remove(String jobId, String attemptId) {
        byJobId.computeIfPresent(jobId, (id, entry) ->
                entry.attemptId().equals(attemptId) ? null : entry);
    }

    /**
     * Interrupts the tracked task iff both ids match, then forgets it.
     *
     * @return true if a matching task was found and cancellation was requested
     */
    boolean cancelIfMatches(String jobId, String attemptId) {
        Entry entry = byJobId.get(jobId);
        if (entry == null || !entry.attemptId().equals(attemptId)) {
            return false;
        }
        entry.future().cancel(true);
        byJobId.remove(jobId, entry);
        return true;
    }

    /** Cancels every tracked task; used when the connection drops or the worker stops. */
    void cancelAll() {
        byJobId.values().forEach(entry -> entry.future().cancel(true));
        byJobId.clear();
    }
}
