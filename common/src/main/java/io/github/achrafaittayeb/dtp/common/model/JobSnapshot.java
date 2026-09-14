package io.github.achrafaittayeb.dtp.common.model;

/**
 * Immutable, client-facing view of a job's current state. This is what status
 * queries return; it never exposes coordinator-internal objects.
 *
 * <p>{@code priority} is the job's base (submitted) priority. Effective (aged)
 * priority is intentionally not exposed: it changes with wall-clock time, so
 * surfacing it in a point-in-time snapshot would be misleading. A {@code null}
 * priority decodes as {@link TaskPriority#NORMAL} for compatibility with older
 * peers that predate the field.
 */
public record JobSnapshot(
        String jobId,
        TaskType taskType,
        JobState state,
        int attempts,
        int maxAttempts,
        String workerId,
        String result,
        String error,
        long createdAtMillis,
        long updatedAtMillis,
        TaskPriority priority) {

    /**
     * Back-compatible constructor for callers that predate the priority field;
     * priority defaults to {@code null}, which reads as {@link TaskPriority#NORMAL}
     * via {@link #priorityOrDefault()}. Keeps existing call sites source-compatible.
     */
    public JobSnapshot(String jobId, TaskType taskType, JobState state, int attempts, int maxAttempts,
                       String workerId, String result, String error,
                       long createdAtMillis, long updatedAtMillis) {
        this(jobId, taskType, state, attempts, maxAttempts, workerId, result, error,
                createdAtMillis, updatedAtMillis, null);
    }

    /** Normalized base priority: {@code null} (from an older peer) reads as {@link TaskPriority#NORMAL}. */
    public TaskPriority priorityOrDefault() {
        return TaskPriority.normalize(priority);
    }
}
