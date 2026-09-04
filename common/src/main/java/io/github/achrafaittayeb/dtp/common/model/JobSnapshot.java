package io.github.achrafaittayeb.dtp.common.model;

/**
 * Immutable, client-facing view of a job's current state. This is what status
 * queries return; it never exposes coordinator-internal objects.
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
        long updatedAtMillis) {
}
