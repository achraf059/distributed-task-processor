package io.github.achrafaittayeb.dtp.common.protocol;

/**
 * Outcome of one task attempt. Exactly one of {@code result} / {@code error}
 * is meaningful depending on {@code success}.
 */
public record TaskResult(
        String workerId,
        String jobId,
        String attemptId,
        boolean success,
        String result,
        String error) implements Message {

    public static TaskResult success(String workerId, String jobId, String attemptId, String result) {
        return new TaskResult(workerId, jobId, attemptId, true, result, null);
    }

    public static TaskResult failure(String workerId, String jobId, String attemptId, String error) {
        return new TaskResult(workerId, jobId, attemptId, false, null, error);
    }
}
