package io.github.achrafaittayeb.dtp.common.protocol;

/**
 * Reply to {@link SubmitJob} when the coordinator refuses a <em>valid</em>
 * request because it is at its configured active-job limit (overload / load
 * shedding). This is deliberately a distinct message from {@link ErrorReply}:
 * an {@code ErrorReply} means the request was malformed or otherwise wrong and
 * must not be resent as-is, whereas a {@code SubmitRejected} means the request
 * was fine and the client should back off and retry later.
 *
 * @param reason      human-readable explanation, safe to show to users
 * @param activeCount number of active (non-terminal) jobs at the moment of rejection
 * @param limit       the configured {@code --max-active-jobs} ceiling
 * @param retryable   always {@code true} — overload is a transient condition
 */
public record SubmitRejected(String reason, int activeCount, int limit, boolean retryable)
        implements Message {

    /** Builds an overload rejection; overload is always retryable. */
    public static SubmitRejected overloaded(int activeCount, int limit) {
        return new SubmitRejected(
                "coordinator at capacity: " + activeCount + " active jobs (limit " + limit + ")",
                activeCount, limit, true);
    }
}
