package io.github.achrafaittayeb.dtp.client;

import io.github.achrafaittayeb.dtp.common.protocol.SubmitRejected;

import java.io.IOException;

/**
 * Thrown by {@link CoordinatorClient#submit} when the coordinator rejected a
 * well-formed submission because it is at its active-job limit. It extends
 * {@link IOException} so it flows through existing {@code throws IOException}
 * signatures, but callers that care can catch it specifically to tell
 * "overloaded, back off and retry" apart from a malformed-request error.
 */
public final class SubmitRejectedException extends IOException {

    private final int activeCount;
    private final int limit;
    private final boolean retryable;

    public SubmitRejectedException(SubmitRejected rejected) {
        super(rejected.reason());
        this.activeCount = rejected.activeCount();
        this.limit = rejected.limit();
        this.retryable = rejected.retryable();
    }

    public int activeCount() {
        return activeCount;
    }

    public int limit() {
        return limit;
    }

    /**
     * Whether the coordinator marked this rejection retryable. Reflects the
     * {@code retryable} field on the wire reply rather than assuming a value,
     * so a caller (or the client's own retry loop) only retries when the server
     * actually said it was safe to. Overload rejections are retryable today,
     * but honoring the field keeps clients correct if that ever changes.
     */
    public boolean retryable() {
        return retryable;
    }
}
