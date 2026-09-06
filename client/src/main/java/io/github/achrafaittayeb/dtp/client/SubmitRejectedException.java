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

    public SubmitRejectedException(SubmitRejected rejected) {
        super(rejected.reason());
        this.activeCount = rejected.activeCount();
        this.limit = rejected.limit();
    }

    public int activeCount() {
        return activeCount;
    }

    public int limit() {
        return limit;
    }

    /** Overload rejection is always a transient, retryable condition. */
    public boolean retryable() {
        return true;
    }
}
