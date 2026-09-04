package io.github.achrafaittayeb.dtp.coordinator;

/**
 * Exponential backoff for retries: {@code baseDelay * 2^(attempt-1)}, capped.
 * Whether a retry happens at all is decided by the job's attempt budget;
 * this class only answers "how long to wait before the next one".
 */
public final class RetryPolicy {

    private final long baseDelayMillis;
    private final long maxDelayMillis;

    public RetryPolicy(long baseDelayMillis, long maxDelayMillis) {
        if (baseDelayMillis < 0 || maxDelayMillis < baseDelayMillis) {
            throw new IllegalArgumentException(
                    "Require 0 <= baseDelay <= maxDelay, got " + baseDelayMillis + "/" + maxDelayMillis);
        }
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
    }

    /** Delay before the next attempt, given how many attempts have already run (>= 1). */
    public long delayBeforeNextAttemptMillis(int attemptsSoFar) {
        if (attemptsSoFar < 1) {
            throw new IllegalArgumentException("attemptsSoFar must be >= 1, got " + attemptsSoFar);
        }
        // Shift instead of Math.pow; clamp the exponent so it can never overflow.
        int exponent = Math.min(attemptsSoFar - 1, 30);
        long delay = baseDelayMillis << exponent;
        return Math.min(delay < 0 ? maxDelayMillis : delay, maxDelayMillis);
    }
}
