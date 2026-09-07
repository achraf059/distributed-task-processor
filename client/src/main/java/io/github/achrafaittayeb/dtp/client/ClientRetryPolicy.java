package io.github.achrafaittayeb.dtp.client;

/**
 * Client-side policy for retrying <em>submission</em> after a typed, retryable
 * {@link io.github.achrafaittayeb.dtp.common.protocol.SubmitRejected} (coordinator
 * overload). This is distinct from the coordinator's execution-retry policy,
 * which decides how a <em>running</em> job's failed attempts are re-run — see
 * the coordinator's {@code RetryPolicy}. This one never re-runs work; it only
 * re-attempts admission of a job that was never accepted.
 *
 * <p>Back-off is bounded exponential with full jitter: the wait before the
 * retry that follows attempt {@code n} is a uniform random value in
 * {@code [0, min(maxDelay, baseDelay * 2^(n-1))]}. Full jitter (rather than a
 * fixed schedule) desynchronizes many clients that were all rejected at once,
 * so they do not retry in lockstep and re-create the overload spike.
 */
public final class ClientRetryPolicy {

    private final int maxAttempts;
    private final long baseDelayMillis;
    private final long maxDelayMillis;

    private ClientRetryPolicy(int maxAttempts, long baseDelayMillis, long maxDelayMillis) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
        }
        if (baseDelayMillis < 0 || maxDelayMillis < baseDelayMillis) {
            throw new IllegalArgumentException(
                    "Require 0 <= baseDelay <= maxDelay, got "
                            + baseDelayMillis + "/" + maxDelayMillis);
        }
        this.maxAttempts = maxAttempts;
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
    }

    /** No retry: exactly one submission attempt (today's default behavior). */
    public static ClientRetryPolicy none() {
        return new ClientRetryPolicy(1, 0, 0);
    }

    /**
     * {@code retries} attempts <em>after</em> the initial one, so the total
     * attempt budget is {@code retries + 1}. {@code retries == 0} is equivalent
     * to {@link #none()}.
     */
    public static ClientRetryPolicy ofRetries(int retries, long baseDelayMillis, long maxDelayMillis) {
        if (retries < 0) {
            throw new IllegalArgumentException("retries must be >= 0, got " + retries);
        }
        return new ClientRetryPolicy(retries + 1, baseDelayMillis, maxDelayMillis);
    }

    /** Total submission attempts allowed, including the first. Always {@code >= 1}. */
    public int maxAttempts() {
        return maxAttempts;
    }

    /** Whether this policy ever retries (i.e. allows more than one attempt). */
    public boolean enabled() {
        return maxAttempts > 1;
    }

    /**
     * Full-jitter back-off before the retry that follows {@code attemptNumber}
     * (the number of attempts already made, {@code >= 1}).
     *
     * @param attemptNumber attempts made so far (1 = only the first has run)
     * @param random        a value in {@code [0, 1)}, e.g. from an RNG
     * @return milliseconds to wait, in {@code [0, min(maxDelay, base*2^(n-1))]}
     */
    long backoffMillis(int attemptNumber, double random) {
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be >= 1, got " + attemptNumber);
        }
        if (random < 0 || random >= 1) {
            throw new IllegalArgumentException("random must be in [0, 1), got " + random);
        }
        // Shift instead of Math.pow; clamp the exponent so it can never overflow.
        int exponent = Math.min(attemptNumber - 1, 30);
        long ceiling = baseDelayMillis << exponent;
        if (ceiling < 0 || ceiling > maxDelayMillis) {
            ceiling = maxDelayMillis;
        }
        // Full jitter over the inclusive range [0, ceiling]; random in [0, 1)
        // maps onto 0..ceiling. Guard the +1 against overflow at extreme caps.
        long span = ceiling == Long.MAX_VALUE ? ceiling : ceiling + 1;
        long delay = (long) (random * span);
        return Math.min(delay, maxDelayMillis);
    }
}
