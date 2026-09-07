package io.github.achrafaittayeb.dtp.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Runs a single submission attempt under a {@link ClientRetryPolicy}, retrying
 * only when the coordinator returns a typed, <em>retryable</em>
 * {@link SubmitRejectedException} (overload). Everything else — a
 * {@code retryable == false} rejection, a malformed-request error, a protocol
 * violation, or any transport failure — is propagated on the first occurrence
 * and never retried: those either must not be resent as-is, or are ambiguous
 * about whether the job was actually created, and a blind resend could submit
 * the same job twice.
 *
 * <p>This class deliberately knows nothing about sockets. The wire exchange is
 * supplied as an {@link Attempt}, and the clock ({@link Sleeper}) and jitter
 * source (an RNG returning {@code [0, 1)}) are injectable, so the retry
 * decision and back-off timing can be unit-tested deterministically without a
 * network or real sleeps.
 */
final class SubmitRetrier {

    /** One submission attempt over the wire. */
    @FunctionalInterface
    interface Attempt {
        String submit() throws IOException;
    }

    /** Injectable sleep, so tests can observe delays instead of waiting them out. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final ClientRetryPolicy policy;
    private final Sleeper sleeper;
    private final DoubleSupplier jitter;

    /** Production wiring: real sleep and a per-thread RNG. */
    SubmitRetrier(ClientRetryPolicy policy) {
        this(policy, Thread::sleep, () -> ThreadLocalRandom.current().nextDouble());
    }

    SubmitRetrier(ClientRetryPolicy policy, Sleeper sleeper, DoubleSupplier jitter) {
        this.policy = policy;
        this.sleeper = sleeper;
        this.jitter = jitter;
    }

    /**
     * Attempts the submission, retrying retryable overload rejections up to the
     * policy's attempt budget with jittered exponential back-off.
     *
     * @return the submitted job id
     * @throws SubmitRejectedException the last rejection, if the budget is
     *     exhausted or the rejection is not retryable
     * @throws IOException any other error or transport failure, unretried; also
     *     an {@link InterruptedIOException} (interrupt flag restored) if a
     *     back-off sleep is interrupted
     */
    String submit(Attempt attempt) throws IOException {
        int attemptNumber = 1;
        while (true) {
            try {
                return attempt.submit();
            } catch (SubmitRejectedException rejected) {
                if (!rejected.retryable() || attemptNumber >= policy.maxAttempts()) {
                    throw rejected;
                }
                long delayMillis = policy.backoffMillis(attemptNumber, jitter.getAsDouble());
                try {
                    sleeper.sleep(delayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    InterruptedIOException abort = new InterruptedIOException(
                            "Interrupted during submit-retry back-off after "
                                    + attemptNumber + " attempt(s)");
                    abort.initCause(rejected);
                    throw abort;
                }
                attemptNumber++;
            }
        }
    }
}
