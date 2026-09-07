package io.github.achrafaittayeb.dtp.client;

import io.github.achrafaittayeb.dtp.common.net.ProtocolException;
import io.github.achrafaittayeb.dtp.common.protocol.SubmitRejected;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deterministic unit tests for the submission retry loop: no sockets, no real
 * sleeps. A recording {@link SubmitRetrier.Sleeper} captures the back-off
 * delays and a fixed jitter source removes randomness, so both the retry
 * <em>decision</em> and the delay <em>bounds</em> are asserted exactly.
 */
class SubmitRetrierTest {

    /** Records requested sleeps instead of performing them. */
    private static final class RecordingSleeper implements SubmitRetrier.Sleeper {
        final List<Long> delays = new ArrayList<>();
        boolean interruptNext;

        @Override
        public void sleep(long millis) throws InterruptedException {
            if (interruptNext) {
                throw new InterruptedException("test interrupt");
            }
            delays.add(millis);
        }
    }

    private static final DoubleSupplier HALF = () -> 0.5;

    private static SubmitRejectedException overloaded() {
        return new SubmitRejectedException(SubmitRejected.overloaded(5, 5));
    }

    private static SubmitRejectedException nonRetryable() {
        return new SubmitRejectedException(new SubmitRejected("nope", 5, 5, false));
    }

    @Test
    void returnsImmediatelyWhenFirstAttemptSucceeds() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        SubmitRetrier retrier = new SubmitRetrier(
                ClientRetryPolicy.ofRetries(3, 10, 100), sleeper, HALF);

        String id = retrier.submit(() -> "job-1");

        assertThat(id).isEqualTo("job-1");
        assertThat(sleeper.delays).isEmpty();
    }

    @Test
    void retriesRetryableRejectionsThenSucceeds() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        SubmitRetrier retrier = new SubmitRetrier(
                ClientRetryPolicy.ofRetries(3, 10, 100), sleeper, HALF);
        int[] calls = {0};

        String id = retrier.submit(() -> {
            if (++calls[0] < 3) {
                throw overloaded();
            }
            return "job-2";
        });

        assertThat(id).isEqualTo("job-2");
        assertThat(calls[0]).isEqualTo(3);           // two rejections + one success
        assertThat(sleeper.delays).hasSize(2);       // one back-off before each retry
        assertThat(sleeper.delays).allSatisfy(d -> assertThat(d).isBetween(0L, 100L));
    }

    @Test
    void throwsLastRejectionWhenBudgetExhausted() {
        RecordingSleeper sleeper = new RecordingSleeper();
        SubmitRetrier retrier = new SubmitRetrier(
                ClientRetryPolicy.ofRetries(2, 10, 100), sleeper, HALF); // 3 attempts total
        int[] calls = {0};

        assertThatThrownBy(() -> retrier.submit(() -> {
            calls[0]++;
            throw overloaded();
        })).isInstanceOf(SubmitRejectedException.class);

        assertThat(calls[0]).isEqualTo(3);      // initial + 2 retries
        assertThat(sleeper.delays).hasSize(2);  // slept only between attempts
    }

    @Test
    void doesNotRetryNonRetryableRejection() {
        RecordingSleeper sleeper = new RecordingSleeper();
        SubmitRetrier retrier = new SubmitRetrier(
                ClientRetryPolicy.ofRetries(3, 10, 100), sleeper, HALF);
        int[] calls = {0};

        assertThatThrownBy(() -> retrier.submit(() -> {
            calls[0]++;
            throw nonRetryable();
        })).isInstanceOf(SubmitRejectedException.class);

        assertThat(calls[0]).isEqualTo(1);      // no retry
        assertThat(sleeper.delays).isEmpty();
    }

    @Test
    void doesNotRetryTransportOrProtocolFailures() {
        RecordingSleeper sleeper = new RecordingSleeper();
        SubmitRetrier retrier = new SubmitRetrier(
                ClientRetryPolicy.ofRetries(3, 10, 100), sleeper, HALF);
        int[] calls = {0};

        // A plain IOException (e.g. a dropped connection) is ambiguous about
        // whether the job was created, so it must propagate unretried.
        assertThatThrownBy(() -> retrier.submit(() -> {
            calls[0]++;
            throw new ProtocolException("boom");
        })).isInstanceOf(IOException.class)
           .isNotInstanceOf(SubmitRejectedException.class);

        assertThat(calls[0]).isEqualTo(1);
        assertThat(sleeper.delays).isEmpty();
    }

    @Test
    void neverRetriesUnderNonePolicy() {
        RecordingSleeper sleeper = new RecordingSleeper();
        SubmitRetrier retrier = new SubmitRetrier(ClientRetryPolicy.none(), sleeper, HALF);
        int[] calls = {0};

        assertThatThrownBy(() -> retrier.submit(() -> {
            calls[0]++;
            throw overloaded();
        })).isInstanceOf(SubmitRejectedException.class);

        assertThat(calls[0]).isEqualTo(1);
        assertThat(sleeper.delays).isEmpty();
    }

    @Test
    void restoresInterruptAndAbortsWhenBackoffSleepInterrupted() {
        RecordingSleeper sleeper = new RecordingSleeper();
        sleeper.interruptNext = true;
        SubmitRetrier retrier = new SubmitRetrier(
                ClientRetryPolicy.ofRetries(3, 10, 100), sleeper, HALF);

        Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(
                () -> retrier.submit(SubmitRetrierTest::overloaded0));

        assertThat(thrown).isInstanceOf(InterruptedIOException.class);
        assertThat(thrown.getCause()).isInstanceOf(SubmitRejectedException.class);
        assertThat(Thread.interrupted()).isTrue(); // flag restored (and cleared for other tests)
    }

    /** First attempt rejects (retryable), forcing the loop into a back-off sleep. */
    private static String overloaded0() throws IOException {
        throw overloaded();
    }
}
