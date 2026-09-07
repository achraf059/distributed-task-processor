package io.github.achrafaittayeb.dtp.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the retry policy's attempt budget and jittered back-off bounds. */
class ClientRetryPolicyTest {

    @Test
    void noneIsASingleAttempt() {
        ClientRetryPolicy policy = ClientRetryPolicy.none();
        assertThat(policy.maxAttempts()).isEqualTo(1);
        assertThat(policy.enabled()).isFalse();
    }

    @Test
    void retriesAreCountedOnTopOfTheFirstAttempt() {
        // --submit-retries 3 -> 4 total attempts.
        assertThat(ClientRetryPolicy.ofRetries(3, 100, 1000).maxAttempts()).isEqualTo(4);
        assertThat(ClientRetryPolicy.ofRetries(0, 100, 1000).maxAttempts()).isEqualTo(1);
        assertThat(ClientRetryPolicy.ofRetries(0, 100, 1000).enabled()).isFalse();
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> ClientRetryPolicy.ofRetries(-1, 100, 1000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClientRetryPolicy.ofRetries(3, 1000, 100)) // max < base
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClientRetryPolicy.ofRetries(3, -1, 1000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void backoffStaysWithinTheJitteredExponentialCeiling() {
        ClientRetryPolicy policy = ClientRetryPolicy.ofRetries(10, 100, 5_000);
        // For each attempt, the ceiling is min(maxDelay, base * 2^(n-1)); a
        // full-jitter delay must land in [0, ceiling] for every random value.
        for (int attempt = 1; attempt <= 10; attempt++) {
            long ceiling = Math.min(5_000L, 100L << Math.min(attempt - 1, 30));
            for (double r : new double[] {0.0, 0.25, 0.5, 0.999}) {
                long delay = policy.backoffMillis(attempt, r);
                assertThat(delay).isBetween(0L, ceiling);
            }
        }
    }

    @Test
    void backoffGrowsThenSaturatesAtTheCap() {
        ClientRetryPolicy policy = ClientRetryPolicy.ofRetries(20, 100, 800);
        // With the jitter pinned high, the delay tracks the exponential ceiling
        // until it saturates at maxDelay and never exceeds it.
        assertThat(policy.backoffMillis(1, 0.999)).isLessThanOrEqualTo(100);
        assertThat(policy.backoffMillis(2, 0.999)).isLessThanOrEqualTo(200);
        assertThat(policy.backoffMillis(20, 0.999)).isLessThanOrEqualTo(800);
    }

    @Test
    void rejectsOutOfRangeJitter() {
        ClientRetryPolicy policy = ClientRetryPolicy.ofRetries(3, 100, 1000);
        assertThatThrownBy(() -> policy.backoffMillis(1, 1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy.backoffMillis(1, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy.backoffMillis(0, 0.5))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
