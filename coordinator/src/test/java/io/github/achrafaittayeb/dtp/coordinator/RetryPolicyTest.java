package io.github.achrafaittayeb.dtp.coordinator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    void doublesDelayPerAttempt() {
        RetryPolicy policy = new RetryPolicy(100, 10_000);
        assertThat(policy.delayBeforeNextAttemptMillis(1)).isEqualTo(100);
        assertThat(policy.delayBeforeNextAttemptMillis(2)).isEqualTo(200);
        assertThat(policy.delayBeforeNextAttemptMillis(3)).isEqualTo(400);
        assertThat(policy.delayBeforeNextAttemptMillis(4)).isEqualTo(800);
    }

    @Test
    void capsAtMaxDelay() {
        RetryPolicy policy = new RetryPolicy(100, 500);
        assertThat(policy.delayBeforeNextAttemptMillis(3)).isEqualTo(400);
        assertThat(policy.delayBeforeNextAttemptMillis(4)).isEqualTo(500);
        assertThat(policy.delayBeforeNextAttemptMillis(60)).isEqualTo(500); // no overflow
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new RetryPolicy(-1, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(200, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(100, 200).delayBeforeNextAttemptMillis(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
