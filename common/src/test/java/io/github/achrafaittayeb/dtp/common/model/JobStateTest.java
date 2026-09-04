package io.github.achrafaittayeb.dtp.common.model;

import org.junit.jupiter.api.Test;

import static io.github.achrafaittayeb.dtp.common.model.JobState.COMPLETED;
import static io.github.achrafaittayeb.dtp.common.model.JobState.FAILED;
import static io.github.achrafaittayeb.dtp.common.model.JobState.QUEUED;
import static io.github.achrafaittayeb.dtp.common.model.JobState.RETRY_WAIT;
import static io.github.achrafaittayeb.dtp.common.model.JobState.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

class JobStateTest {

    @Test
    void allowsDocumentedTransitions() {
        assertThat(QUEUED.canTransitionTo(RUNNING)).isTrue();
        assertThat(RUNNING.canTransitionTo(COMPLETED)).isTrue();
        assertThat(RUNNING.canTransitionTo(FAILED)).isTrue();
        assertThat(RUNNING.canTransitionTo(RETRY_WAIT)).isTrue();
        assertThat(RUNNING.canTransitionTo(QUEUED)).isTrue(); // coordinator recovery requeue
        assertThat(RETRY_WAIT.canTransitionTo(QUEUED)).isTrue();
    }

    @Test
    void rejectsIllegalTransitions() {
        assertThat(QUEUED.canTransitionTo(COMPLETED)).isFalse();
        assertThat(QUEUED.canTransitionTo(FAILED)).isFalse();
        assertThat(RETRY_WAIT.canTransitionTo(RUNNING)).isFalse();
        assertThat(COMPLETED.canTransitionTo(RUNNING)).isFalse();
        assertThat(FAILED.canTransitionTo(QUEUED)).isFalse();
    }

    @Test
    void terminalStatesHaveNoOutgoingTransitions() {
        for (JobState target : JobState.values()) {
            assertThat(COMPLETED.canTransitionTo(target)).isFalse();
            assertThat(FAILED.canTransitionTo(target)).isFalse();
        }
        assertThat(COMPLETED.isTerminal()).isTrue();
        assertThat(FAILED.isTerminal()).isTrue();
        assertThat(RUNNING.isTerminal()).isFalse();
    }
}
