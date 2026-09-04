package io.github.achrafaittayeb.dtp.coordinator.job;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobTest {

    private static Job newJob(int maxAttempts) {
        return Job.createQueued(TaskType.SLEEP,
                JsonNodeFactory.instance.objectNode().put("durationMillis", 100),
                maxAttempts, 1_000L);
    }

    @Test
    void assignmentConsumesAttemptAndIssuesFreshLease() {
        Job job = newJob(3);
        String firstLease = job.assignTo("worker-1", 2_000L);

        assertThat(job.state()).isEqualTo(JobState.RUNNING);
        assertThat(job.attempts()).isEqualTo(1);
        assertThat(job.assignedWorkerId()).isEqualTo("worker-1");
        assertThat(job.isCurrentAttempt(firstLease)).isTrue();

        job.scheduleRetry("worker lost", 3_000L, 2_500L);
        job.requeueAfterRetryWait(3_100L);
        String secondLease = job.assignTo("worker-2", 3_200L);

        assertThat(secondLease).isNotEqualTo(firstLease);
        assertThat(job.isCurrentAttempt(firstLease)).isFalse();
        assertThat(job.isCurrentAttempt(secondLease)).isTrue();
        assertThat(job.attempts()).isEqualTo(2);
    }

    @Test
    void completionClearsLeaseAndStoresResult() {
        Job job = newJob(3);
        String lease = job.assignTo("worker-1", 2_000L);
        job.complete("done", 3_000L);

        assertThat(job.state()).isEqualTo(JobState.COMPLETED);
        assertThat(job.result()).isEqualTo("done");
        assertThat(job.assignedWorkerId()).isNull();
        assertThat(job.isCurrentAttempt(lease)).isFalse();
    }

    @Test
    void staleLeaseIsNeverCurrentOutsideRunning() {
        Job job = newJob(3);
        String lease = job.assignTo("worker-1", 2_000L);
        job.scheduleRetry("boom", 5_000L, 2_500L);

        assertThat(job.state()).isEqualTo(JobState.RETRY_WAIT);
        assertThat(job.isCurrentAttempt(lease)).isFalse();
        assertThat(job.isEligibleToRun(4_999L)).isFalse();
        assertThat(job.isEligibleToRun(5_000L)).isTrue();
    }

    @Test
    void recoveryRequeueDiscardsLease() {
        Job job = newJob(3);
        String lease = job.assignTo("worker-1", 2_000L);
        job.requeueForRecovery(3_000L);

        assertThat(job.state()).isEqualTo(JobState.QUEUED);
        assertThat(job.isCurrentAttempt(lease)).isFalse();
        assertThat(job.assignedWorkerId()).isNull();
        assertThat(job.attempts()).isEqualTo(1); // crashed attempt still counts
    }

    @Test
    void attemptBudgetIsEnforced() {
        Job job = newJob(2);
        job.assignTo("w", 1L);
        assertThat(job.hasAttemptsLeft()).isTrue();
        job.scheduleRetry("e", 2L, 2L);
        job.requeueAfterRetryWait(3L);
        job.assignTo("w", 4L);
        assertThat(job.hasAttemptsLeft()).isFalse();
    }

    @Test
    void illegalTransitionsThrow() {
        Job queued = newJob(3);
        assertThatThrownBy(() -> queued.complete("nope", 1L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> queued.scheduleRetry("nope", 1L, 1L))
                .isInstanceOf(IllegalStateException.class);

        Job completed = newJob(3);
        completed.assignTo("w", 1L);
        completed.complete("ok", 2L);
        assertThatThrownBy(() -> completed.assignTo("w", 3L))
                .isInstanceOf(IllegalStateException.class);
        assertThat(completed.state()).isEqualTo(JobState.COMPLETED);
    }
}
