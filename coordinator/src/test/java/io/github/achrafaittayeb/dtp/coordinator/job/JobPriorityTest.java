package io.github.achrafaittayeb.dtp.coordinator.job;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Priority, submission-sequence, and aging behavior of the {@link Job} model. */
class JobPriorityTest {

    private static final long TIMEOUT = 60_000L;
    private static final long CREATED_AT = 1_000L;

    private static Job newJob(TaskPriority priority, long sequence) {
        return Job.createQueued(TaskType.SLEEP,
                JsonNodeFactory.instance.objectNode().put("durationMillis", 100),
                3, TIMEOUT, CREATED_AT, null, priority, sequence);
    }

    @Test
    void newJobKeepsItsBasePriorityAndSequence() {
        Job job = newJob(TaskPriority.HIGH, 42);
        assertThat(job.priority()).isEqualTo(TaskPriority.HIGH);
        assertThat(job.submissionSequence()).isEqualTo(42);
    }

    @Test
    void defaultFactoriesUseNormalPriorityAndZeroSequence() {
        Job shortForm = Job.createQueued(TaskType.SLEEP,
                JsonNodeFactory.instance.objectNode().put("durationMillis", 100),
                3, TIMEOUT, CREATED_AT);
        assertThat(shortForm.priority()).isEqualTo(TaskPriority.NORMAL);
        assertThat(shortForm.submissionSequence()).isZero();

        Job keyedForm = Job.createQueued(TaskType.SLEEP,
                JsonNodeFactory.instance.objectNode().put("durationMillis", 100),
                3, TIMEOUT, CREATED_AT, "key");
        assertThat(keyedForm.priority()).isEqualTo(TaskPriority.NORMAL);
        assertThat(keyedForm.submissionSequence()).isZero();
    }

    @Test
    void nullPriorityNormalizesToNormalAtConstruction() {
        Job job = newJob(null, 1);
        assertThat(job.priority()).isEqualTo(TaskPriority.NORMAL);
    }

    @Test
    void retryAndRequeueRetainPriorityAndSequence() {
        Job job = newJob(TaskPriority.LOW, 7);
        job.assignTo("worker-1", 2_000L);
        job.scheduleRetry("boom", 3_000L, 2_500L);
        job.requeueAfterRetryWait(3_100L);

        assertThat(job.priority()).isEqualTo(TaskPriority.LOW);
        assertThat(job.submissionSequence()).isEqualTo(7);

        // A second attempt then a recovery requeue: still unchanged.
        job.assignTo("worker-2", 3_200L);
        job.requeueForRecovery(4_000L);
        assertThat(job.priority()).isEqualTo(TaskPriority.LOW);
        assertThat(job.submissionSequence()).isEqualTo(7);
    }

    @Test
    void effectiveLevelStartsAtBaseLevelWhenNotYetAged() {
        assertThat(newJob(TaskPriority.LOW, 1).effectiveLevel(CREATED_AT, 100)).isEqualTo(0);
        assertThat(newJob(TaskPriority.NORMAL, 1).effectiveLevel(CREATED_AT, 100)).isEqualTo(1);
        assertThat(newJob(TaskPriority.HIGH, 1).effectiveLevel(CREATED_AT, 100)).isEqualTo(2);
    }

    @Test
    void effectiveLevelRisesOnePerCompletedAgingInterval() {
        Job low = newJob(TaskPriority.LOW, 1);
        assertThat(low.effectiveLevel(CREATED_AT + 99, 100)).isEqualTo(0);   // interval not complete
        assertThat(low.effectiveLevel(CREATED_AT + 100, 100)).isEqualTo(1);  // one interval
        assertThat(low.effectiveLevel(CREATED_AT + 200, 100)).isEqualTo(2);  // two intervals
    }

    @Test
    void agedNormalReachesHigh() {
        Job normal = newJob(TaskPriority.NORMAL, 1);
        assertThat(normal.effectiveLevel(CREATED_AT + 100, 100)).isEqualTo(2);
    }

    @Test
    void effectiveLevelIsCappedAtHigh() {
        Job low = newJob(TaskPriority.LOW, 1);
        assertThat(low.effectiveLevel(CREATED_AT + 10_000, 100)).isEqualTo(TaskPriority.MAX_LEVEL);

        Job high = newJob(TaskPriority.HIGH, 1);
        assertThat(high.effectiveLevel(CREATED_AT + 10_000, 100)).isEqualTo(TaskPriority.MAX_LEVEL);
    }

    @Test
    void negativeWaitingAgeNeverReducesBelowBaseLevel() {
        // A clock that reads before the job's creation time must not demote it.
        Job normal = newJob(TaskPriority.NORMAL, 1);
        assertThat(normal.effectiveLevel(CREATED_AT - 5_000, 100)).isEqualTo(1);

        Job low = newJob(TaskPriority.LOW, 1);
        assertThat(low.effectiveLevel(CREATED_AT - 5_000, 100)).isEqualTo(0);
    }
}
