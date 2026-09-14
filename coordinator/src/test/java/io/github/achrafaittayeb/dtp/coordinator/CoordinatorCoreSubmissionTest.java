package io.github.achrafaittayeb.dtp.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.task.InvalidPayloadException;
import io.github.achrafaittayeb.dtp.coordinator.job.InMemoryJobRepository;
import io.github.achrafaittayeb.dtp.coordinator.job.Job;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Submission-path semantics that are not observable through the client snapshot:
 * how idempotent submissions, conflicts, admission rejection, and the monotonic
 * submission sequence interact. The submission sequence is read back off the
 * repository, which stores the live {@link Job} instances.
 */
class CoordinatorCoreSubmissionTest {

    private InMemoryJobRepository repository;
    private CoordinatorCore core;

    private CoordinatorConfig config(int maxActiveJobs) {
        return new CoordinatorConfig(0, 0, 6_000, 500, 60_000, 3, 50, 200,
                maxActiveJobs, 60_000, CoordinatorConfig.IN_MEMORY_DATABASE);
    }

    @BeforeEach
    void setUp() {
        repository = new InMemoryJobRepository();
        core = new CoordinatorCore(config(100), repository);
        core.start();
    }

    @AfterEach
    void tearDown() {
        core.close();
    }

    private static JsonNode text(String value) {
        return JsonNodeFactory.instance.objectNode().put("text", value);
    }

    private long sequenceOf(String jobId) {
        return repository.loadAll().stream()
                .filter(job -> job.id().equals(jobId))
                .map(Job::submissionSequence)
                .findFirst()
                .orElseThrow();
    }

    @Test
    void sameKeyAndEquivalentPriorityReturnsOriginalJob() throws InvalidPayloadException {
        CoordinatorCore.SubmitOutcome first =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K", TaskPriority.HIGH);
        CoordinatorCore.SubmitOutcome duplicate =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K", TaskPriority.HIGH);

        assertThat(first.accepted()).isTrue();
        assertThat(duplicate.accepted()).isTrue();
        assertThat(duplicate.jobId()).isEqualTo(first.jobId());
    }

    @Test
    void omittedPriorityAndExplicitNormalAreIdempotentlyEquivalent() throws InvalidPayloadException {
        // First submission omits priority (null → NORMAL); the resubmission states
        // NORMAL explicitly. They describe the same logical work, so no conflict.
        CoordinatorCore.SubmitOutcome omitted =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K2", null);
        CoordinatorCore.SubmitOutcome explicitNormal =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K2", TaskPriority.NORMAL);

        assertThat(explicitNormal.accepted()).isTrue();
        assertThat(explicitNormal.jobId()).isEqualTo(omitted.jobId());
    }

    @Test
    void sameKeyWithDifferentPriorityIsAConflict() throws InvalidPayloadException {
        CoordinatorCore.SubmitOutcome first =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K3", TaskPriority.NORMAL);
        CoordinatorCore.SubmitOutcome conflicting =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K3", TaskPriority.HIGH);

        assertThat(first.accepted()).isTrue();
        assertThat(conflicting.status()).isEqualTo(CoordinatorCore.SubmitOutcome.Status.CONFLICT);
        assertThat(conflicting.conflictMessage()).contains("different submission");
    }

    @Test
    void idempotentDuplicateDoesNotConsumeASequence() throws InvalidPayloadException {
        CoordinatorCore.SubmitOutcome first =
                core.submitJob(TaskType.SHA256, text("abc"), 0, "K4", TaskPriority.NORMAL);
        // A duplicate creates no job and must not advance the sequence counter.
        core.submitJob(TaskType.SHA256, text("abc"), 0, "K4", TaskPriority.NORMAL);
        CoordinatorCore.SubmitOutcome next =
                core.submitJob(TaskType.SHA256, text("xyz"), 0, null, TaskPriority.NORMAL);

        assertThat(sequenceOf(next.jobId())).isEqualTo(sequenceOf(first.jobId()) + 1);
    }

    @Test
    void rejectedSubmissionDoesNotConsumeASequence() throws InvalidPayloadException {
        core.close(); // replace the default core with a limit-of-1 one
        core = new CoordinatorCore(config(1), repository);
        core.start();

        CoordinatorCore.SubmitOutcome first =
                core.submitJob(TaskType.SHA256, text("abc"), 0, null, TaskPriority.NORMAL);
        CoordinatorCore.SubmitOutcome rejected =
                core.submitJob(TaskType.SHA256, text("def"), 0, null, TaskPriority.NORMAL);
        assertThat(rejected.status()).isEqualTo(CoordinatorCore.SubmitOutcome.Status.REJECTED);

        // Cancel the first to free an admission slot, then submit again. The
        // rejected submission must not have burned a sequence, so the next job's
        // sequence is exactly one past the first's.
        core.cancelJob(first.jobId());
        CoordinatorCore.SubmitOutcome next =
                core.submitJob(TaskType.SHA256, text("ghi"), 0, null, TaskPriority.NORMAL);

        assertThat(sequenceOf(next.jobId())).isEqualTo(sequenceOf(first.jobId()) + 1);
    }

    @Test
    void nextSequenceAfterRestartExceedsHighestPersistedTerminalJobSequence()
            throws InvalidPayloadException {
        // The @BeforeEach core already recovered from the empty repository; discard
        // it and seed the repository as if a prior lifetime had run.
        core.close();

        // A COMPLETED (terminal) job holds the highest persisted sequence — higher
        // than any active job would. The recovery seed must consider it, not just
        // the non-terminal jobs loaded into the active set.
        Job terminal = Job.createQueued(TaskType.SHA256, text("done"), 3, 60_000, 1_000L,
                null, TaskPriority.HIGH, 500L);
        terminal.assignTo("worker", 1_000L);
        terminal.complete("hash", 2_000L);
        assertThat(terminal.state()).isEqualTo(JobState.COMPLETED);
        repository.save(terminal);

        // "Restart": a fresh core recovers from the same repository.
        core = new CoordinatorCore(config(100), repository);
        core.start();

        CoordinatorCore.SubmitOutcome next =
                core.submitJob(TaskType.SHA256, text("after-restart"), 0, null, TaskPriority.NORMAL);
        assertThat(sequenceOf(next.jobId()))
                .isGreaterThan(terminal.submissionSequence())
                .isEqualTo(501L);
    }

    @Test
    void freshSubmissionsReceiveStrictlyIncreasingSequences() throws InvalidPayloadException {
        CoordinatorCore.SubmitOutcome a =
                core.submitJob(TaskType.SHA256, text("a"), 0, null, TaskPriority.LOW);
        CoordinatorCore.SubmitOutcome b =
                core.submitJob(TaskType.SHA256, text("b"), 0, null, TaskPriority.HIGH);
        CoordinatorCore.SubmitOutcome c =
                core.submitJob(TaskType.SHA256, text("c"), 0, null, TaskPriority.NORMAL);

        long seqA = sequenceOf(a.jobId());
        assertThat(sequenceOf(b.jobId())).isEqualTo(seqA + 1);
        assertThat(sequenceOf(c.jobId())).isEqualTo(seqA + 2);
    }
}
