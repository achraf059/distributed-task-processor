package io.github.achrafaittayeb.dtp.coordinator;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.coordinator.job.InMemoryJobRepository;
import io.github.achrafaittayeb.dtp.coordinator.job.Job;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the isolated queued-job ordering policy ({@code queuedOrder}).
 * No coordinator is started; the comparator is a pure function of a captured
 * {@code now}, so ordering is exercised deterministically without any clock or
 * threads.
 */
class CoordinatorSchedulingOrderTest {

    private static final long AGING_STEP = 100L;
    private static final long CREATED = 1_000L;

    private CoordinatorCore core() {
        CoordinatorConfig config = new CoordinatorConfig(
                0, 0, 6_000, 500, 60_000, 3, 50, 200, 10_000, AGING_STEP,
                CoordinatorConfig.IN_MEMORY_DATABASE);
        // Constructed but never started: queuedOrder needs only the config.
        return new CoordinatorCore(config, new InMemoryJobRepository());
    }

    private static Job job(TaskPriority priority, long sequence, long createdAt) {
        return Job.createQueued(TaskType.SLEEP,
                JsonNodeFactory.instance.objectNode().put("durationMillis", 1),
                3, 60_000, createdAt, null, priority, sequence);
    }

    private List<Long> sequencesInOrder(long now, Job... jobs) {
        Comparator<Job> order = core().queuedOrder(now);
        return Stream.of(jobs).sorted(order).map(Job::submissionSequence).toList();
    }

    @Test
    void higherBasePriorityRunsFirstWhenNotAged() {
        Job high = job(TaskPriority.HIGH, 3, CREATED);
        Job normal = job(TaskPriority.NORMAL, 2, CREATED);
        Job low = job(TaskPriority.LOW, 1, CREATED);

        assertThat(sequencesInOrder(CREATED, low, normal, high))
                .containsExactly(3L, 2L, 1L); // HIGH(seq3), NORMAL(seq2), LOW(seq1)
    }

    @Test
    void fifoBySequenceAmongEqualEffectivePriority() {
        Job first = job(TaskPriority.NORMAL, 1, CREATED);
        Job second = job(TaskPriority.NORMAL, 2, CREATED);
        Job third = job(TaskPriority.NORMAL, 3, CREATED);

        // Submitted out of order into the comparator; sequence restores FIFO.
        assertThat(sequencesInOrder(CREATED, third, first, second))
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void sameMillisecondSubmissionsStillFollowSequenceOrder() {
        // All three share the exact same createdAt: only the sequence breaks the tie,
        // so ordering is deterministic even at identical millisecond timestamps.
        Job a = job(TaskPriority.NORMAL, 10, CREATED);
        Job b = job(TaskPriority.NORMAL, 11, CREATED);
        Job c = job(TaskPriority.NORMAL, 12, CREATED);

        assertThat(sequencesInOrder(CREATED, c, a, b))
                .containsExactly(10L, 11L, 12L);
    }

    @Test
    void agedLowTiesHighAndBeatsNewerHighBySequence() {
        // An old LOW job (seq 1) that has aged to effective HIGH must run before a
        // freshly submitted HIGH job (seq 100) because its sequence is lower.
        Job oldLow = job(TaskPriority.LOW, 1, CREATED);
        Job newHigh = job(TaskPriority.HIGH, 100, CREATED + 10 * AGING_STEP);

        long now = CREATED + 10 * AGING_STEP; // oldLow aged well past HIGH cap
        assertThat(oldLow.effectiveLevel(now, AGING_STEP)).isEqualTo(TaskPriority.MAX_LEVEL);
        assertThat(sequencesInOrder(now, newHigh, oldLow))
                .containsExactly(1L, 100L); // aged LOW first, by sequence
    }

    @Test
    void notYetAgedLowStillLosesToHigh() {
        // Before the LOW job has aged, HIGH wins regardless of sequence.
        Job low = job(TaskPriority.LOW, 1, CREATED);
        Job high = job(TaskPriority.HIGH, 100, CREATED);

        assertThat(sequencesInOrder(CREATED, low, high))
                .containsExactly(100L, 1L); // HIGH first
    }

    @Test
    void extremeAgeCapsAtHighAndOrderingFallsToSequence() {
        // Very old jobs of every base priority all cap at effective HIGH; with equal
        // effective level the ordering is total and decided purely by sequence.
        Job oldHigh = job(TaskPriority.HIGH, 30, 0L);
        Job oldNormal = job(TaskPriority.NORMAL, 20, 0L);
        Job oldLow = job(TaskPriority.LOW, 10, 0L);
        long farFuture = 1_000_000_000L; // astronomically many aging steps → all capped at 2

        assertThat(oldHigh.effectiveLevel(farFuture, AGING_STEP)).isEqualTo(TaskPriority.MAX_LEVEL);
        assertThat(oldLow.effectiveLevel(farFuture, AGING_STEP)).isEqualTo(TaskPriority.MAX_LEVEL);
        assertThat(sequencesInOrder(farFuture, oldNormal, oldHigh, oldLow))
                .containsExactly(10L, 20L, 30L); // pure sequence order
    }

    @Test
    void negativeWaitingAgeKeepsBasePriorityOrder() {
        // A clock reading before every job's creation must not reorder below base:
        // no promotion happens, so HIGH > NORMAL > LOW, then sequence.
        Job high = job(TaskPriority.HIGH, 3, CREATED);
        Job normal = job(TaskPriority.NORMAL, 2, CREATED);
        Job low = job(TaskPriority.LOW, 1, CREATED);

        assertThat(sequencesInOrder(CREATED - 5_000, low, normal, high))
                .containsExactly(3L, 2L, 1L); // HIGH, NORMAL, LOW by base level
    }

    @Test
    void legacyZeroSequenceRowsOrderDeterministicallyByCreatedAt() {
        // Migrated legacy rows all carry sequence 0; the created-at fallback keeps
        // their ordering deterministic (best-effort FIFO).
        Job older = job(TaskPriority.NORMAL, 0, CREATED);
        Job newer = job(TaskPriority.NORMAL, 0, CREATED + 5);

        Comparator<Job> order = core().queuedOrder(CREATED + 100);
        List<Job> sorted = Stream.of(newer, older).sorted(order).toList();
        assertThat(sorted).containsExactly(older, newer);
    }
}
