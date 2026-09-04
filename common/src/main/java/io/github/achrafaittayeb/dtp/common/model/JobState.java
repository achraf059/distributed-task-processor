package io.github.achrafaittayeb.dtp.common.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Lifecycle states of a job.
 *
 * <pre>
 * QUEUED ─────▶ RUNNING ─────▶ COMPLETED
 *    ▲            │ │
 *    │            │ └────────▶ FAILED        (retries exhausted, or non-retryable)
 *    ├────────────┘                          (coordinator recovery requeue)
 *    │
 *    └──── RETRY_WAIT ◀─────── RUNNING       (worker lost / task failed, retry pending)
 * </pre>
 */
public enum JobState {
    /** Waiting to be assigned to a worker. */
    QUEUED,
    /** Assigned to a worker under an active attempt lease. */
    RUNNING,
    /** A retry is pending; the job becomes QUEUED again once its backoff delay elapses. */
    RETRY_WAIT,
    /** Terminal: a worker reported a successful result that was accepted. */
    COMPLETED,
    /** Terminal: retries were exhausted or the job was rejected permanently. */
    FAILED;

    private static final Map<JobState, Set<JobState>> LEGAL_TRANSITIONS = Map.of(
            QUEUED, EnumSet.of(RUNNING),
            RUNNING, EnumSet.of(COMPLETED, FAILED, RETRY_WAIT, QUEUED),
            RETRY_WAIT, EnumSet.of(QUEUED),
            COMPLETED, EnumSet.noneOf(JobState.class),
            FAILED, EnumSet.noneOf(JobState.class));

    public boolean canTransitionTo(JobState target) {
        return LEGAL_TRANSITIONS.get(this).contains(target);
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }
}
