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
 *    └──── RETRY_WAIT ◀─────── RUNNING       (worker lost / task failed / deadline
 *                                             expired, retry pending)
 *
 * QUEUED / RETRY_WAIT / RUNNING ──▶ CANCELLED   (client-requested)
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
    FAILED,
    /** Terminal: cancelled on client request; any in-flight attempt lease was revoked. */
    CANCELLED;

    private static final Map<JobState, Set<JobState>> LEGAL_TRANSITIONS = Map.of(
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(COMPLETED, FAILED, RETRY_WAIT, QUEUED, CANCELLED),
            RETRY_WAIT, EnumSet.of(QUEUED, CANCELLED),
            COMPLETED, EnumSet.noneOf(JobState.class),
            FAILED, EnumSet.noneOf(JobState.class),
            CANCELLED, EnumSet.noneOf(JobState.class));

    public boolean canTransitionTo(JobState target) {
        return LEGAL_TRANSITIONS.get(this).contains(target);
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
