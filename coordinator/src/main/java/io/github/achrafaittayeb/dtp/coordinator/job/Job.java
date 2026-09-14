package io.github.achrafaittayeb.dtp.coordinator.job;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.model.TaskType;

import java.util.UUID;

/**
 * Coordinator-side job entity. All state changes go through the transition
 * methods below, which enforce the {@link JobState} machine — illegal
 * transitions throw instead of silently corrupting state.
 *
 * <p>Thread-safety: instances are confined to the coordinator core thread
 * (see {@code CoordinatorCore}); they are never mutated concurrently.
 */
public final class Job {

    private final String id;
    private final TaskType taskType;
    private final JsonNode payload;
    private final int maxAttempts;
    private final long executionTimeoutMillis;
    private final long createdAtMillis;
    private final String idempotencyKey;

    /**
     * Base (submitted) priority. Immutable for the job's whole life, so a retry
     * — which mutates state in place on the same instance — automatically keeps
     * it. Normalized to non-null at construction. See {@link #effectiveLevel}.
     */
    private final TaskPriority priority;

    /**
     * Coordinator-assigned monotonic submission order, the deterministic FIFO
     * tiebreak among jobs of equal effective level. Immutable like {@link #priority}
     * (retries and recovery keep it), assigned once when a genuinely new job is
     * accepted, and persisted so ordering survives restart.
     */
    private final long submissionSequence;

    private JobState state;
    private int attempts;
    private String currentAttemptId;
    private String assignedWorkerId;
    private long deadlineMillis;
    private long nextEligibleTimeMillis;
    private String result;
    private String error;
    private long updatedAtMillis;

    /** Creates a new QUEUED job with no idempotency key, NORMAL priority, and sequence 0. */
    public static Job createQueued(TaskType taskType, JsonNode payload, int maxAttempts,
                                   long executionTimeoutMillis, long now) {
        return createQueued(taskType, payload, maxAttempts, executionTimeoutMillis, now, null);
    }

    /** Creates a new QUEUED job with an optional key, NORMAL priority, and sequence 0. */
    public static Job createQueued(TaskType taskType, JsonNode payload, int maxAttempts,
                                   long executionTimeoutMillis, long now, String idempotencyKey) {
        return createQueued(taskType, payload, maxAttempts, executionTimeoutMillis, now,
                idempotencyKey, TaskPriority.NORMAL, 0);
    }

    /**
     * Creates a new QUEUED job. The coordinator supplies the {@code priority}
     * (normalized to non-null) and the {@code submissionSequence} it assigned on
     * its single-writer path.
     */
    public static Job createQueued(TaskType taskType, JsonNode payload, int maxAttempts,
                                   long executionTimeoutMillis, long now, String idempotencyKey,
                                   TaskPriority priority, long submissionSequence) {
        return new Job(UUID.randomUUID().toString(), taskType, payload, maxAttempts,
                executionTimeoutMillis, JobState.QUEUED, 0, null, null, 0, 0, null, null, now, now,
                idempotencyKey, priority, submissionSequence);
    }

    /** Full-field constructor used when rehydrating from persistent storage. */
    public Job(String id, TaskType taskType, JsonNode payload, int maxAttempts,
               long executionTimeoutMillis, JobState state, int attempts,
               String currentAttemptId, String assignedWorkerId,
               long deadlineMillis, long nextEligibleTimeMillis, String result, String error,
               long createdAtMillis, long updatedAtMillis, String idempotencyKey,
               TaskPriority priority, long submissionSequence) {
        this.id = id;
        this.taskType = taskType;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.executionTimeoutMillis = executionTimeoutMillis;
        this.state = state;
        this.attempts = attempts;
        this.currentAttemptId = currentAttemptId;
        this.assignedWorkerId = assignedWorkerId;
        this.deadlineMillis = deadlineMillis;
        this.nextEligibleTimeMillis = nextEligibleTimeMillis;
        this.result = result;
        this.error = error;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
        this.idempotencyKey = idempotencyKey;
        this.priority = TaskPriority.normalize(priority);
        this.submissionSequence = submissionSequence;
    }

    /**
     * QUEUED → RUNNING. Consumes one attempt and issues a fresh attempt lease;
     * only a result carrying this lease id will ever be accepted. The lease is
     * time-bounded: it expires at {@code now + executionTimeoutMillis}, measured
     * exclusively on the coordinator's clock.
     */
    public String assignTo(String workerId, long now) {
        transition(JobState.RUNNING, now);
        attempts++;
        currentAttemptId = UUID.randomUUID().toString();
        assignedWorkerId = workerId;
        deadlineMillis = now + executionTimeoutMillis;
        return currentAttemptId;
    }

    /** RUNNING → COMPLETED. */
    public void complete(String taskResult, long now) {
        transition(JobState.COMPLETED, now);
        result = taskResult;
        clearLease();
    }

    /** RUNNING → RETRY_WAIT. The job becomes QUEUED again once {@code eligibleAt} passes. */
    public void scheduleRetry(String attemptError, long eligibleAt, long now) {
        transition(JobState.RETRY_WAIT, now);
        error = attemptError;
        nextEligibleTimeMillis = eligibleAt;
        clearLease();
    }

    /** RUNNING → FAILED (retries exhausted or permanently rejected). */
    public void failPermanently(String finalError, long now) {
        transition(JobState.FAILED, now);
        error = finalError;
        clearLease();
    }

    /** RETRY_WAIT → QUEUED once the backoff delay has elapsed. */
    public void requeueAfterRetryWait(long now) {
        transition(JobState.QUEUED, now);
        nextEligibleTimeMillis = 0;
    }

    /**
     * RUNNING → QUEUED. Coordinator-restart recovery: the previous assignment
     * cannot be trusted (its worker session is gone), so the lease is discarded
     * and the job is scheduled again immediately.
     */
    public void requeueForRecovery(long now) {
        transition(JobState.QUEUED, now);
        clearLease();
    }

    /**
     * QUEUED / RETRY_WAIT / RUNNING → CANCELLED. Client-requested termination;
     * any active attempt lease is discarded, so a late result from a worker
     * that keeps computing fails the lease check and is rejected.
     */
    public void cancel(long now) {
        transition(JobState.CANCELLED, now);
        error = "cancelled by client request";
        nextEligibleTimeMillis = 0;
        clearLease();
    }

    public boolean hasAttemptsLeft() {
        return attempts < maxAttempts;
    }

    /**
     * True when this job's current attempt has outlived its execution deadline.
     * Expiry is suspicion that the task is stuck, not proof the worker died —
     * the worker may be healthy and heartbeating while one task wedges.
     */
    public boolean isDeadlineExpired(long now) {
        return state == JobState.RUNNING && now > deadlineMillis;
    }

    /** True when a reported result belongs to this job's currently leased attempt. */
    public boolean isCurrentAttempt(String attemptId) {
        return state == JobState.RUNNING
                && currentAttemptId != null
                && currentAttemptId.equals(attemptId);
    }

    public boolean isEligibleToRun(long now) {
        return state == JobState.QUEUED
                || (state == JobState.RETRY_WAIT && nextEligibleTimeMillis <= now);
    }

    /**
     * The job's aged scheduling level at instant {@code now}:
     * {@code min(MAX_LEVEL, baseLevel + floor(waitingAge / agingStepMillis))}.
     *
     * <p>Aging is measured from {@link #createdAtMillis} (original submission),
     * so a retried job keeps accumulating fairness rather than resetting. The
     * waiting age is floored at zero, so a backwards clock reading can never push
     * a job <em>below</em> its base level. Pure function of its inputs: given the
     * same {@code now} it always returns the same value, which is what keeps the
     * scheduler deterministic and testable with a controlled clock.
     *
     * @param now             the single time captured for this scheduling pass
     * @param agingStepMillis the promotion interval; must be positive
     */
    public int effectiveLevel(long now, long agingStepMillis) {
        long waitingAge = Math.max(0L, now - createdAtMillis);
        long promotions = waitingAge / agingStepMillis;
        long level = priority.baseLevel() + promotions;
        return (int) Math.min(TaskPriority.MAX_LEVEL, level);
    }

    private void transition(JobState target, long now) {
        if (!state.canTransitionTo(target)) {
            throw new IllegalStateException(
                    "Illegal job transition " + state + " -> " + target + " for job " + id);
        }
        state = target;
        updatedAtMillis = now;
    }

    private void clearLease() {
        currentAttemptId = null;
        assignedWorkerId = null;
        deadlineMillis = 0;
    }

    public JobSnapshot snapshot() {
        return new JobSnapshot(id, taskType, state, attempts, maxAttempts,
                assignedWorkerId, result, error, createdAtMillis, updatedAtMillis, priority);
    }

    public String id() {
        return id;
    }

    /** Client-supplied submission-deduplication key, or {@code null} if none was provided. */
    public String idempotencyKey() {
        return idempotencyKey;
    }

    /** Base (submitted) priority; never null. */
    public TaskPriority priority() {
        return priority;
    }

    /** Coordinator-assigned monotonic submission order, the deterministic FIFO tiebreak. */
    public long submissionSequence() {
        return submissionSequence;
    }

    public TaskType taskType() {
        return taskType;
    }

    public JsonNode payload() {
        return payload;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public long executionTimeoutMillis() {
        return executionTimeoutMillis;
    }

    /** Coordinator-clock instant at which the current attempt's lease expires; 0 when not RUNNING. */
    public long deadlineMillis() {
        return deadlineMillis;
    }

    public JobState state() {
        return state;
    }

    public int attempts() {
        return attempts;
    }

    public String currentAttemptId() {
        return currentAttemptId;
    }

    public String assignedWorkerId() {
        return assignedWorkerId;
    }

    public long nextEligibleTimeMillis() {
        return nextEligibleTimeMillis;
    }

    public String result() {
        return result;
    }

    public String error() {
        return error;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    public long updatedAtMillis() {
        return updatedAtMillis;
    }
}
