package io.github.achrafaittayeb.dtp.coordinator.job;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
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
    private final long createdAtMillis;

    private JobState state;
    private int attempts;
    private String currentAttemptId;
    private String assignedWorkerId;
    private long nextEligibleTimeMillis;
    private String result;
    private String error;
    private long updatedAtMillis;

    public static Job createQueued(TaskType taskType, JsonNode payload, int maxAttempts, long now) {
        return new Job(UUID.randomUUID().toString(), taskType, payload, maxAttempts,
                JobState.QUEUED, 0, null, null, 0, null, null, now, now);
    }

    /** Full-field constructor used when rehydrating from persistent storage. */
    public Job(String id, TaskType taskType, JsonNode payload, int maxAttempts,
               JobState state, int attempts, String currentAttemptId, String assignedWorkerId,
               long nextEligibleTimeMillis, String result, String error,
               long createdAtMillis, long updatedAtMillis) {
        this.id = id;
        this.taskType = taskType;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.state = state;
        this.attempts = attempts;
        this.currentAttemptId = currentAttemptId;
        this.assignedWorkerId = assignedWorkerId;
        this.nextEligibleTimeMillis = nextEligibleTimeMillis;
        this.result = result;
        this.error = error;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
    }

    /**
     * QUEUED → RUNNING. Consumes one attempt and issues a fresh attempt lease;
     * only a result carrying this lease id will ever be accepted.
     */
    public String assignTo(String workerId, long now) {
        transition(JobState.RUNNING, now);
        attempts++;
        currentAttemptId = UUID.randomUUID().toString();
        assignedWorkerId = workerId;
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

    public boolean hasAttemptsLeft() {
        return attempts < maxAttempts;
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
    }

    public JobSnapshot snapshot() {
        return new JobSnapshot(id, taskType, state, attempts, maxAttempts,
                assignedWorkerId, result, error, createdAtMillis, updatedAtMillis);
    }

    public String id() {
        return id;
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
