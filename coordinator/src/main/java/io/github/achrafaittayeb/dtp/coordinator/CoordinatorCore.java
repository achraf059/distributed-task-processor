package io.github.achrafaittayeb.dtp.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.model.WorkerSnapshot;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.common.protocol.TaskCancel;
import io.github.achrafaittayeb.dtp.common.protocol.TaskResult;
import io.github.achrafaittayeb.dtp.common.task.InvalidPayloadException;
import io.github.achrafaittayeb.dtp.common.task.TaskPayloads;
import io.github.achrafaittayeb.dtp.coordinator.job.Job;
import io.github.achrafaittayeb.dtp.coordinator.job.JobRepository;
import io.github.achrafaittayeb.dtp.coordinator.workers.WorkerRegistry;
import io.github.achrafaittayeb.dtp.coordinator.workers.WorkerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The coordinator's brain. All mutable state — the worker registry and every
 * {@link Job} — is owned by a single core thread; network threads never touch
 * it directly, they submit events here instead.
 *
 * <p><b>Concurrency invariant:</b> because scheduling, heartbeat evaluation,
 * result handling, and recovery all run on one thread, no interleaving can
 * assign the same job to two workers or double-apply a result. Duplicate
 * <em>execution</em> can still occur — that is the documented at-least-once
 * semantics under worker failure, and attempt leases keep it harmless to
 * coordinator state.
 *
 * <p>Every job state change is written through to the {@link JobRepository}
 * before it is observable by clients, so a restarted coordinator recovers from
 * storage (see {@link #recoverFromRepository}).
 */
public final class CoordinatorCore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorCore.class);

    private final CoordinatorConfig config;
    private final JobRepository repository;
    private final RetryPolicy retryPolicy;
    private final WorkerRegistry registry = new WorkerRegistry();
    private final Map<String, Job> jobs = new HashMap<>();

    /**
     * Maps a client-supplied idempotency key to the id of the one logical job it
     * created, so a resubmission of the same key returns the original job rather
     * than creating a duplicate. Confined to the core thread like {@link #jobs},
     * so the check-then-create is atomic without locking. Rebuilt from persisted
     * job rows on recovery, so deduplication survives a restart. A job keeps its
     * entry for its whole lifetime, including terminal states — resubmitting a
     * completed/failed/cancelled key returns that same terminal job.
     */
    private final Map<String, String> jobIdByKey = new HashMap<>();

    private final ScheduledExecutorService coreThread;

    /**
     * Number of active (non-terminal) jobs: QUEUED + RETRY_WAIT + RUNNING. Kept
     * as a running counter — never recomputed by scanning {@link #jobs} — so the
     * admission check in {@link #submitJob} stays O(1) even under overload, when
     * scanning would be most expensive. Confined to the core thread, so the
     * increment/decrement need no synchronization: the single-writer design that
     * already prevents double-assignment also makes this counter race-free.
     *
     * <p>Invariant: it is incremented exactly once when a job enters the active
     * set (a new submission, or a non-terminal job recovered from storage) and
     * decremented exactly once when a job leaves it (any active → terminal
     * transition, funnelled through {@link #persistRetired}). Retries stay
     * active (RUNNING → RETRY_WAIT → QUEUED) and never touch it.
     */
    private int activeJobCount;

    public CoordinatorCore(CoordinatorConfig config, JobRepository repository) {
        this.config = config;
        this.repository = repository;
        this.retryPolicy = new RetryPolicy(config.retryBaseDelayMillis(), config.retryMaxDelayMillis());
        this.coreThread = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "coordinator-core"));
    }

    /** Recovers persisted state, then starts the periodic sweep. */
    public void start() {
        runOnCore(this::recoverFromRepository);
        coreThread.scheduleWithFixedDelay(this::sweep,
                config.sweepIntervalMillis(), config.sweepIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------
    // Events from worker connections (called on connection reader threads)
    // ------------------------------------------------------------------

    public void onWorkerRegistered(WorkerSession session) {
        runOnCore(() -> {
            registry.register(session).ifPresent(previous -> {
                log.warn("Worker {} re-registered; treating its previous session as lost",
                        session.workerId());
                invalidateAssignments(previous, "worker re-registered");
                previous.close();
            });
            log.info("Worker registered: workerId={} capacity={} ({} workers online)",
                    session.workerId(), session.capacity(), registry.size());
            scheduleQueuedJobs();
        });
    }

    public void onWorkerDisconnected(WorkerSession session, String reason) {
        runOnCore(() -> {
            // Ignore if a newer session already replaced this one.
            if (registry.find(session.workerId()).orElse(null) != session) {
                return;
            }
            log.warn("Worker connection lost: workerId={} reason={}", session.workerId(), reason);
            handleWorkerLoss(session, "connection lost: " + reason);
        });
    }

    public void onHeartbeat(String workerId) {
        runOnCore(() -> registry.find(workerId)
                .ifPresent(session -> session.recordHeartbeat(now())));
    }

    public void onTaskResult(TaskResult result) {
        runOnCore(() -> applyTaskResult(result));
    }

    // ------------------------------------------------------------------
    // Requests from client connections (block until the core thread answers)
    // ------------------------------------------------------------------

    /**
     * Outcome of a submission. {@code ACCEPTED} carries the job id (a fresh job,
     * or the original job for an idempotent duplicate); {@code REJECTED} means
     * the coordinator is at its active-job limit; {@code CONFLICT} means the
     * idempotency key is already bound to a materially different submission and
     * {@code conflictMessage} explains why (surfaced to the client as an error).
     */
    public record SubmitOutcome(Status status, String jobId, int activeCount, int limit,
                                String conflictMessage) {

        public enum Status { ACCEPTED, REJECTED, CONFLICT }

        static SubmitOutcome accepted(String jobId, int activeCount, int limit) {
            return new SubmitOutcome(Status.ACCEPTED, jobId, activeCount, limit, null);
        }

        static SubmitOutcome rejected(int activeCount, int limit) {
            return new SubmitOutcome(Status.REJECTED, null, activeCount, limit, null);
        }

        static SubmitOutcome conflict(String message) {
            return new SubmitOutcome(Status.CONFLICT, null, 0, 0, message);
        }

        public boolean accepted() {
            return status == Status.ACCEPTED;
        }
    }

    /** Overload {@link #submitJob(TaskType, JsonNode, int, String)} without an idempotency key. */
    public SubmitOutcome submitJob(TaskType taskType, JsonNode payload, int requestedMaxAttempts)
            throws InvalidPayloadException {
        return submitJob(taskType, payload, requestedMaxAttempts, null);
    }

    /**
     * Admission point for new work. A malformed payload is rejected up front
     * (before the core thread) as {@link InvalidPayloadException}. On the core
     * thread the request is handled in this order:
     *
     * <ol>
     *   <li><b>Deduplication first.</b> If a non-null {@code idempotencyKey} is
     *       already known, the original job's id is returned. This happens
     *       <em>before</em> admission control and so bypasses the active-job
     *       limit: a known key creates no new job, so there is nothing to admit —
     *       exactly like a retry of already-accepted work. If the key is known
     *       but the request differs materially (task type, payload, or effective
     *       max attempts), it is a {@link SubmitOutcome#conflict}: the existing
     *       job is left untouched and no new job is created.</li>
     *   <li><b>Admission.</b> A genuinely new submission (no key, or an unseen
     *       key) is admitted only if the active-job count is below the limit;
     *       otherwise it is shed with {@link SubmitOutcome#rejected}.</li>
     * </ol>
     *
     * <p>Deduplication prevents a <em>duplicate logical submission</em>; it does
     * not change execution semantics, which remain at-least-once. Only fresh
     * submissions pass through admission; execution retries never do.
     */
    public SubmitOutcome submitJob(TaskType taskType, JsonNode payload, int requestedMaxAttempts,
                                   String idempotencyKey) throws InvalidPayloadException {
        TaskPayloads.validate(taskType, payload);
        int maxAttempts = requestedMaxAttempts > 0 ? requestedMaxAttempts : config.defaultMaxAttempts();
        return askCore(() -> {
            if (idempotencyKey != null) {
                String existingId = jobIdByKey.get(idempotencyKey);
                if (existingId != null) {
                    Job existing = jobs.get(existingId);
                    if (!matchesSubmission(existing, taskType, payload, maxAttempts)) {
                        log.warn("Idempotency conflict: key={} bound to jobId={} but resubmission "
                                + "differs (type/payload/maxAttempts)", idempotencyKey, existingId);
                        return SubmitOutcome.conflict("Idempotency key '" + idempotencyKey
                                + "' is already bound to a different submission (job " + existingId + ")");
                    }
                    log.info("Idempotent duplicate: key={} returning original jobId={} (state {})",
                            idempotencyKey, existingId, existing.state());
                    return SubmitOutcome.accepted(existingId, activeJobCount, config.maxActiveJobs());
                }
            }
            int limit = config.maxActiveJobs();
            if (activeJobCount >= limit) {
                log.warn("Job submission rejected (overloaded): activeJobs={} limit={} type={}",
                        activeJobCount, limit, taskType);
                return SubmitOutcome.rejected(activeJobCount, limit);
            }
            Job job = Job.createQueued(taskType, payload, maxAttempts,
                    config.taskTimeoutMillis(), now(), idempotencyKey);
            jobs.put(job.id(), job);
            if (idempotencyKey != null) {
                jobIdByKey.put(idempotencyKey, job.id());
            }
            activeJobCount++;
            repository.save(job);
            log.info("Job submitted: jobId={} type={} maxAttempts={} activeJobs={}/{} idempotencyKey={}",
                    job.id(), taskType, maxAttempts, activeJobCount, limit, idempotencyKey);
            scheduleQueuedJobs();
            return SubmitOutcome.accepted(job.id(), activeJobCount, limit);
        });
    }

    /**
     * Whether a resubmission under a known key describes the same logical work as
     * the job the key already created. Payload equality is structural
     * ({@link JsonNode#equals}), and the stored job holds the already-resolved
     * effective max attempts, so it is compared against the incoming effective
     * value.
     */
    private boolean matchesSubmission(Job existing, TaskType taskType, JsonNode payload,
                                      int effectiveMaxAttempts) {
        return existing != null
                && existing.taskType() == taskType
                && existing.maxAttempts() == effectiveMaxAttempts
                && existing.payload().equals(payload);
    }

    /** Observability/test hook: current number of active (non-terminal) jobs, read on the core thread. */
    public int activeJobCount() {
        return askCore(() -> activeJobCount);
    }

    public Optional<JobSnapshot> getJob(String jobId) {
        return askCore(() -> Optional.ofNullable(jobs.get(jobId)).map(Job::snapshot));
    }

    /** Outcome of a cancellation request: whether the job is present, and whether this call cancelled it. */
    public record CancelOutcome(boolean found, boolean cancelledNow, JobSnapshot job) {
    }

    /**
     * Cancels a job on client request. QUEUED / RETRY_WAIT / RUNNING → CANCELLED
     * (a RUNNING attempt's lease is revoked and a best-effort TASK_CANCEL is
     * sent); already-terminal jobs are left untouched and reported as such.
     */
    public CancelOutcome cancelJob(String jobId) {
        return askCore(() -> {
            Job job = jobs.get(jobId);
            if (job == null) {
                return new CancelOutcome(false, false, null);
            }
            if (job.state().isTerminal()) {
                return new CancelOutcome(true, false, job.snapshot());
            }
            String attemptId = job.currentAttemptId();
            String workerId = job.assignedWorkerId();
            job.cancel(now());
            persistRetired(job);
            revokeAttemptOnWorker(jobId, attemptId, workerId, "cancelled by client");
            log.info("Job cancelled by client: jobId={} previousWorker={}", jobId, workerId);
            return new CancelOutcome(true, true, job.snapshot());
        });
    }

    public List<JobSnapshot> listJobs() {
        return askCore(() -> jobs.values().stream()
                .sorted(Comparator.comparingLong(Job::createdAtMillis).reversed())
                .map(Job::snapshot)
                .toList());
    }

    public List<WorkerSnapshot> listWorkers() {
        return askCore(registry::snapshots);
    }

    // ------------------------------------------------------------------
    // Core-thread logic
    // ------------------------------------------------------------------

    /** Periodic maintenance: failure detection, deadline enforcement, retry promotion, scheduling. */
    private void sweep() {
        try {
            detectDeadWorkers();
            detectExpiredDeadlines();
            promoteDueRetries();
            scheduleQueuedJobs();
        } catch (RuntimeException unexpected) {
            // The sweep must survive; a thrown exception would cancel the periodic task.
            log.error("Sweep iteration failed", unexpected);
        }
    }

    private void detectDeadWorkers() {
        for (WorkerSession session : registry.sessionsSuspectedDead(now(), config.heartbeatTimeoutMillis())) {
            log.warn("Worker suspected dead (no heartbeat for >{} ms): workerId={}",
                    config.heartbeatTimeoutMillis(), session.workerId());
            handleWorkerLoss(session, "heartbeat timeout");
        }
    }

    /**
     * A lost worker cannot be trusted to still be executing its leases. Each of
     * its running jobs is retried (with backoff) or failed if the attempt budget
     * is spent. A slow-but-alive worker may later report a result for an
     * invalidated lease; {@link #applyTaskResult} rejects it as stale.
     */
    private void handleWorkerLoss(WorkerSession session, String reason) {
        registry.remove(session);
        invalidateAssignments(session, reason);
        session.close();
        // No scheduling here: the invalidated jobs sit in RETRY_WAIT until the
        // sweep promotes them, and losing a worker frees no other capacity.
    }

    private void invalidateAssignments(WorkerSession session, String reason) {
        for (String jobId : session.activeJobIds()) {
            Job job = jobs.get(jobId);
            if (job == null || job.state() != JobState.RUNNING
                    || !session.workerId().equals(job.assignedWorkerId())) {
                continue;
            }
            retryOrFail(job, "worker " + session.workerId() + " lost (" + reason + ")");
        }
    }

    /**
     * A worker being alive does not imply every task on it is making progress.
     * Each RUNNING attempt carries a coordinator-clock deadline; once it
     * expires the attempt's lease is revoked and the job retried, exactly as
     * if the worker had been lost — except the worker stays registered and a
     * best-effort {@code TASK_CANCEL} asks it to stop the wasted work. Expiry
     * is suspicion that the task exceeded its execution contract, not proof of
     * failure: if the task finishes anyway, its result fails the lease check.
     */
    private void detectExpiredDeadlines() {
        long now = now();
        for (Job job : jobs.values()) {
            if (!job.isDeadlineExpired(now)) {
                continue;
            }
            // Capture lease identity before retryOrFail clears it.
            String attemptId = job.currentAttemptId();
            String workerId = job.assignedWorkerId();
            long overdueBy = now - job.deadlineMillis();
            log.warn("Task execution deadline expired: jobId={} workerId={} attempt={}/{} "
                            + "timeoutMillis={} overdueMillis={}",
                    job.id(), workerId, job.attempts(), job.maxAttempts(),
                    job.executionTimeoutMillis(), overdueBy);
            revokeAttemptOnWorker(job.id(), attemptId, workerId, "deadline exceeded");
            retryOrFail(job, "execution deadline exceeded ("
                    + job.executionTimeoutMillis() + " ms) on worker " + workerId);
        }
    }

    /**
     * Best-effort request that {@code workerId} stop executing {@code attemptId},
     * and release the worker's capacity slot for it. Safe to call even if the
     * send fails: the caller revokes the lease anyway, so any result the task
     * still produces is stale-rejected. Shared by deadline expiry and
     * client-requested cancellation.
     */
    private void revokeAttemptOnWorker(String jobId, String attemptId, String workerId, String reason) {
        if (workerId == null || attemptId == null) {
            return;
        }
        registry.find(workerId).ifPresent(session -> {
            session.removeActiveJob(jobId);
            try {
                session.send(new TaskCancel(jobId, attemptId));
                log.info("Sent TASK_CANCEL: jobId={} workerId={} attemptId={} reason=\"{}\"",
                        jobId, workerId, attemptId, reason);
            } catch (IOException sendFailed) {
                log.debug("TASK_CANCEL to {} failed (harmless, lease already revoked): {}",
                        workerId, sendFailed.getMessage());
            }
        });
    }

    private void retryOrFail(Job job, String attemptError) {
        long now = now();
        if (job.hasAttemptsLeft()) {
            long delay = retryPolicy.delayBeforeNextAttemptMillis(job.attempts());
            job.scheduleRetry(attemptError, now + delay, now);
            log.info("Retry scheduled: jobId={} attempt={}/{} delayMillis={} cause=\"{}\"",
                    job.id(), job.attempts(), job.maxAttempts(), delay, attemptError);
            // Still active (RETRY_WAIT): the active-job count is unchanged.
            repository.save(job);
        } else {
            job.failPermanently(attemptError + " (all " + job.maxAttempts() + " attempts used)", now);
            log.warn("Job failed permanently: jobId={} attempts={} error=\"{}\"",
                    job.id(), job.attempts(), attemptError);
            persistRetired(job);
        }
    }

    private void promoteDueRetries() {
        long now = now();
        for (Job job : jobs.values()) {
            if (job.state() == JobState.RETRY_WAIT && job.isEligibleToRun(now)) {
                job.requeueAfterRetryWait(now);
                repository.save(job);
                log.info("Job requeued after backoff: jobId={} attempt={}/{}",
                        job.id(), job.attempts(), job.maxAttempts());
            }
        }
    }

    /** Assigns QUEUED jobs (oldest first) to workers with free capacity (least loaded first). */
    private void scheduleQueuedJobs() {
        List<Job> queued = jobs.values().stream()
                .filter(job -> job.state() == JobState.QUEUED)
                .sorted(Comparator.comparingLong(Job::createdAtMillis))
                .toList();
        for (Job job : queued) {
            if (job.state() != JobState.QUEUED) {
                continue; // a failed send earlier in this loop may have moved it
            }
            List<WorkerSession> available = registry.availableWorkers();
            if (available.isEmpty()) {
                return;
            }
            assign(job, available.getFirst());
        }
    }

    private void assign(Job job, WorkerSession worker) {
        String attemptId = job.assignTo(worker.workerId(), now());
        worker.addActiveJob(job.id());
        repository.save(job);
        TaskAssign message = new TaskAssign(
                job.id(), attemptId, job.attempts(), job.taskType(), job.payload());
        try {
            worker.send(message);
            log.info("Job assigned: jobId={} workerId={} attempt={}/{} attemptId={}",
                    job.id(), worker.workerId(), job.attempts(), job.maxAttempts(), attemptId);
        } catch (IOException sendFailed) {
            log.warn("Assignment send failed, treating worker {} as lost", worker.workerId());
            handleWorkerLoss(worker, "send failed: " + sendFailed.getMessage());
        }
    }

    private void applyTaskResult(TaskResult result) {
        // Free the reporting worker's slot regardless of lease validity.
        registry.find(result.workerId())
                .ifPresent(session -> session.removeActiveJob(result.jobId()));

        Job job = jobs.get(result.jobId());
        if (job == null) {
            log.warn("Result for unknown job rejected: jobId={} workerId={}",
                    result.jobId(), result.workerId());
            return;
        }
        if (!job.isCurrentAttempt(result.attemptId())) {
            log.warn("Stale result rejected: jobId={} workerId={} attemptId={} jobState={}",
                    result.jobId(), result.workerId(), result.attemptId(), job.state());
            return;
        }
        if (result.success()) {
            job.complete(result.result(), now());
            persistRetired(job);
            log.info("Job completed: jobId={} workerId={} attempts={}",
                    job.id(), result.workerId(), job.attempts());
        } else {
            log.info("Attempt failed: jobId={} workerId={} error=\"{}\"",
                    job.id(), result.workerId(), result.error());
            retryOrFail(job, result.error());
        }
        scheduleQueuedJobs();
    }

    /**
     * Coordinator-restart rule: persisted RUNNING jobs reference worker sessions
     * that no longer exist, and the outcome of those attempts is unknown. Each
     * one is requeued if it still has attempt budget, otherwise failed. This is
     * where at-least-once semantics shows up across restarts: the lost attempt
     * may in fact have executed on a still-alive worker.
     */
    private void recoverFromRepository() {
        List<Job> stored = repository.loadAll();
        if (stored.isEmpty()) {
            log.info("Recovery: no persisted jobs found, starting clean");
            return;
        }
        log.info("Recovery started: {} persisted jobs", stored.size());
        int requeued = 0;
        for (Job job : stored) {
            if (job.state() == JobState.RUNNING) {
                if (job.hasAttemptsLeft()) {
                    job.requeueForRecovery(now());
                    requeued++;
                    log.info("Recovery requeued in-flight job: jobId={} attempt budget {}/{}",
                            job.id(), job.attempts(), job.maxAttempts());
                } else {
                    job.failPermanently(
                            "coordinator restarted during final attempt; outcome unknown", now());
                    log.warn("Recovery failed job with exhausted attempts: jobId={}", job.id());
                }
                repository.save(job);
            }
            jobs.put(job.id(), job);
            // Rebuild the dedup map so idempotent submission survives restart. A
            // job keeps its key in every state, including terminal, so a resend
            // of a completed/failed/cancelled key still returns the same job.
            if (job.idempotencyKey() != null) {
                jobIdByKey.put(job.idempotencyKey(), job.id());
            }
            // Seed the active-job counter from each job's post-recovery state, so
            // admission control resumes with an exact count. A restart may leave
            // activeJobCount above a since-lowered limit; that is intended — the
            // recovered jobs run, and only new submissions are rejected until the
            // count drops back below the limit.
            if (!job.state().isTerminal()) {
                activeJobCount++;
            }
        }
        log.info("Recovery completed: {} jobs loaded, {} requeued, {} active",
                stored.size(), requeued, activeJobCount);
    }

    // ------------------------------------------------------------------
    // Core-thread plumbing
    // ------------------------------------------------------------------

    private long now() {
        return System.currentTimeMillis();
    }

    /**
     * The single place a job leaves the active set. Every active → terminal
     * transition (completion, permanent failure, cancellation) is persisted
     * through here so {@link #activeJobCount} is decremented exactly once, in one
     * spot, regardless of which path retired the job. The guard makes underflow
     * impossible: the counter can never go negative even if a transition were
     * ever double-applied.
     */
    private void persistRetired(Job job) {
        if (activeJobCount > 0) {
            activeJobCount--;
        } else {
            log.error("activeJobCount underflow prevented while retiring job {}", job.id());
        }
        repository.save(job);
    }

    private void runOnCore(Runnable event) {
        // Once shutdown begins, no further state transitions are applied: a
        // graceful stop leaves the same durable state as a crash, so recovery
        // has exactly one shutdown shape to reason about.
        if (coreThread.isShutdown()) {
            return;
        }
        try {
            coreThread.execute(() -> {
                try {
                    event.run();
                } catch (RuntimeException unexpected) {
                    log.error("Core event failed", unexpected);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
            log.debug("Event dropped during shutdown");
        }
    }

    /** Runs a query on the core thread and waits for the answer. */
    private <T> T askCore(Supplier<T> query) {
        try {
            return CompletableFuture.supplyAsync(query, coreThread).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for coordinator core", interrupted);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Coordinator core request failed", e);
        }
    }

    @Override
    public void close() {
        shutdownExecutor(coreThread);
        repository.close();
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
