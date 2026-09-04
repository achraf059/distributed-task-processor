package io.github.achrafaittayeb.dtp.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.model.WorkerSnapshot;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
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
    private final ScheduledExecutorService coreThread;

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

    public String submitJob(TaskType taskType, JsonNode payload, int requestedMaxAttempts)
            throws InvalidPayloadException {
        TaskPayloads.validate(taskType, payload);
        int maxAttempts = requestedMaxAttempts > 0 ? requestedMaxAttempts : config.defaultMaxAttempts();
        return askCore(() -> {
            Job job = Job.createQueued(taskType, payload, maxAttempts, now());
            jobs.put(job.id(), job);
            repository.save(job);
            log.info("Job submitted: jobId={} type={} maxAttempts={}",
                    job.id(), taskType, maxAttempts);
            scheduleQueuedJobs();
            return job.id();
        });
    }

    public Optional<JobSnapshot> getJob(String jobId) {
        return askCore(() -> Optional.ofNullable(jobs.get(jobId)).map(Job::snapshot));
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

    /** Periodic maintenance: failure detection, retry promotion, scheduling. */
    private void sweep() {
        try {
            detectDeadWorkers();
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

    private void retryOrFail(Job job, String attemptError) {
        long now = now();
        if (job.hasAttemptsLeft()) {
            long delay = retryPolicy.delayBeforeNextAttemptMillis(job.attempts());
            job.scheduleRetry(attemptError, now + delay, now);
            log.info("Retry scheduled: jobId={} attempt={}/{} delayMillis={} cause=\"{}\"",
                    job.id(), job.attempts(), job.maxAttempts(), delay, attemptError);
        } else {
            job.failPermanently(attemptError + " (all " + job.maxAttempts() + " attempts used)", now);
            log.warn("Job failed permanently: jobId={} attempts={} error=\"{}\"",
                    job.id(), job.attempts(), attemptError);
        }
        repository.save(job);
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
            repository.save(job);
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
        }
        log.info("Recovery completed: {} jobs loaded, {} requeued", stored.size(), requeued);
    }

    // ------------------------------------------------------------------
    // Core-thread plumbing
    // ------------------------------------------------------------------

    private long now() {
        return System.currentTimeMillis();
    }

    private void runOnCore(Runnable event) {
        coreThread.execute(() -> {
            try {
                event.run();
            } catch (RuntimeException unexpected) {
                log.error("Core event failed", unexpected);
            }
        });
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
