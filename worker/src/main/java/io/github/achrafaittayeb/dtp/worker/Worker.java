package io.github.achrafaittayeb.dtp.worker;

import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.net.ProtocolException;
import io.github.achrafaittayeb.dtp.common.protocol.Heartbeat;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.common.protocol.TaskCancel;
import io.github.achrafaittayeb.dtp.common.protocol.TaskResult;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerRegister;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerRegistered;
import io.github.achrafaittayeb.dtp.common.task.TaskPayloads;
import io.github.achrafaittayeb.dtp.worker.tasks.TaskRunner;
import io.github.achrafaittayeb.dtp.worker.tasks.TaskRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Worker process: connects to the coordinator, registers, then serves task
 * assignments until told to stop, reconnecting after connection loss.
 *
 * <p>Thread model — three independent concerns, three executors:
 * <ul>
 *   <li>a connection thread that reads assignments (blocking I/O);</li>
 *   <li>a scheduled heartbeat thread, deliberately separate from task
 *       execution so a saturated task pool can never starve heartbeats;</li>
 *   <li>a fixed task pool of {@code capacity} threads.</li>
 * </ul>
 *
 * <p>When a connection dies, in-flight tasks are interrupted: the coordinator
 * invalidates their leases the moment it notices the disconnect, so their
 * results could only ever be rejected as stale.
 */
public final class Worker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final WorkerConfig config;
    private final ExecutorService taskPool;
    private final ScheduledExecutorService heartbeatScheduler;
    private final Thread connectionThread;
    private final InFlightTasks inFlightTasks = new InFlightTasks();
    private volatile boolean running = true;
    private volatile Socket currentSocket;

    public Worker(WorkerConfig config) {
        this.config = config;
        this.taskPool = Executors.newFixedThreadPool(config.capacity(),
                namedThreadFactory(config.workerId() + "-task"));
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
                namedThreadFactory(config.workerId() + "-heartbeat"));
        this.connectionThread = new Thread(this::connectionLoop, config.workerId() + "-connection");
    }

    public void start() {
        connectionThread.start();
    }

    public String workerId() {
        return config.workerId();
    }

    private void connectionLoop() {
        while (running) {
            try {
                runOneConnection();
            } catch (IOException connectionProblem) {
                if (running) {
                    log.warn("Connection to coordinator lost: {}", connectionProblem.getMessage());
                }
            }
            cancelInFlightTasks();
            if (running) {
                log.info("Reconnecting to coordinator in {} ms", config.reconnectDelayMillis());
                sleepQuietly(config.reconnectDelayMillis());
            }
        }
    }

    private void runOneConnection() throws IOException {
        Socket socket = new Socket(config.coordinatorHost(), config.coordinatorPort());
        socket.setTcpNoDelay(true);
        currentSocket = socket;
        try (socket) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            sendLocked(out, new WorkerRegister(config.workerId(), config.capacity()));
            Message reply = MessageIO.receive(in);
            if (!(reply instanceof WorkerRegistered)) {
                throw new ProtocolException("Registration rejected: " + reply);
            }
            log.info("Registered with coordinator at {}:{} as {} (capacity {})",
                    config.coordinatorHost(), config.coordinatorPort(),
                    config.workerId(), config.capacity());

            ScheduledFuture<?> heartbeats = startHeartbeats(out, socket);
            try {
                assignmentLoop(in, out);
            } finally {
                heartbeats.cancel(false);
            }
        } finally {
            currentSocket = null;
        }
    }

    private ScheduledFuture<?> startHeartbeats(OutputStream out, Socket socket) {
        return heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                sendLocked(out, new Heartbeat(config.workerId()));
            } catch (IOException heartbeatFailed) {
                log.warn("Heartbeat send failed; closing connection");
                closeQuietly(socket); // unblocks the assignment reader, triggering reconnect
            }
        }, 0, config.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void assignmentLoop(InputStream in, OutputStream out) throws IOException {
        Message message;
        while ((message = MessageIO.receive(in)) != null) {
            switch (message) {
                case TaskAssign assign -> executeAsync(assign, out);
                case TaskCancel cancel -> cancelTask(cancel);
                default -> log.warn("Ignoring unexpected message: {}",
                        message.getClass().getSimpleName());
            }
        }
        log.info("Coordinator closed the connection");
    }

    private void executeAsync(TaskAssign assign, OutputStream out) {
        log.info("Task accepted: jobId={} type={} attempt={}",
                assign.jobId(), assign.taskType(), assign.attemptNumber());
        Future<?> future = taskPool.submit(() -> {
            TaskResult result = execute(assign);
            // Remove only if this attempt is still the tracked one; a cancel or a
            // newer assignment for the same job must not be clobbered.
            inFlightTasks.remove(assign.jobId(), assign.attemptId());
            try {
                sendLocked(out, result);
                log.info("Task result sent: jobId={} success={}", assign.jobId(), result.success());
            } catch (IOException resultLost) {
                // The lease dies with the connection; the coordinator will retry the job.
                log.warn("Could not report result for jobId={}; connection is gone", assign.jobId());
            }
        });
        inFlightTasks.put(assign.jobId(), assign.attemptId(), future);
    }

    /**
     * Cooperatively cancels a running attempt. Only cancels if both jobId and
     * attemptId match the tracked assignment, so a cancel for a superseded
     * attempt cannot interrupt a newer one. Interruption is best-effort: tasks
     * that don't respond keep running, but their lease is already revoked at the
     * coordinator, so any result they produce is stale-rejected.
     */
    private void cancelTask(TaskCancel cancel) {
        boolean cancelled = inFlightTasks.cancelIfMatches(cancel.jobId(), cancel.attemptId());
        if (cancelled) {
            log.info("Cancelled task on request: jobId={} attemptId={}",
                    cancel.jobId(), cancel.attemptId());
        } else {
            log.debug("Ignoring TASK_CANCEL for untracked attempt: jobId={} attemptId={}",
                    cancel.jobId(), cancel.attemptId());
        }
    }

    private TaskResult execute(TaskAssign assign) {
        try {
            TaskPayloads.validate(assign.taskType(), assign.payload());
            TaskRunner runner = TaskRunners.forType(assign.taskType());
            String output = runner.run(assign.payload(), assign.attemptNumber());
            return TaskResult.success(config.workerId(), assign.jobId(), assign.attemptId(), output);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return TaskResult.failure(config.workerId(), assign.jobId(), assign.attemptId(),
                    "task interrupted");
        } catch (Exception taskFailed) {
            log.info("Task failed: jobId={} error={}", assign.jobId(), taskFailed.getMessage());
            return TaskResult.failure(config.workerId(), assign.jobId(), assign.attemptId(),
                    taskFailed.getMessage() == null
                            ? taskFailed.getClass().getSimpleName()
                            : taskFailed.getMessage());
        }
    }

    /** Frames must not interleave, so all writers lock the shared output stream. */
    private static void sendLocked(OutputStream out, Message message) throws IOException {
        synchronized (out) {
            MessageIO.send(out, message);
        }
    }

    private void cancelInFlightTasks() {
        inFlightTasks.cancelAll();
    }

    /** Graceful stop: close the connection, interrupt tasks, shut down pools. */
    @Override
    public void close() {
        running = false;
        closeQuietly(currentSocket);
        connectionThread.interrupt();
        heartbeatScheduler.shutdownNow();
        cancelInFlightTasks();
        taskPool.shutdownNow();
        try {
            connectionThread.join(2_000);
            taskPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        log.info("Worker {} stopped", config.workerId());
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already closing
            }
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static java.util.concurrent.ThreadFactory namedThreadFactory(String prefix) {
        java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger();
        return runnable -> new Thread(runnable, prefix + "-" + counter.incrementAndGet());
    }
}
