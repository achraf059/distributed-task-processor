package io.github.achrafaittayeb.dtp.it;

import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.protocol.Heartbeat;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import io.github.achrafaittayeb.dtp.common.protocol.TaskAssign;
import io.github.achrafaittayeb.dtp.common.protocol.TaskCancel;
import io.github.achrafaittayeb.dtp.common.protocol.TaskResult;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerRegister;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerRegistered;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A worker impostor driven directly by tests. Unlike the real {@link
 * io.github.achrafaittayeb.dtp.worker.Worker}, it never executes anything —
 * tests decide exactly when it heartbeats, what results it reports, and with
 * which attempt id, which is how heartbeat-timeout and stale-result scenarios
 * are produced deterministically.
 */
final class ScriptedWorker implements AutoCloseable {

    private final String workerId;
    private final Socket socket;
    private final OutputStream out;
    private final BlockingQueue<TaskAssign> assignments = new LinkedBlockingQueue<>();
    private final BlockingQueue<TaskCancel> cancels = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService heartbeater =
            Executors.newSingleThreadScheduledExecutor();
    private final Thread readerThread;
    private volatile boolean heartbeating = true;

    static ScriptedWorker register(int coordinatorWorkerPort, String workerId, int capacity)
            throws IOException {
        return new ScriptedWorker(coordinatorWorkerPort, workerId, capacity);
    }

    private ScriptedWorker(int port, String workerId, int capacity) throws IOException {
        this.workerId = workerId;
        this.socket = new Socket("localhost", port);
        this.socket.setTcpNoDelay(true);
        this.out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        MessageIO.send(out, new WorkerRegister(workerId, capacity));
        Message reply = MessageIO.receive(in);
        if (!(reply instanceof WorkerRegistered)) {
            throw new IOException("Registration failed: " + reply);
        }

        this.readerThread = new Thread(() -> readLoop(in), "scripted-" + workerId + "-reader");
        this.readerThread.start();
        this.heartbeater.scheduleAtFixedRate(this::maybeHeartbeat,
                0, Testbed.WORKER_HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void readLoop(InputStream in) {
        try {
            Message message;
            while ((message = MessageIO.receive(in)) != null) {
                if (message instanceof TaskAssign assign) {
                    assignments.add(assign);
                } else if (message instanceof TaskCancel cancel) {
                    cancels.add(cancel);
                }
            }
        } catch (IOException endOfConnection) {
            // expected when the coordinator declares this worker dead
        }
    }

    private void maybeHeartbeat() {
        if (!heartbeating) {
            return;
        }
        try {
            synchronized (out) {
                MessageIO.send(out, new Heartbeat(workerId));
            }
        } catch (IOException connectionGone) {
            heartbeating = false;
        }
    }

    TaskAssign awaitAssignment() throws InterruptedException {
        TaskAssign assign = assignments.poll(10, TimeUnit.SECONDS);
        if (assign == null) {
            throw new AssertionError("Scripted worker " + workerId + " received no assignment");
        }
        return assign;
    }

    /** Removes and returns all assignments received so far. */
    java.util.List<TaskAssign> drainAssignments() {
        java.util.List<TaskAssign> drained = new java.util.ArrayList<>();
        assignments.drainTo(drained);
        return drained;
    }

    /** Blocks until a TASK_CANCEL arrives, or fails the test after the timeout. */
    TaskCancel awaitCancel() throws InterruptedException {
        TaskCancel cancel = cancels.poll(10, TimeUnit.SECONDS);
        if (cancel == null) {
            throw new AssertionError("Scripted worker " + workerId + " received no TASK_CANCEL");
        }
        return cancel;
    }

    /** Simulates a hung process: the TCP connection stays open but liveness stops. */
    void stopHeartbeats() {
        heartbeating = false;
    }

    void sendSuccess(TaskAssign assign, String result) throws IOException {
        sendResult(TaskResult.success(workerId, assign.jobId(), assign.attemptId(), result));
    }

    void sendFailure(TaskAssign assign, String error) throws IOException {
        sendResult(TaskResult.failure(workerId, assign.jobId(), assign.attemptId(), error));
    }

    void sendResult(TaskResult result) throws IOException {
        synchronized (out) {
            MessageIO.send(out, result);
        }
    }

    @Override
    public void close() {
        heartbeater.shutdownNow();
        try {
            socket.close();
        } catch (IOException ignored) {
            // already closed by the coordinator in most failure tests
        }
        try {
            readerThread.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
