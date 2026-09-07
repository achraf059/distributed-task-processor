package io.github.achrafaittayeb.dtp.client;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.model.WorkerSnapshot;
import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.net.ProtocolException;
import io.github.achrafaittayeb.dtp.common.protocol.CancelJob;
import io.github.achrafaittayeb.dtp.common.protocol.ErrorReply;
import io.github.achrafaittayeb.dtp.common.protocol.GetJobStatus;
import io.github.achrafaittayeb.dtp.common.protocol.JobCancelReply;
import io.github.achrafaittayeb.dtp.common.protocol.JobListReply;
import io.github.achrafaittayeb.dtp.common.protocol.JobStatusReply;
import io.github.achrafaittayeb.dtp.common.protocol.JobSubmitted;
import io.github.achrafaittayeb.dtp.common.protocol.ListJobs;
import io.github.achrafaittayeb.dtp.common.protocol.ListWorkers;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import io.github.achrafaittayeb.dtp.common.protocol.SubmitJob;
import io.github.achrafaittayeb.dtp.common.protocol.SubmitRejected;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerListReply;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

/**
 * Blocking client for the coordinator's request/response protocol. One
 * request is in flight at a time per instance (methods are synchronized),
 * which matches the one-reply-per-request wire contract.
 */
public final class CoordinatorClient implements AutoCloseable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final SubmitRetrier submitRetrier;

    public CoordinatorClient(String host, int port) throws IOException {
        this(host, port, ClientRetryPolicy.none());
    }

    /**
     * Connects with an explicit submission-retry policy. {@link ClientRetryPolicy#none()}
     * (the default of the two-arg constructor) preserves the historical
     * one-shot behavior; any other policy re-attempts admission after a
     * retryable overload rejection.
     */
    public CoordinatorClient(String host, int port, ClientRetryPolicy retryPolicy)
            throws IOException {
        this(host, port, new SubmitRetrier(retryPolicy));
    }

    /** Test seam: inject a retrier with a fake sleeper/RNG for deterministic timing. */
    CoordinatorClient(String host, int port, SubmitRetrier submitRetrier) throws IOException {
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true);
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.submitRetrier = submitRetrier;
    }

    /**
     * Submits a job; returns its id. {@code maxAttempts <= 0} uses the server
     * default. When configured with a retrying {@link ClientRetryPolicy}, a
     * retryable overload rejection is re-attempted with jittered back-off; the
     * back-off sleep happens outside this instance's synchronization, so it
     * never blocks another thread's wire exchange on the same connection. (A
     * connection still serves one request at a time, so concurrent callers on
     * one client are not expected; the point is that sleeping holds no monitor.)
     *
     * @throws SubmitRejectedException if the coordinator refused the submission
     *     under overload and the retry budget (if any) was exhausted — a valid
     *     request refused, distinct from a malformed-request error
     * @throws IOException on any other error reply or transport failure (never
     *     retried, since a job may already exist)
     */
    public String submit(TaskType taskType, JsonNode payload, int maxAttempts)
            throws IOException {
        return submit(taskType, payload, maxAttempts, null);
    }

    /**
     * Submits with an optional idempotency key. A {@code null} key is the
     * historical behavior (always a fresh job). A non-null, stable key lets the
     * coordinator deduplicate: resubmitting the same key returns the original
     * job's id instead of creating another logical job. Resubmitting a key that
     * the coordinator bound to a <em>different</em> request (task type, payload,
     * or max attempts) is a conflict, surfaced as a plain {@link IOException} —
     * not a {@link SubmitRejectedException}, so the retry loop never retries it.
     */
    public String submit(TaskType taskType, JsonNode payload, int maxAttempts, String idempotencyKey)
            throws IOException {
        return submitRetrier.submit(() -> submitOnce(taskType, payload, maxAttempts, idempotencyKey));
    }

    /** One submission exchange over the wire; synchronized like every other request. */
    private synchronized String submitOnce(TaskType taskType, JsonNode payload, int maxAttempts,
                                           String idempotencyKey) throws IOException {
        Message reply = exchange(new SubmitJob(taskType, payload, maxAttempts, idempotencyKey));
        if (reply instanceof JobSubmitted submitted) {
            return submitted.jobId();
        }
        if (reply instanceof SubmitRejected rejected) {
            throw new SubmitRejectedException(rejected);
        }
        throw asError(reply);
    }

    public synchronized JobSnapshot status(String jobId) throws IOException {
        Message reply = exchange(new GetJobStatus(jobId));
        if (reply instanceof JobStatusReply status) {
            return status.job();
        }
        throw asError(reply);
    }

    public synchronized List<JobSnapshot> listJobs() throws IOException {
        Message reply = exchange(new ListJobs());
        if (reply instanceof JobListReply list) {
            return list.jobs();
        }
        throw asError(reply);
    }

    public synchronized List<WorkerSnapshot> listWorkers() throws IOException {
        Message reply = exchange(new ListWorkers());
        if (reply instanceof WorkerListReply list) {
            return list.workers();
        }
        throw asError(reply);
    }

    /**
     * Requests cancellation of a job. Returns the job's snapshot; check its
     * state to see the outcome (CANCELLED if this call cancelled it, or the
     * job's actual terminal state if it had already finished).
     *
     * @throws IOException if the job id is unknown
     */
    public synchronized JobCancelReply cancel(String jobId) throws IOException {
        Message reply = exchange(new CancelJob(jobId));
        if (reply instanceof JobCancelReply cancelReply) {
            return cancelReply;
        }
        throw asError(reply);
    }

    /**
     * Polls until the job reaches a terminal state (COMPLETED or FAILED).
     *
     * @throws IOException if the timeout elapses first
     */
    public JobSnapshot awaitTerminal(String jobId, Duration pollInterval, Duration timeout)
            throws IOException, InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            JobSnapshot snapshot = status(jobId);
            if (snapshot.state().isTerminal()) {
                return snapshot;
            }
            if (System.nanoTime() > deadline) {
                throw new IOException("Job " + jobId + " not terminal within " + timeout
                        + " (state " + snapshot.state() + ")");
            }
            Thread.sleep(pollInterval.toMillis());
        }
    }

    private Message exchange(Message request) throws IOException {
        MessageIO.send(out, request);
        Message reply = MessageIO.receive(in);
        if (reply == null) {
            throw new IOException("Coordinator closed the connection");
        }
        return reply;
    }

    private static IOException asError(Message reply) {
        if (reply instanceof ErrorReply error) {
            return new IOException(error.message());
        }
        return new ProtocolException("Unexpected reply: " + reply.getClass().getSimpleName());
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
