package io.github.achrafaittayeb.dtp.coordinator.net;

import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.net.ProtocolException;
import io.github.achrafaittayeb.dtp.common.protocol.ErrorReply;
import io.github.achrafaittayeb.dtp.common.protocol.GetJobStatus;
import io.github.achrafaittayeb.dtp.common.protocol.JobListReply;
import io.github.achrafaittayeb.dtp.common.protocol.JobStatusReply;
import io.github.achrafaittayeb.dtp.common.protocol.JobSubmitted;
import io.github.achrafaittayeb.dtp.common.protocol.ListJobs;
import io.github.achrafaittayeb.dtp.common.protocol.ListWorkers;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import io.github.achrafaittayeb.dtp.common.protocol.SubmitJob;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerListReply;
import io.github.achrafaittayeb.dtp.common.task.InvalidPayloadException;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Accepts client connections and serves a simple request/response protocol:
 * each request frame gets exactly one reply frame on the same connection.
 * A client may issue many requests over one connection (the CLI's
 * {@code wait} command polls this way).
 */
public final class ClientServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClientServer.class);

    private final ServerSocket serverSocket;
    private final CoordinatorCore core;
    private final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();
    private final Thread acceptorThread;
    private volatile boolean closed;

    public ClientServer(int port, CoordinatorCore core) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.core = core;
        this.acceptorThread = new Thread(this::acceptLoop, "client-acceptor");
    }

    public void start() {
        acceptorThread.start();
        log.info("Listening for clients on port {}", port());
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                openSockets.add(socket);
                new Thread(() -> handleConnection(socket),
                        "client-conn-" + socket.getRemoteSocketAddress()).start();
            } catch (IOException e) {
                if (!closed) {
                    log.error("Client accept failed", e);
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            Message request;
            while ((request = MessageIO.receive(in)) != null) {
                MessageIO.send(out, handle(request));
            }
        } catch (ProtocolException badPeer) {
            log.warn("Client protocol violation from {}: {}",
                    socket.getRemoteSocketAddress(), badPeer.getMessage());
            tryReplyError(socket, "Malformed request: " + badPeer.getMessage());
        } catch (IOException disconnected) {
            log.debug("Client connection ended: {}", disconnected.getMessage());
        } finally {
            openSockets.remove(socket);
        }
    }

    private Message handle(Message request) {
        try {
            return switch (request) {
                case SubmitJob submit -> new JobSubmitted(
                        core.submitJob(submit.taskType(), submit.payload(), submit.maxAttempts()));
                case GetJobStatus status -> core.getJob(status.jobId())
                        .<Message>map(JobStatusReply::new)
                        .orElseGet(() -> new ErrorReply("Unknown job: " + status.jobId()));
                case ListJobs ignored -> new JobListReply(core.listJobs());
                case ListWorkers ignored -> new WorkerListReply(core.listWorkers());
                default -> new ErrorReply("Unsupported request: "
                        + request.getClass().getSimpleName());
            };
        } catch (InvalidPayloadException invalid) {
            return new ErrorReply("Invalid payload: " + invalid.getMessage());
        } catch (RuntimeException unexpected) {
            log.error("Client request failed", unexpected);
            return new ErrorReply("Internal error handling "
                    + request.getClass().getSimpleName());
        }
    }

    private void tryReplyError(Socket socket, String message) {
        try {
            MessageIO.send(socket.getOutputStream(), new ErrorReply(message));
        } catch (IOException ignored) {
            // connection already unusable
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // shutting down
        }
        for (Socket socket : openSockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // shutting down
            }
        }
        try {
            acceptorThread.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
