package io.github.achrafaittayeb.dtp.coordinator.net;

import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.net.ProtocolException;
import io.github.achrafaittayeb.dtp.common.protocol.ErrorReply;
import io.github.achrafaittayeb.dtp.common.protocol.Heartbeat;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import io.github.achrafaittayeb.dtp.common.protocol.TaskResult;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerRegister;
import io.github.achrafaittayeb.dtp.common.protocol.WorkerRegistered;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorCore;
import io.github.achrafaittayeb.dtp.coordinator.workers.WorkerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Accepts worker connections. One reader thread per worker: cheap at this
 * scale (tens of workers) and much easier to reason about than NIO selectors.
 *
 * <p>The first message on a connection must be {@link WorkerRegister}; after
 * that only {@link Heartbeat} and {@link TaskResult} are legal. Anything else
 * — or an undecodable frame — closes that one connection without affecting
 * the rest of the coordinator.
 */
public final class WorkerServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerServer.class);

    private final ServerSocket serverSocket;
    private final CoordinatorCore core;
    private final Set<Socket> openSockets = ConcurrentHashMap.newKeySet();
    private final Thread acceptorThread;
    private volatile boolean closed;

    public WorkerServer(int port, CoordinatorCore core) throws IOException {
        // SO_REUSEADDR so a restarted coordinator can rebind its port while old
        // connections linger in TIME_WAIT.
        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new java.net.InetSocketAddress(port));
        this.core = core;
        this.acceptorThread = new Thread(this::acceptLoop, "worker-acceptor");
    }

    public void start() {
        acceptorThread.start();
        log.info("Listening for workers on port {}", port());
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
                Thread reader = new Thread(() -> handleConnection(socket),
                        "worker-conn-" + socket.getRemoteSocketAddress());
                reader.start();
            } catch (IOException e) {
                if (!closed) {
                    log.error("Worker accept failed", e);
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        WorkerSession session = null;
        try {
            InputStream in = socket.getInputStream();
            session = awaitRegistration(socket, in);
            if (session == null) {
                return; // peer closed or spoke garbage before registering
            }
            readLoop(session, in);
            core.onWorkerDisconnected(session, "connection closed");
        } catch (ProtocolException badPeer) {
            log.warn("Protocol violation from {}: {}", socket.getRemoteSocketAddress(), badPeer.getMessage());
            if (session != null) {
                core.onWorkerDisconnected(session, "protocol violation");
            }
        } catch (IOException io) {
            if (session != null) {
                core.onWorkerDisconnected(session, io.getMessage() == null ? "I/O error" : io.getMessage());
            }
        } finally {
            closeQuietly(socket);
            openSockets.remove(socket);
        }
    }

    private WorkerSession awaitRegistration(Socket socket, InputStream in) throws IOException {
        Message first = MessageIO.receive(in);
        if (first == null) {
            return null;
        }
        if (!(first instanceof WorkerRegister register)) {
            MessageIO.send(socket.getOutputStream(),
                    new ErrorReply("First message must be WORKER_REGISTER"));
            throw new ProtocolException("Expected WORKER_REGISTER, got "
                    + first.getClass().getSimpleName());
        }
        if (register.workerId() == null || register.workerId().isBlank()
                || register.capacity() < 1 || register.capacity() > 1024) {
            MessageIO.send(socket.getOutputStream(),
                    new ErrorReply("Registration requires a worker id and capacity in 1..1024"));
            throw new ProtocolException("Invalid registration: " + register);
        }
        WorkerSession session = new WorkerSession(register.workerId(), register.capacity(),
                socket.getOutputStream(), socket, System.currentTimeMillis());
        core.onWorkerRegistered(session);
        session.send(new WorkerRegistered(register.workerId()));
        return session;
    }

    private void readLoop(WorkerSession session, InputStream in) throws IOException {
        Message message;
        while ((message = MessageIO.receive(in)) != null) {
            switch (message) {
                case Heartbeat heartbeat -> {
                    log.debug("Heartbeat from {}", heartbeat.workerId());
                    core.onHeartbeat(session.workerId());
                }
                case TaskResult result -> core.onTaskResult(result);
                default -> throw new ProtocolException("Unexpected message from registered worker: "
                        + message.getClass().getSimpleName());
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(serverSocket);
        openSockets.forEach(WorkerServer::closeQuietly);
        try {
            acceptorThread.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best-effort shutdown
        }
    }
}
