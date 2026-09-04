package io.github.achrafaittayeb.dtp.coordinator.workers;

import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * One live worker connection as seen by the coordinator: identity, capacity,
 * heartbeat freshness, and the set of jobs currently leased to it.
 *
 * <p>Bookkeeping fields are confined to the coordinator core thread. Only
 * {@link #send} and {@link #close} touch the connection; sends are serialized
 * by a lock on the output stream because frames from concurrent writers would
 * interleave and corrupt the stream.
 */
public final class WorkerSession {

    private static final Logger log = LoggerFactory.getLogger(WorkerSession.class);

    private final String workerId;
    private final int capacity;
    private final OutputStream out;
    private final Closeable connection;
    private final Set<String> activeJobIds = new HashSet<>();

    private long lastHeartbeatMillis;

    public WorkerSession(String workerId, int capacity, OutputStream out,
                         Closeable connection, long now) {
        this.workerId = workerId;
        this.capacity = capacity;
        this.out = out;
        this.connection = connection;
        this.lastHeartbeatMillis = now;
    }

    /** Sends one message; throws {@link IOException} if the connection is broken. */
    public void send(Message message) throws IOException {
        synchronized (out) {
            MessageIO.send(out, message);
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
            log.debug("Error closing connection of worker {}", workerId, e);
        }
    }

    public void recordHeartbeat(long now) {
        lastHeartbeatMillis = now;
    }

    public long lastHeartbeatMillis() {
        return lastHeartbeatMillis;
    }

    public String workerId() {
        return workerId;
    }

    public int capacity() {
        return capacity;
    }

    public int freeCapacity() {
        return capacity - activeJobIds.size();
    }

    public void addActiveJob(String jobId) {
        activeJobIds.add(jobId);
    }

    public void removeActiveJob(String jobId) {
        activeJobIds.remove(jobId);
    }

    public Set<String> activeJobIds() {
        return Set.copyOf(activeJobIds);
    }
}
