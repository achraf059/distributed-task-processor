package io.github.achrafaittayeb.dtp.common.protocol;

/**
 * Periodic liveness signal, sent by a dedicated worker thread so long-running
 * tasks can never starve heartbeats. Absence of heartbeats past the configured
 * timeout makes the coordinator suspect the worker has failed.
 */
public record Heartbeat(String workerId) implements Message {
}
