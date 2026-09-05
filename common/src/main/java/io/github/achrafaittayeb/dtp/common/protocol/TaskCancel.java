package io.github.achrafaittayeb.dtp.common.protocol;

/**
 * Coordinator's request that a worker stop one specific attempt. The worker
 * must cancel only if both {@code jobId} and {@code attemptId} match its
 * currently executing assignment — a cancel for a superseded attempt must
 * never touch a newer one.
 *
 * <p>Delivery is best-effort and cancellation is cooperative (thread
 * interruption). Correctness never depends on it: by the time this message is
 * sent the attempt's lease is already revoked, so a task that ignores the
 * cancel can only ever produce a stale, rejected result.
 */
public record TaskCancel(String jobId, String attemptId) implements Message {
}
