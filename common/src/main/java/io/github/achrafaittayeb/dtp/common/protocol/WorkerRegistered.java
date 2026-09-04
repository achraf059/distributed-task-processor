package io.github.achrafaittayeb.dtp.common.protocol;

/** Coordinator's acknowledgement of a successful {@link WorkerRegister}. */
public record WorkerRegistered(String workerId) implements Message {
}
