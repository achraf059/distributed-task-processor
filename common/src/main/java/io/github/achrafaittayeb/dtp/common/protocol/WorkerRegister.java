package io.github.achrafaittayeb.dtp.common.protocol;

/**
 * First message a worker sends after connecting. {@code capacity} is the number
 * of tasks the worker is willing to run concurrently; the coordinator never
 * assigns more than that.
 */
public record WorkerRegister(String workerId, int capacity) implements Message {
}
