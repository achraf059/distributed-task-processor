package io.github.achrafaittayeb.dtp.common.protocol;

/** Client request for the current state of one job. */
public record GetJobStatus(String jobId) implements Message {
}
