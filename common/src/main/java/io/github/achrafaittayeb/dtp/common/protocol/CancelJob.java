package io.github.achrafaittayeb.dtp.common.protocol;

/** Client request to cancel a job by id. */
public record CancelJob(String jobId) implements Message {
}
