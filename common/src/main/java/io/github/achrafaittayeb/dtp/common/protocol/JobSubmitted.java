package io.github.achrafaittayeb.dtp.common.protocol;

/** Reply to {@link SubmitJob}: the job is durably stored and queued. */
public record JobSubmitted(String jobId) implements Message {
}
