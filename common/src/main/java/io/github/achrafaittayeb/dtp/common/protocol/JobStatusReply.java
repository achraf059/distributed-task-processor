package io.github.achrafaittayeb.dtp.common.protocol;

import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;

/** Reply to {@link GetJobStatus}. */
public record JobStatusReply(JobSnapshot job) implements Message {
}
