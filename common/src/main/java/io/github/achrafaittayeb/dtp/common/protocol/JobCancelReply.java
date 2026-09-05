package io.github.achrafaittayeb.dtp.common.protocol;

import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;

/**
 * Reply to {@link CancelJob}. {@code cancelled} is true when this request moved
 * the job into CANCELLED; it is false when the job was already terminal (its
 * snapshot shows the actual final state). Unknown ids return {@link ErrorReply}.
 */
public record JobCancelReply(boolean cancelled, JobSnapshot job) implements Message {
}
