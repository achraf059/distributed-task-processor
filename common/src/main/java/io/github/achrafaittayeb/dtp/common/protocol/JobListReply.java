package io.github.achrafaittayeb.dtp.common.protocol;

import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;

import java.util.List;

/** Reply to {@link ListJobs}. */
public record JobListReply(List<JobSnapshot> jobs) implements Message {
}
