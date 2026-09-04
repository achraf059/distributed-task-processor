package io.github.achrafaittayeb.dtp.common.protocol;

import io.github.achrafaittayeb.dtp.common.model.WorkerSnapshot;

import java.util.List;

/** Reply to {@link ListWorkers}. */
public record WorkerListReply(List<WorkerSnapshot> workers) implements Message {
}
