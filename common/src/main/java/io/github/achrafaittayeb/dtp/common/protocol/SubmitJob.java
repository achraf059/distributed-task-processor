package io.github.achrafaittayeb.dtp.common.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;

/**
 * Client request to enqueue a new job. {@code maxAttempts <= 0} means
 * "use the coordinator's configured default".
 *
 * <p>{@code idempotencyKey} is optional (nullable). When a client supplies a
 * stable key, the coordinator deduplicates: a second {@code SubmitJob} carrying
 * a key it has already seen returns the <em>original</em> job's id instead of
 * creating another logical job (see {@code CoordinatorCore}). A {@code null}
 * key preserves the historical behavior — every submission creates a fresh job.
 * The field is deliberately last with a delegating constructor so existing
 * call sites and older peers (which omit it, and it decodes to {@code null})
 * remain source- and wire-compatible.
 */
public record SubmitJob(TaskType taskType, JsonNode payload, int maxAttempts, String idempotencyKey)
        implements Message {

    /** Back-compatible constructor for a submission without an idempotency key. */
    public SubmitJob(TaskType taskType, JsonNode payload, int maxAttempts) {
        this(taskType, payload, maxAttempts, null);
    }
}
