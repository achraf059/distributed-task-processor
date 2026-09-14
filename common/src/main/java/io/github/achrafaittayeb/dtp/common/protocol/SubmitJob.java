package io.github.achrafaittayeb.dtp.common.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
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
 *
 * <p>{@code priority} is also optional (nullable). A {@code null} priority
 * decodes and executes as {@link TaskPriority#NORMAL}, so old clients and older
 * peers that omit the field are unchanged. Both optional fields are deliberately
 * trailing with delegating constructors so existing call sites stay source-
 * compatible and older wire peers (which omit them, decoding to {@code null})
 * stay wire-compatible.
 */
public record SubmitJob(TaskType taskType, JsonNode payload, int maxAttempts,
                        String idempotencyKey, TaskPriority priority)
        implements Message {

    /** Back-compatible constructor for a submission without a key or priority. */
    public SubmitJob(TaskType taskType, JsonNode payload, int maxAttempts) {
        this(taskType, payload, maxAttempts, null, null);
    }

    /** Back-compatible constructor for a keyed submission without an explicit priority. */
    public SubmitJob(TaskType taskType, JsonNode payload, int maxAttempts, String idempotencyKey) {
        this(taskType, payload, maxAttempts, idempotencyKey, null);
    }
}
