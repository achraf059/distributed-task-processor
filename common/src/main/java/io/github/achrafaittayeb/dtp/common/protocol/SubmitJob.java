package io.github.achrafaittayeb.dtp.common.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;

/**
 * Client request to enqueue a new job. {@code maxAttempts <= 0} means
 * "use the coordinator's configured default".
 */
public record SubmitJob(TaskType taskType, JsonNode payload, int maxAttempts) implements Message {
}
