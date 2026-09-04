package io.github.achrafaittayeb.dtp.common.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;

/**
 * Assigns one attempt of a job to a worker.
 *
 * <p>{@code attemptId} is the lease for this specific execution. The worker must
 * echo it back in {@link TaskResult}; the coordinator only accepts a result whose
 * attemptId matches the job's current lease, which is what makes results from
 * dead-and-resurrected workers harmless.
 */
public record TaskAssign(
        String jobId,
        String attemptId,
        int attemptNumber,
        TaskType taskType,
        JsonNode payload) implements Message {
}
