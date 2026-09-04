package io.github.achrafaittayeb.dtp.worker.tasks;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Executes one task attempt. Implementations must be deterministic,
 * resource-bounded, and side-effect free — the payload has already been
 * validated by {@link io.github.achrafaittayeb.dtp.common.task.TaskPayloads}.
 */
public interface TaskRunner {

    /**
     * @param payload validated task input
     * @param attemptNumber 1-based attempt count for this job (lets test tasks
     *        behave differently across retries)
     * @return human-readable result reported back to the client
     * @throws Exception any failure; the message becomes the attempt error
     */
    String run(JsonNode payload, int attemptNumber) throws Exception;
}
