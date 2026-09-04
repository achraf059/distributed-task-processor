package io.github.achrafaittayeb.dtp.common.task;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;

/**
 * Validation rules for task payloads, shared by the coordinator (reject bad
 * submissions early) and the worker (never trust the wire). Bounds exist so a
 * single job cannot accidentally monopolize a worker.
 */
public final class TaskPayloads {

    public static final long MAX_SLEEP_MILLIS = 10 * 60 * 1000L; // 10 minutes
    /** Bounded so a worst-case UTF-8 encoding still fits comfortably in one 1 MiB frame. */
    public static final int MAX_TEXT_CHARS = 200_000;
    public static final long MAX_PRIME_LIMIT = 50_000_000L;

    private TaskPayloads() {
    }

    /** Validates {@code payload} for {@code type}; throws with a client-friendly message otherwise. */
    public static void validate(TaskType type, JsonNode payload) throws InvalidPayloadException {
        if (type == null) {
            throw new InvalidPayloadException("Missing task type");
        }
        if (payload == null || !payload.isObject()) {
            throw new InvalidPayloadException("Payload must be a JSON object");
        }
        switch (type) {
            case SLEEP -> requireLongInRange(payload, "durationMillis", 0, MAX_SLEEP_MILLIS);
            case WORD_COUNT, SHA256 -> requireText(payload, "text");
            case PRIME_COUNT -> requireLongInRange(payload, "limit", 2, MAX_PRIME_LIMIT);
            case FAIL -> requireLongInRange(payload, "failUntilAttempt", 0, 1000);
        }
    }

    public static long longField(JsonNode payload, String field) {
        return payload.get(field).asLong();
    }

    public static String textField(JsonNode payload, String field) {
        return payload.get(field).asText();
    }

    private static void requireLongInRange(JsonNode payload, String field, long min, long max)
            throws InvalidPayloadException {
        JsonNode value = payload.get(field);
        if (value == null || !value.canConvertToLong()) {
            throw new InvalidPayloadException("Field '" + field + "' must be an integer");
        }
        long number = value.asLong();
        if (number < min || number > max) {
            throw new InvalidPayloadException(
                    "Field '" + field + "' must be in [" + min + ", " + max + "], got " + number);
        }
    }

    private static void requireText(JsonNode payload, String field) throws InvalidPayloadException {
        JsonNode value = payload.get(field);
        if (value == null || !value.isTextual()) {
            throw new InvalidPayloadException("Field '" + field + "' must be a string");
        }
        if (value.asText().length() > MAX_TEXT_CHARS) {
            throw new InvalidPayloadException(
                    "Field '" + field + "' exceeds " + MAX_TEXT_CHARS + " characters");
        }
    }
}
