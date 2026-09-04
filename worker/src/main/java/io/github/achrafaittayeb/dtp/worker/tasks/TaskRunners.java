package io.github.achrafaittayeb.dtp.worker.tasks;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.task.TaskPayloads;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.Map;

/** The worker's catalog of built-in task implementations. */
public final class TaskRunners {

    private static final Map<TaskType, TaskRunner> RUNNERS = Map.of(
            TaskType.SLEEP, TaskRunners::sleep,
            TaskType.WORD_COUNT, TaskRunners::wordCount,
            TaskType.SHA256, TaskRunners::sha256,
            TaskType.PRIME_COUNT, TaskRunners::primeCount,
            TaskType.FAIL, TaskRunners::fail);

    private TaskRunners() {
    }

    public static TaskRunner forType(TaskType type) {
        return RUNNERS.get(type);
    }

    private static String sleep(JsonNode payload, int attempt) throws InterruptedException {
        long millis = TaskPayloads.longField(payload, "durationMillis");
        Thread.sleep(millis);
        return "slept " + millis + " ms";
    }

    private static String wordCount(JsonNode payload, int attempt) {
        String text = TaskPayloads.textField(payload, "text").strip();
        long words = text.isEmpty() ? 0 : text.split("\\s+").length;
        return String.valueOf(words);
    }

    private static String sha256(JsonNode payload, int attempt) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(TaskPayloads.textField(payload, "text").getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }

    /** Sieve of Eratosthenes; the payload bound keeps the BitSet a few MiB at most. */
    private static String primeCount(JsonNode payload, int attempt) {
        int limit = (int) TaskPayloads.longField(payload, "limit");
        BitSet composite = new BitSet(limit);
        int count = 0;
        for (int candidate = 2; candidate < limit; candidate++) {
            if (!composite.get(candidate)) {
                count++;
                for (long multiple = (long) candidate * candidate; multiple < limit; multiple += candidate) {
                    composite.set((int) multiple);
                }
            }
        }
        return String.valueOf(count);
    }

    /** Deliberately fails early attempts so retry behavior can be demonstrated end to end. */
    private static String fail(JsonNode payload, int attempt) {
        long failUntilAttempt = TaskPayloads.longField(payload, "failUntilAttempt");
        if (attempt < failUntilAttempt) {
            throw new IllegalStateException(
                    "deliberate failure on attempt " + attempt + " (failing until " + failUntilAttempt + ")");
        }
        return "succeeded on attempt " + attempt;
    }
}
