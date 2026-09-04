package io.github.achrafaittayeb.dtp.common.task;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskPayloadsTest {

    private static ObjectNode payload() {
        return JsonNodeFactory.instance.objectNode();
    }

    @Test
    void acceptsValidPayloads() {
        assertThatCode(() -> {
            TaskPayloads.validate(TaskType.SLEEP, payload().put("durationMillis", 5000));
            TaskPayloads.validate(TaskType.WORD_COUNT, payload().put("text", "one two"));
            TaskPayloads.validate(TaskType.SHA256, payload().put("text", "abc"));
            TaskPayloads.validate(TaskType.PRIME_COUNT, payload().put("limit", 100_000));
            TaskPayloads.validate(TaskType.FAIL, payload().put("failUntilAttempt", 3));
        }).doesNotThrowAnyException();
    }

    @Test
    void rejectsNonObjectPayload() {
        assertThatThrownBy(() -> TaskPayloads.validate(TaskType.SLEEP, null))
                .isInstanceOf(InvalidPayloadException.class);
        assertThatThrownBy(() -> TaskPayloads.validate(
                TaskType.SLEEP, JsonNodeFactory.instance.textNode("nope")))
                .isInstanceOf(InvalidPayloadException.class);
    }

    @Test
    void rejectsMissingOrMistypedFields() {
        assertThatThrownBy(() -> TaskPayloads.validate(TaskType.SLEEP, payload()))
                .isInstanceOf(InvalidPayloadException.class)
                .hasMessageContaining("durationMillis");
        assertThatThrownBy(() -> TaskPayloads.validate(
                TaskType.SLEEP, payload().put("durationMillis", "soon")))
                .isInstanceOf(InvalidPayloadException.class);
        assertThatThrownBy(() -> TaskPayloads.validate(TaskType.SHA256, payload().put("text", 5)))
                .isInstanceOf(InvalidPayloadException.class);
    }

    @Test
    void enforcesResourceBounds() {
        assertThatThrownBy(() -> TaskPayloads.validate(
                TaskType.SLEEP, payload().put("durationMillis", TaskPayloads.MAX_SLEEP_MILLIS + 1)))
                .isInstanceOf(InvalidPayloadException.class);
        assertThatThrownBy(() -> TaskPayloads.validate(
                TaskType.PRIME_COUNT, payload().put("limit", TaskPayloads.MAX_PRIME_LIMIT + 1)))
                .isInstanceOf(InvalidPayloadException.class);
        assertThatThrownBy(() -> TaskPayloads.validate(
                TaskType.SHA256, payload().put("text", "x".repeat(TaskPayloads.MAX_TEXT_CHARS + 1))))
                .isInstanceOf(InvalidPayloadException.class);
    }
}
