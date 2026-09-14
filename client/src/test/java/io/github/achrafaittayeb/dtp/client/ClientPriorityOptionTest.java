package io.github.achrafaittayeb.dtp.client;

import io.github.achrafaittayeb.dtp.common.model.TaskPriority;
import io.github.achrafaittayeb.dtp.common.util.Args;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks the CLI's {@code --priority} contract: the flag name, the default when
 * omitted, and the validation the {@code submit} command applies. It exercises
 * the exact expression {@code ClientMain} uses to turn the option into a
 * {@link TaskPriority}.
 */
class ClientPriorityOptionTest {

    private static TaskPriority parsePriority(String... args) {
        return TaskPriority.fromString(Args.parse(args).get("priority", null));
    }

    @Test
    void acceptsValidPriorityValuesCaseInsensitively() {
        assertThat(parsePriority("--priority", "high")).isEqualTo(TaskPriority.HIGH);
        assertThat(parsePriority("--priority", "HIGH")).isEqualTo(TaskPriority.HIGH);
        assertThat(parsePriority("--priority", "Low")).isEqualTo(TaskPriority.LOW);
        assertThat(parsePriority("--priority", "normal")).isEqualTo(TaskPriority.NORMAL);
    }

    @Test
    void defaultsToNormalWhenPriorityOmitted() {
        assertThat(parsePriority()).isEqualTo(TaskPriority.NORMAL);
        assertThat(parsePriority("--max-attempts", "2")).isEqualTo(TaskPriority.NORMAL);
    }

    @Test
    void invalidPriorityProducesAClearError() {
        assertThatThrownBy(() -> parsePriority("--priority", "urgent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("urgent")
                .hasMessageContaining("high, normal, low");
    }
}
