package io.github.achrafaittayeb.dtp.common.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskPriorityTest {

    @Test
    void baseLevelsAreHighestForHigh() {
        assertThat(TaskPriority.HIGH.baseLevel()).isEqualTo(2);
        assertThat(TaskPriority.NORMAL.baseLevel()).isEqualTo(1);
        assertThat(TaskPriority.LOW.baseLevel()).isEqualTo(0);
        assertThat(TaskPriority.MAX_LEVEL).isEqualTo(2);
        assertThat(TaskPriority.HIGH.baseLevel()).isEqualTo(TaskPriority.MAX_LEVEL);
    }

    @Test
    void normalizeMapsNullToNormal() {
        assertThat(TaskPriority.normalize(null)).isEqualTo(TaskPriority.NORMAL);
        assertThat(TaskPriority.normalize(TaskPriority.HIGH)).isEqualTo(TaskPriority.HIGH);
        assertThat(TaskPriority.normalize(TaskPriority.LOW)).isEqualTo(TaskPriority.LOW);
    }

    @Test
    void fromStringIsCaseInsensitive() {
        assertThat(TaskPriority.fromString("high")).isEqualTo(TaskPriority.HIGH);
        assertThat(TaskPriority.fromString("HIGH")).isEqualTo(TaskPriority.HIGH);
        assertThat(TaskPriority.fromString("Normal")).isEqualTo(TaskPriority.NORMAL);
        assertThat(TaskPriority.fromString("  low  ")).isEqualTo(TaskPriority.LOW);
    }

    @Test
    void fromStringDefaultsToNormalWhenNullOrBlank() {
        assertThat(TaskPriority.fromString(null)).isEqualTo(TaskPriority.NORMAL);
        assertThat(TaskPriority.fromString("")).isEqualTo(TaskPriority.NORMAL);
        assertThat(TaskPriority.fromString("   ")).isEqualTo(TaskPriority.NORMAL);
    }

    @Test
    void fromStringRejectsUnknownValueWithGuidance() {
        assertThatThrownBy(() -> TaskPriority.fromString("urgent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("urgent")
                .hasMessageContaining("high, normal, low");
    }
}
