package io.github.achrafaittayeb.dtp.coordinator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorConfigTest {

    @Test
    void defaultsMaxActiveJobs() {
        CoordinatorConfig config = CoordinatorConfig.fromArgs(new String[0]);
        assertThat(config.maxActiveJobs()).isEqualTo(CoordinatorConfig.DEFAULT_MAX_ACTIVE_JOBS);
    }

    @Test
    void parsesMaxActiveJobsFlag() {
        CoordinatorConfig config = CoordinatorConfig.fromArgs(
                new String[]{"--max-active-jobs", "250"});
        assertThat(config.maxActiveJobs()).isEqualTo(250);
    }

    @Test
    void rejectsZeroMaxActiveJobs() {
        assertThatThrownBy(() -> CoordinatorConfig.fromArgs(new String[]{"--max-active-jobs", "0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-active-jobs");
    }

    @Test
    void rejectsNegativeMaxActiveJobs() {
        assertThatThrownBy(() -> CoordinatorConfig.fromArgs(new String[]{"--max-active-jobs", "-1"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-active-jobs");
    }
}
