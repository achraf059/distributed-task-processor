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

    @Test
    void defaultsAgingStepMillis() {
        CoordinatorConfig config = CoordinatorConfig.fromArgs(new String[0]);
        assertThat(config.agingStepMillis()).isEqualTo(CoordinatorConfig.DEFAULT_AGING_STEP_MILLIS);
        assertThat(config.agingStepMillis()).isEqualTo(60_000L);
    }

    @Test
    void parsesAgingStepMillisFlag() {
        CoordinatorConfig config = CoordinatorConfig.fromArgs(
                new String[]{"--aging-step-millis", "1000"});
        assertThat(config.agingStepMillis()).isEqualTo(1000L);
    }

    @Test
    void rejectsZeroAgingStepMillis() {
        assertThatThrownBy(() -> CoordinatorConfig.fromArgs(new String[]{"--aging-step-millis", "0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aging-step-millis");
    }

    @Test
    void rejectsNegativeAgingStepMillis() {
        assertThatThrownBy(() -> CoordinatorConfig.fromArgs(new String[]{"--aging-step-millis", "-5"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aging-step-millis");
    }
}
