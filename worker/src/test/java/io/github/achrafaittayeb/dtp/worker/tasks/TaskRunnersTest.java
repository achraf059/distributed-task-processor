package io.github.achrafaittayeb.dtp.worker.tasks;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskRunnersTest {

    private static ObjectNode payload() {
        return JsonNodeFactory.instance.objectNode();
    }

    @Test
    void everyTaskTypeHasARunner() {
        for (TaskType type : TaskType.values()) {
            assertThat(TaskRunners.forType(type)).as("runner for %s", type).isNotNull();
        }
    }

    @Test
    void wordCountCountsWhitespaceSeparatedWords() throws Exception {
        TaskRunner runner = TaskRunners.forType(TaskType.WORD_COUNT);
        assertThat(runner.run(payload().put("text", "one  two\tthree\nfour"), 1)).isEqualTo("4");
        assertThat(runner.run(payload().put("text", "   "), 1)).isEqualTo("0");
        assertThat(runner.run(payload().put("text", ""), 1)).isEqualTo("0");
    }

    @Test
    void sha256MatchesKnownVector() throws Exception {
        // SHA-256("abc") is a published NIST test vector.
        assertThat(TaskRunners.forType(TaskType.SHA256).run(payload().put("text", "abc"), 1))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void primeCountMatchesKnownValues() throws Exception {
        TaskRunner runner = TaskRunners.forType(TaskType.PRIME_COUNT);
        assertThat(runner.run(payload().put("limit", 10), 1)).isEqualTo("4");      // 2 3 5 7
        assertThat(runner.run(payload().put("limit", 100), 1)).isEqualTo("25");
        assertThat(runner.run(payload().put("limit", 1_000_000), 1)).isEqualTo("78498");
    }

    @Test
    void failTaskFailsUntilConfiguredAttempt() throws Exception {
        TaskRunner runner = TaskRunners.forType(TaskType.FAIL);
        ObjectNode input = payload().put("failUntilAttempt", 3);

        assertThatThrownBy(() -> runner.run(input, 1)).hasMessageContaining("attempt 1");
        assertThatThrownBy(() -> runner.run(input, 2)).hasMessageContaining("attempt 2");
        assertThat(runner.run(input, 3)).isEqualTo("succeeded on attempt 3");
    }

    @Test
    void sleepReportsDuration() throws Exception {
        long before = System.nanoTime();
        String result = TaskRunners.forType(TaskType.SLEEP).run(payload().put("durationMillis", 50), 1);
        long elapsedMillis = (System.nanoTime() - before) / 1_000_000;

        assertThat(result).isEqualTo("slept 50 ms");
        assertThat(elapsedMillis).isGreaterThanOrEqualTo(50);
    }
}
