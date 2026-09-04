package io.github.achrafaittayeb.dtp.coordinator.job;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SqliteJobRepositoryTest {

    @TempDir
    Path tempDir;

    private String dbPath() {
        return tempDir.resolve("jobs.db").toString();
    }

    @Test
    void savedJobsSurviveRepositoryReopen() {
        Job job = Job.createQueued(TaskType.WORD_COUNT,
                JsonNodeFactory.instance.objectNode().put("text", "a b c"), 3, 42L);
        job.assignTo("worker-9", 50L);

        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            repository.save(job);
        }

        try (SqliteJobRepository reopened = new SqliteJobRepository(dbPath())) {
            assertThat(reopened.loadAll()).hasSize(1);
            Job loaded = reopened.loadAll().getFirst();
            assertThat(loaded.id()).isEqualTo(job.id());
            assertThat(loaded.taskType()).isEqualTo(TaskType.WORD_COUNT);
            assertThat(loaded.state()).isEqualTo(JobState.RUNNING);
            assertThat(loaded.attempts()).isEqualTo(1);
            assertThat(loaded.assignedWorkerId()).isEqualTo("worker-9");
            assertThat(loaded.currentAttemptId()).isEqualTo(job.currentAttemptId());
            assertThat(loaded.payload().get("text").asText()).isEqualTo("a b c");
            assertThat(loaded.createdAtMillis()).isEqualTo(42L);
        }
    }

    @Test
    void saveIsAnUpsertReflectingLatestState() {
        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            Job job = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 10), 1, 1L);
            repository.save(job);
            job.assignTo("w", 2L);
            repository.save(job);
            job.complete("slept", 3L);
            repository.save(job);

            Job loaded = repository.loadAll().getFirst();
            assertThat(repository.loadAll()).hasSize(1);
            assertThat(loaded.state()).isEqualTo(JobState.COMPLETED);
            assertThat(loaded.result()).isEqualTo("slept");
            assertThat(loaded.assignedWorkerId()).isNull();
        }
    }

    @Test
    void loadsJobsInCreationOrder() {
        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            Job second = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 1), 1, 200L);
            Job first = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 1), 1, 100L);
            repository.save(second);
            repository.save(first);

            assertThat(repository.loadAll())
                    .extracting(Job::id)
                    .containsExactly(first.id(), second.id());
        }
    }
}
