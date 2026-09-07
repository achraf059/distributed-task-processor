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
                JsonNodeFactory.instance.objectNode().put("text", "a b c"), 3, 5_000L, 42L);
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
            assertThat(loaded.executionTimeoutMillis()).isEqualTo(5_000L);
            assertThat(loaded.deadlineMillis()).isEqualTo(50L + 5_000L);
            assertThat(loaded.payload().get("text").asText()).isEqualTo("a b c");
            assertThat(loaded.createdAtMillis()).isEqualTo(42L);
        }
    }

    @Test
    void saveIsAnUpsertReflectingLatestState() {
        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            Job job = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 10), 1, 5_000L, 1L);
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
    void migratesLegacyDatabaseWithoutDeadlineColumns() throws Exception {
        // A database written by the pre-deadline schema must remain loadable:
        // the migration adds execution_timeout/deadline with safe defaults.
        try (java.sql.Connection legacy =
                     java.sql.DriverManager.getConnection("jdbc:sqlite:" + dbPath());
             java.sql.Statement statement = legacy.createStatement()) {
            statement.execute("""
                    CREATE TABLE jobs (
                        id TEXT PRIMARY KEY, task_type TEXT NOT NULL, payload TEXT NOT NULL,
                        max_attempts INTEGER NOT NULL, state TEXT NOT NULL,
                        attempts INTEGER NOT NULL, current_attempt_id TEXT,
                        assigned_worker_id TEXT, next_eligible_time INTEGER NOT NULL,
                        result TEXT, error TEXT,
                        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
                    )""");
            statement.execute("""
                    INSERT INTO jobs VALUES ('legacy-job', 'SHA256', '{"text":"abc"}',
                        3, 'COMPLETED', 1, NULL, NULL, 0, 'old-result', NULL, 10, 20)""");
        }

        try (SqliteJobRepository migrated = new SqliteJobRepository(dbPath())) {
            Job loaded = migrated.loadAll().getFirst();
            assertThat(loaded.id()).isEqualTo("legacy-job");
            assertThat(loaded.state()).isEqualTo(JobState.COMPLETED);
            assertThat(loaded.result()).isEqualTo("old-result");
            assertThat(loaded.executionTimeoutMillis()).isEqualTo(600_000L); // migration default
            assertThat(loaded.deadlineMillis()).isZero();
            assertThat(loaded.idempotencyKey()).isNull(); // migrated column defaults to NULL

            // The migrated database is fully usable: a new keyed job saves and loads.
            Job keyed = Job.createQueued(TaskType.SHA256,
                    JsonNodeFactory.instance.objectNode().put("text", "x"), 3, 5_000L, 30L, "key-1");
            migrated.save(keyed);
            assertThat(migrated.loadAll())
                    .filteredOn(j -> "key-1".equals(j.idempotencyKey()))
                    .singleElement()
                    .satisfies(j -> assertThat(j.id()).isEqualTo(keyed.id()));
        }
    }

    @Test
    void persistsAndReloadsIdempotencyKey() {
        Job keyed = Job.createQueued(TaskType.WORD_COUNT,
                JsonNodeFactory.instance.objectNode().put("text", "a b"), 3, 5_000L, 42L, "submit-key-7");

        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            repository.save(keyed);
        }
        try (SqliteJobRepository reopened = new SqliteJobRepository(dbPath())) {
            Job loaded = reopened.loadAll().getFirst();
            assertThat(loaded.idempotencyKey()).isEqualTo("submit-key-7");
        }
    }

    @Test
    void reSavingAKeyedJobKeepsItsKeyImmutable() {
        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            Job keyed = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 10), 1, 5_000L, 1L, "k");
            repository.save(keyed);
            keyed.assignTo("w", 2L); // a later state change re-saves the row
            repository.save(keyed);

            assertThat(repository.loadAll().getFirst().idempotencyKey()).isEqualTo("k");
        }
    }

    @Test
    void loadsJobsInCreationOrder() {
        try (SqliteJobRepository repository = new SqliteJobRepository(dbPath())) {
            Job second = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 1), 1, 5_000L, 200L);
            Job first = Job.createQueued(TaskType.SLEEP,
                    JsonNodeFactory.instance.objectNode().put("durationMillis", 1), 1, 5_000L, 100L);
            repository.save(second);
            repository.save(first);

            assertThat(repository.loadAll())
                    .extracting(Job::id)
                    .containsExactly(first.id(), second.id());
        }
    }
}
