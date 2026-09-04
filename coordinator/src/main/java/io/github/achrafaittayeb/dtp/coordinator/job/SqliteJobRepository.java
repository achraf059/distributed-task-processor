package io.github.achrafaittayeb.dtp.coordinator.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLite-backed job storage. One row per job, fully rewritten on every state
 * change — job records are tiny and SQLite upserts are cheap, so write-through
 * keeps recovery trivial: the table always holds the latest committed state.
 *
 * <p>Called only from the coordinator core thread, so a single JDBC connection
 * needs no pooling or locking.
 */
public final class SqliteJobRepository implements JobRepository {

    private static final Logger log = LoggerFactory.getLogger(SqliteJobRepository.class);

    private static final String UPSERT = """
            INSERT INTO jobs (id, task_type, payload, max_attempts, state, attempts,
                              current_attempt_id, assigned_worker_id, next_eligible_time,
                              result, error, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                state = excluded.state,
                attempts = excluded.attempts,
                current_attempt_id = excluded.current_attempt_id,
                assigned_worker_id = excluded.assigned_worker_id,
                next_eligible_time = excluded.next_eligible_time,
                result = excluded.result,
                error = excluded.error,
                updated_at = excluded.updated_at
            """;

    private final Connection connection;

    public SqliteJobRepository(String databasePath) {
        try {
            Path parent = Path.of(databasePath).toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
            createSchema();
            log.info("Job database opened at {}", databasePath);
        } catch (SQLException | IOException e) {
            throw new IllegalStateException("Cannot open job database at " + databasePath, e);
        }
    }

    private void createSchema() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        id TEXT PRIMARY KEY,
                        task_type TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        max_attempts INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        attempts INTEGER NOT NULL,
                        current_attempt_id TEXT,
                        assigned_worker_id TEXT,
                        next_eligible_time INTEGER NOT NULL,
                        result TEXT,
                        error TEXT,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL
                    )""");
        }
    }

    @Override
    public void save(Job job) {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT)) {
            statement.setString(1, job.id());
            statement.setString(2, job.taskType().name());
            statement.setString(3, MessageIO.mapper().writeValueAsString(job.payload()));
            statement.setInt(4, job.maxAttempts());
            statement.setString(5, job.state().name());
            statement.setInt(6, job.attempts());
            statement.setString(7, job.currentAttemptId());
            statement.setString(8, job.assignedWorkerId());
            statement.setLong(9, job.nextEligibleTimeMillis());
            statement.setString(10, job.result());
            statement.setString(11, job.error());
            statement.setLong(12, job.createdAtMillis());
            statement.setLong(13, job.updatedAtMillis());
            statement.executeUpdate();
        } catch (SQLException | JsonProcessingException e) {
            // Losing durability silently would break recovery guarantees; fail loudly.
            throw new IllegalStateException("Failed to persist job " + job.id(), e);
        }
    }

    @Override
    public List<Job> loadAll() {
        List<Job> loaded = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT * FROM jobs ORDER BY created_at")) {
            while (rows.next()) {
                loaded.add(rowToJob(rows));
            }
            return loaded;
        } catch (SQLException | IOException e) {
            throw new IllegalStateException("Failed to load persisted jobs", e);
        }
    }

    private static Job rowToJob(ResultSet row) throws SQLException, IOException {
        JsonNode payload = MessageIO.mapper().readTree(row.getString("payload"));
        return new Job(
                row.getString("id"),
                TaskType.valueOf(row.getString("task_type")),
                payload,
                row.getInt("max_attempts"),
                JobState.valueOf(row.getString("state")),
                row.getInt("attempts"),
                row.getString("current_attempt_id"),
                row.getString("assigned_worker_id"),
                row.getLong("next_eligible_time"),
                row.getString("result"),
                row.getString("error"),
                row.getLong("created_at"),
                row.getLong("updated_at"));
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            log.warn("Error closing job database", e);
        }
    }
}
