package io.github.achrafaittayeb.dtp.coordinator.job;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Non-durable repository for unit tests and ephemeral runs. */
public final class InMemoryJobRepository implements JobRepository {

    private final Map<String, Job> rows = new LinkedHashMap<>();

    @Override
    public void save(Job job) {
        rows.put(job.id(), job);
    }

    @Override
    public List<Job> loadAll() {
        List<Job> all = new ArrayList<>(rows.values());
        all.sort(Comparator.comparingLong(Job::createdAtMillis));
        return all;
    }

    @Override
    public void close() {
    }
}
