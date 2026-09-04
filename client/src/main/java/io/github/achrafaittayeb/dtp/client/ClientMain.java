package io.github.achrafaittayeb.dtp.client;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.model.WorkerSnapshot;
import io.github.achrafaittayeb.dtp.common.util.Args;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * Command-line interface.
 *
 * <pre>
 * client submit sleep --milliseconds 30000
 * client submit word-count --text "one two three"
 * client submit sha256 --text "abc"
 * client submit prime-count --limit 1000000
 * client submit fail --fail-until-attempt 3
 * client status &lt;job-id&gt;
 * client wait &lt;job-id&gt;
 * client list
 * client workers
 * </pre>
 *
 * Global options: {@code --host} (default localhost), {@code --port} (default 7071),
 * and {@code --max-attempts} on submit.
 */
public final class ClientMain {

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private ClientMain() {
    }

    public static void main(String[] rawArgs) {
        try {
            run(rawArgs);
        } catch (UsageException usage) {
            System.err.println("Error: " + usage.getMessage());
            printUsage();
            System.exit(2);
        } catch (Exception failure) {
            System.err.println("Error: " + failure.getMessage());
            System.exit(1);
        }
    }

    private static void run(String[] rawArgs) throws Exception {
        if (rawArgs.length == 0) {
            throw new UsageException("Missing command");
        }
        String command = rawArgs[0];
        List<String> positionals = Arrays.stream(rawArgs).skip(1)
                .takeWhile(arg -> !arg.startsWith("--")).toList();
        Args options = Args.parse(Arrays.copyOfRange(
                rawArgs, 1 + positionals.size(), rawArgs.length));
        String host = options.get("host", "localhost");
        int port = options.getInt("port", 7071);

        try (CoordinatorClient client = new CoordinatorClient(host, port)) {
            switch (command) {
                case "submit" -> submit(client, positionals, options);
                case "status" -> printJob(client.status(requireJobId(positionals)));
                case "wait" -> {
                    String jobId = requireJobId(positionals);
                    System.out.println("Waiting for job " + jobId + " ...");
                    printJob(client.awaitTerminal(jobId, Duration.ofMillis(500),
                            Duration.ofMillis(options.getLong("timeout-millis", 15 * 60 * 1000))));
                }
                case "list" -> listJobs(client);
                case "workers" -> listWorkers(client);
                default -> throw new UsageException("Unknown command: " + command);
            }
        }
    }

    private static void submit(CoordinatorClient client, List<String> positionals, Args options)
            throws Exception {
        if (positionals.isEmpty()) {
            throw new UsageException("submit requires a task type");
        }
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        TaskType taskType = switch (positionals.getFirst()) {
            case "sleep" -> {
                payload.put("durationMillis", requiredLong(options, "milliseconds"));
                yield TaskType.SLEEP;
            }
            case "word-count" -> {
                payload.put("text", requiredText(options, "text"));
                yield TaskType.WORD_COUNT;
            }
            case "sha256" -> {
                payload.put("text", requiredText(options, "text"));
                yield TaskType.SHA256;
            }
            case "prime-count" -> {
                payload.put("limit", requiredLong(options, "limit"));
                yield TaskType.PRIME_COUNT;
            }
            case "fail" -> {
                payload.put("failUntilAttempt", requiredLong(options, "fail-until-attempt"));
                yield TaskType.FAIL;
            }
            default -> throw new UsageException("Unknown task type: " + positionals.getFirst());
        };

        String jobId = client.submit(taskType, payload, options.getInt("max-attempts", 0));
        System.out.println("Job submitted");
        System.out.println("  ID:    " + jobId);
        System.out.println("  Type:  " + taskType);
        System.out.println("  State: QUEUED");
    }

    private static void printJob(JobSnapshot job) {
        System.out.println("Job " + job.jobId());
        System.out.println("  Type:     " + job.taskType());
        System.out.println("  State:    " + job.state());
        System.out.println("  Attempt:  " + job.attempts() + "/" + job.maxAttempts());
        if (job.workerId() != null) {
            System.out.println("  Worker:   " + job.workerId());
        }
        if (job.result() != null) {
            System.out.println("  Result:   " + job.result());
        }
        if (job.error() != null) {
            System.out.println("  Error:    " + job.error());
        }
        System.out.println("  Updated:  " + TIME.format(Instant.ofEpochMilli(job.updatedAtMillis())));
    }

    private static void listJobs(CoordinatorClient client) throws Exception {
        List<JobSnapshot> jobs = client.listJobs();
        if (jobs.isEmpty()) {
            System.out.println("No jobs");
            return;
        }
        System.out.printf("%-36s  %-11s  %-10s  %-7s  %-10s%n",
                "JOB ID", "TYPE", "STATE", "ATTEMPT", "WORKER");
        for (JobSnapshot job : jobs) {
            System.out.printf("%-36s  %-11s  %-10s  %-7s  %-10s%n",
                    job.jobId(), job.taskType(), job.state(),
                    job.attempts() + "/" + job.maxAttempts(),
                    job.workerId() == null ? "-" : job.workerId());
        }
    }

    private static void listWorkers(CoordinatorClient client) throws Exception {
        List<WorkerSnapshot> workers = client.listWorkers();
        if (workers.isEmpty()) {
            System.out.println("No workers registered");
            return;
        }
        System.out.printf("%-12s  %-8s  %-6s  %-14s%n", "WORKER", "CAPACITY", "ACTIVE", "LAST HEARTBEAT");
        for (WorkerSnapshot worker : workers) {
            System.out.printf("%-12s  %-8d  %-6d  %-14s%n",
                    worker.workerId(), worker.capacity(), worker.activeTasks(),
                    TIME.format(Instant.ofEpochMilli(worker.lastHeartbeatMillis())));
        }
    }

    private static String requireJobId(List<String> positionals) throws UsageException {
        if (positionals.isEmpty()) {
            throw new UsageException("Missing job id");
        }
        return positionals.getFirst();
    }

    private static long requiredLong(Args options, String key) throws UsageException {
        long value = options.getLong(key, Long.MIN_VALUE);
        if (value == Long.MIN_VALUE) {
            throw new UsageException("Missing required option --" + key);
        }
        return value;
    }

    private static String requiredText(Args options, String key) throws UsageException {
        String value = options.get(key, null);
        if (value == null) {
            throw new UsageException("Missing required option --" + key);
        }
        return value;
    }

    private static void printUsage() {
        System.err.println("""
                Usage: client <command> [options]
                  submit sleep --milliseconds <ms>
                  submit word-count --text "<text>"
                  submit sha256 --text "<text>"
                  submit prime-count --limit <n>
                  submit fail --fail-until-attempt <n>
                  status <job-id>
                  wait <job-id> [--timeout-millis <ms>]
                  list
                  workers
                Global options: --host <host> (default localhost), --port <port> (default 7071)
                Submit options: --max-attempts <n> (default: coordinator setting)""");
    }

    private static final class UsageException extends Exception {
        UsageException(String message) {
            super(message);
        }
    }
}
