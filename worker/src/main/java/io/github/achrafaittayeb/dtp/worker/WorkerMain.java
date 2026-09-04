package io.github.achrafaittayeb.dtp.worker;

import java.util.concurrent.CountDownLatch;

/** Entry point: {@code java -jar dtp-worker.jar --id worker-1 [--capacity 4 ...]}. */
public final class WorkerMain {

    private WorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        WorkerConfig config;
        try {
            config = WorkerConfig.fromArgs(args);
        } catch (IllegalArgumentException badArgs) {
            System.err.println("Error: " + badArgs.getMessage());
            System.err.println("""
                    Usage: worker [options]
                      --coordinator-host <host>          default localhost
                      --coordinator-port <port>          default 7070
                      --id <worker-id>                   default worker-<random>
                      --capacity <n>                     default 4
                      --heartbeat-interval-millis <ms>   default 2000
                      --reconnect-delay-millis <ms>      default 2000""");
            System.exit(2);
            return;
        }

        Worker worker = new Worker(config);
        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            shutdown.countDown();
        }, "shutdown-hook"));

        worker.start();
        shutdown.await();
    }
}
