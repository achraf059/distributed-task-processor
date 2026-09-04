package io.github.achrafaittayeb.dtp.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/** Entry point: {@code java -jar dtp-coordinator.jar [--worker-port 7070 ...]}. */
public final class CoordinatorMain {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorMain.class);

    private CoordinatorMain() {
    }

    public static void main(String[] args) throws Exception {
        CoordinatorConfig config;
        try {
            config = CoordinatorConfig.fromArgs(args);
        } catch (IllegalArgumentException badArgs) {
            System.err.println("Error: " + badArgs.getMessage());
            System.err.println("""
                    Usage: coordinator [options]
                      --worker-port <port>              default 7070
                      --client-port <port>              default 7071
                      --heartbeat-timeout-millis <ms>   default 6000
                      --sweep-interval-millis <ms>      default 500
                      --max-attempts <n>                default 3
                      --retry-base-delay-millis <ms>    default 1000
                      --retry-max-delay-millis <ms>     default 30000
                      --database <path|:memory:>        default data/coordinator.db""");
            System.exit(2);
            return;
        }

        Coordinator coordinator = new Coordinator(config);
        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            coordinator.close();
            shutdown.countDown();
        }, "shutdown-hook"));

        coordinator.start();
        shutdown.await();
    }
}
