package io.github.achrafaittayeb.dtp.it;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.achrafaittayeb.dtp.client.CoordinatorClient;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.net.FrameCodec;
import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.protocol.ErrorReply;
import io.github.achrafaittayeb.dtp.common.protocol.Message;
import io.github.achrafaittayeb.dtp.coordinator.Coordinator;
import io.github.achrafaittayeb.dtp.coordinator.CoordinatorConfig;
import io.github.achrafaittayeb.dtp.worker.Worker;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario H: robustness against garbage on the wire. A misbehaving peer
 * loses its own connection, never the coordinator.
 */
class MalformedProtocolIT {

    @Test
    void coordinatorSurvivesGarbageOnBothPorts() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3);
             Worker worker = Testbed.startWorker(coordinator, "worker-1", 1);
             CoordinatorClient client = Testbed.connectClient(coordinator)) {

            // 1. Random bytes that are not even a frame.
            try (Socket socket = new Socket("localhost", coordinator.workerPort())) {
                socket.getOutputStream().write(new byte[]{(byte) 0x93, 1, 2, 3});
            }

            // 2. A frame whose declared length is absurd (2 GiB).
            try (Socket socket = new Socket("localhost", coordinator.workerPort())) {
                new DataOutputStream(socket.getOutputStream()).writeInt(Integer.MAX_VALUE);
            }

            // 3. A well-framed payload that is not valid JSON.
            try (Socket socket = new Socket("localhost", coordinator.workerPort())) {
                FrameCodec.writeFrame(socket.getOutputStream(),
                        "this is not json".getBytes(StandardCharsets.UTF_8));
            }

            // 4. Valid JSON with an unknown message type.
            try (Socket socket = new Socket("localhost", coordinator.workerPort())) {
                FrameCodec.writeFrame(socket.getOutputStream(),
                        "{\"type\":\"TOTALLY_UNKNOWN\"}".getBytes(StandardCharsets.UTF_8));
            }

            // 5. A registered-looking message sent on the client port.
            try (Socket socket = new Socket("localhost", coordinator.clientPort())) {
                FrameCodec.writeFrame(socket.getOutputStream(),
                        "not json either".getBytes(StandardCharsets.UTF_8));
                Message reply = MessageIO.receive(socket.getInputStream());
                assertThat(reply).isInstanceOf(ErrorReply.class);
            }

            // After all of that, the coordinator still schedules and completes work.
            String jobId = client.submit(TaskType.SHA256,
                    JsonNodeFactory.instance.objectNode().put("text", "still alive"), 0);
            assertThat(client.awaitTerminal(jobId, Testbed.POLL, Testbed.TERMINAL_TIMEOUT).state())
                    .isEqualTo(JobState.COMPLETED);
            assertThat(client.listWorkers()).hasSize(1);
        }
    }

    @Test
    void workerSendingWrongFirstMessageIsRejected() throws Exception {
        try (Coordinator coordinator = Testbed.startCoordinator(CoordinatorConfig.IN_MEMORY_DATABASE, 3)) {
            try (Socket socket = new Socket("localhost", coordinator.workerPort())) {
                // Heartbeat before registration violates the protocol.
                MessageIO.send(socket.getOutputStream(),
                        new io.github.achrafaittayeb.dtp.common.protocol.Heartbeat("sneaky"));
                Message reply = MessageIO.receive(socket.getInputStream());
                assertThat(reply).isInstanceOf(ErrorReply.class);
                assertThat(((ErrorReply) reply).message()).contains("WORKER_REGISTER");
                // The coordinator then closes the connection.
                assertThat(MessageIO.receive(socket.getInputStream())).isNull();
            }
        }
    }
}
