package io.github.achrafaittayeb.dtp.common.protocol;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.achrafaittayeb.dtp.common.model.JobSnapshot;
import io.github.achrafaittayeb.dtp.common.model.JobState;
import io.github.achrafaittayeb.dtp.common.model.TaskType;
import io.github.achrafaittayeb.dtp.common.net.MessageIO;
import io.github.achrafaittayeb.dtp.common.net.ProtocolException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageSerializationTest {

    private static Message roundTrip(Message message) throws IOException {
        return MessageIO.decode(MessageIO.encode(message));
    }

    @Test
    void roundTripsWorkerMessages() throws IOException {
        assertThat(roundTrip(new WorkerRegister("worker-1", 4)))
                .isEqualTo(new WorkerRegister("worker-1", 4));
        assertThat(roundTrip(new Heartbeat("worker-1")))
                .isEqualTo(new Heartbeat("worker-1"));
        assertThat(roundTrip(TaskResult.success("worker-1", "job-1", "att-1", "42")))
                .isEqualTo(TaskResult.success("worker-1", "job-1", "att-1", "42"));
    }

    @Test
    void roundTripsTaskAssignWithPayload() throws IOException {
        ObjectNode payload = JsonNodeFactory.instance.objectNode().put("durationMillis", 5000);
        TaskAssign assign = new TaskAssign("job-1", "att-1", 2, TaskType.SLEEP, payload);

        TaskAssign decoded = (TaskAssign) roundTrip(assign);
        assertThat(decoded).isEqualTo(assign);
        assertThat(decoded.payload().get("durationMillis").asLong()).isEqualTo(5000);
    }

    @Test
    void roundTripsClientMessages() throws IOException {
        JobSnapshot snapshot = new JobSnapshot("job-1", TaskType.SHA256, JobState.COMPLETED,
                1, 3, "worker-2", "abc123", null, 100L, 200L);
        assertThat(roundTrip(new JobStatusReply(snapshot)))
                .isEqualTo(new JobStatusReply(snapshot));
        assertThat(roundTrip(new JobListReply(List.of(snapshot))))
                .isEqualTo(new JobListReply(List.of(snapshot)));
    }

    @Test
    void encodedMessagesCarryTypeDiscriminator() throws IOException {
        String json = new String(MessageIO.encode(new Heartbeat("w")), StandardCharsets.UTF_8);
        assertThat(json).contains("\"type\":\"HEARTBEAT\"");
    }

    @Test
    void rejectsUnknownMessageType() {
        byte[] bogus = "{\"type\":\"NOT_A_REAL_TYPE\"}".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> MessageIO.decode(bogus)).isInstanceOf(ProtocolException.class);
    }

    @Test
    void rejectsNonJsonPayload() {
        byte[] garbage = {0x00, 0x01, 0x7F, (byte) 0xFF};
        assertThatThrownBy(() -> MessageIO.decode(garbage)).isInstanceOf(ProtocolException.class);
    }
}
