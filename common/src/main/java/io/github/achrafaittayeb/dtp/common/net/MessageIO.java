package io.github.achrafaittayeb.dtp.common.net;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.achrafaittayeb.dtp.common.protocol.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializes {@link Message}s onto framed streams and back.
 *
 * <p>Any malformed payload (invalid JSON, unknown {@code type}, wrong field types)
 * is converted into a {@link ProtocolException} so connection handlers have a
 * single failure mode for "this peer is speaking garbage".
 */
public final class MessageIO {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private MessageIO() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static byte[] encode(Message message) throws IOException {
        return MAPPER.writeValueAsBytes(message);
    }

    public static Message decode(byte[] payload) throws ProtocolException {
        try {
            return MAPPER.readValue(payload, Message.class);
        } catch (IOException badPayload) {
            throw new ProtocolException("Undecodable message: " + badPayload.getMessage(), badPayload);
        }
    }

    /** Writes one message as one frame. Callers must serialize concurrent writers per stream. */
    public static void send(OutputStream out, Message message) throws IOException {
        FrameCodec.writeFrame(out, encode(message));
    }

    /** Reads one message, or {@code null} if the peer closed the connection cleanly. */
    public static Message receive(InputStream in) throws IOException {
        byte[] frame = FrameCodec.readFrame(in);
        return frame == null ? null : decode(frame);
    }
}
