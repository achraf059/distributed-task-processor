package io.github.achrafaittayeb.dtp.common.net;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameCodecTest {

    @Test
    void roundTripsSingleFrame() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        FrameCodec.writeFrame(out, payload);

        byte[] read = FrameCodec.readFrame(new ByteArrayInputStream(out.toByteArray()));
        assertThat(read).isEqualTo(payload);
    }

    @Test
    void readsMultipleFramesFromOneStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeFrame(out, "first".getBytes(StandardCharsets.UTF_8));
        FrameCodec.writeFrame(out, "second".getBytes(StandardCharsets.UTF_8));

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThat(FrameCodec.readFrame(in)).isEqualTo("first".getBytes(StandardCharsets.UTF_8));
        assertThat(FrameCodec.readFrame(in)).isEqualTo("second".getBytes(StandardCharsets.UTF_8));
        assertThat(FrameCodec.readFrame(in)).isNull(); // clean EOF at frame boundary
    }

    @Test
    void assemblesFrameFromPartialReads() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] payload = "partial-read-payload".getBytes(StandardCharsets.UTF_8);
        FrameCodec.writeFrame(out, payload);
        byte[] wire = out.toByteArray();

        // Stream that returns at most one byte per read(), like a slow socket.
        InputStream oneByteAtATime = new InputStream() {
            private int pos;

            @Override
            public int read() {
                return pos < wire.length ? wire[pos++] & 0xFF : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                int single = read();
                if (single == -1) {
                    return -1;
                }
                b[off] = (byte) single;
                return 1;
            }
        };

        assertThat(FrameCodec.readFrame(oneByteAtATime)).isEqualTo(payload);
    }

    @Test
    void rejectsNegativeLength() {
        byte[] wire = ByteBuffer.allocate(4).putInt(-5).array();
        assertThatThrownBy(() -> FrameCodec.readFrame(new ByteArrayInputStream(wire)))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("-5");
    }

    @Test
    void rejectsOversizedLength() {
        byte[] wire = ByteBuffer.allocate(4).putInt(FrameCodec.MAX_FRAME_BYTES + 1).array();
        assertThatThrownBy(() -> FrameCodec.readFrame(new ByteArrayInputStream(wire)))
                .isInstanceOf(ProtocolException.class);
    }

    @Test
    void truncatedFrameThrowsEof() {
        ByteBuffer wire = ByteBuffer.allocate(6).putInt(100).put((byte) 1).put((byte) 2);
        assertThatThrownBy(() -> FrameCodec.readFrame(new ByteArrayInputStream(wire.array())))
                .isInstanceOf(EOFException.class);
    }

    @Test
    void refusesToWriteOversizedFrame() {
        assertThatThrownBy(() -> FrameCodec.writeFrame(
                new ByteArrayOutputStream(), new byte[FrameCodec.MAX_FRAME_BYTES + 1]))
                .isInstanceOf(ProtocolException.class);
    }
}
