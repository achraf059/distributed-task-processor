package io.github.achrafaittayeb.dtp.common.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Length-prefixed framing over a byte stream:
 *
 * <pre>[4-byte big-endian payload length][payload bytes]</pre>
 *
 * TCP is a byte stream with no message boundaries, so a single {@code read()} may
 * return half a frame or two frames. {@link DataInputStream#readFully} loops over
 * partial reads until the exact frame is assembled, which is what makes this codec
 * correct rather than "usually working".
 */
public final class FrameCodec {

    /** Upper bound on a single frame; anything larger is a protocol violation. */
    public static final int MAX_FRAME_BYTES = 1 << 20; // 1 MiB

    private FrameCodec() {
    }

    /**
     * Reads one complete frame.
     *
     * @return the payload, or {@code null} if the peer closed the connection
     *         cleanly at a frame boundary
     * @throws ProtocolException if the declared length is non-positive or exceeds
     *         {@link #MAX_FRAME_BYTES}
     * @throws EOFException if the connection dropped in the middle of a frame
     */
    public static byte[] readFrame(InputStream in) throws IOException {
        DataInputStream data = new DataInputStream(in);
        int length;
        try {
            length = data.readInt();
        } catch (EOFException endOfStream) {
            return null; // clean close between frames
        }
        if (length <= 0 || length > MAX_FRAME_BYTES) {
            throw new ProtocolException("Invalid frame length " + length
                    + " (must be 1.." + MAX_FRAME_BYTES + ")");
        }
        byte[] payload = new byte[length];
        data.readFully(payload);
        return payload;
    }

    /** Writes one frame. The caller is responsible for serializing concurrent writers. */
    public static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        if (payload.length == 0 || payload.length > MAX_FRAME_BYTES) {
            throw new ProtocolException("Refusing to write frame of length " + payload.length);
        }
        DataOutputStream data = new DataOutputStream(out);
        data.writeInt(payload.length);
        data.write(payload);
        data.flush();
    }
}
