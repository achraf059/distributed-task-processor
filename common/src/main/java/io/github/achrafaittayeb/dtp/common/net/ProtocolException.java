package io.github.achrafaittayeb.dtp.common.net;

import java.io.IOException;

/**
 * A peer violated the wire protocol: bad frame length, undecodable JSON,
 * or an unknown message type. The connection that produced it should be closed;
 * the rest of the process must keep running.
 */
public class ProtocolException extends IOException {

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
