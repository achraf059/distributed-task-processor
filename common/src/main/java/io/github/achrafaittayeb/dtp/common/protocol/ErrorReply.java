package io.github.achrafaittayeb.dtp.common.protocol;

/** Generic error reply; the human-readable message is safe to show to users. */
public record ErrorReply(String message) implements Message {
}
