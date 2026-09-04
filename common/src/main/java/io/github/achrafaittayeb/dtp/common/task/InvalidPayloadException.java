package io.github.achrafaittayeb.dtp.common.task;

/** A task payload is missing fields, has wrong types, or exceeds resource bounds. */
public class InvalidPayloadException extends Exception {

    public InvalidPayloadException(String message) {
        super(message);
    }
}
