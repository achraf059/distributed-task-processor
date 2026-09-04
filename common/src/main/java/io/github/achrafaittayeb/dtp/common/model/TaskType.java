package io.github.achrafaittayeb.dtp.common.model;

/**
 * Built-in task types. Tasks are deterministic and resource-bounded on purpose:
 * the system schedules work, it does not execute arbitrary code.
 */
public enum TaskType {
    /** Sleeps for {@code durationMillis}. Useful for demos and failure testing. */
    SLEEP,
    /** Counts whitespace-separated words in {@code text}. */
    WORD_COUNT,
    /** Computes the SHA-256 hex digest of {@code text}. */
    SHA256,
    /** Counts primes below {@code limit} (deterministic CPU-bound work). */
    PRIME_COUNT,
    /** Fails while the attempt number is below {@code failUntilAttempt}. For retry testing. */
    FAIL
}
