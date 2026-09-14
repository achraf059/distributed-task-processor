package io.github.achrafaittayeb.dtp.common.model;

import java.util.Locale;

/**
 * Base scheduling priority a client attaches to a submission. Exactly three
 * levels are exposed on the wire — the protocol deliberately carries these
 * names, never raw numbers, so the public contract cannot drift as internal
 * scheduling math changes.
 *
 * <p>Each level has an integer <em>base level</em> that the coordinator's
 * scheduler uses as the starting point for age-based promotion:
 * {@code HIGH=2}, {@code NORMAL=1}, {@code LOW=0}. Effective (aged) level is
 * derived by the coordinator at scheduling time and is never persisted or put
 * on the wire — see {@code Job#effectiveLevel} and {@code CoordinatorCore}.
 *
 * <p><b>Default:</b> a missing priority — an old client, an old persisted row,
 * or any call site that omits it — always means {@link #NORMAL}. Use
 * {@link #normalize(TaskPriority)} to collapse {@code null} to {@code NORMAL}
 * so "omitted" and "explicit NORMAL" are indistinguishable everywhere.
 */
public enum TaskPriority {
    /** Lowest base level (0); still eligible for aging promotion. */
    LOW(0),
    /** Default base level (1); the level a missing/omitted priority maps to. */
    NORMAL(1),
    /** Highest base level (2); also the ceiling that aging can promote toward. */
    HIGH(2);

    /** Ceiling for effective level: no job ever schedules above {@link #HIGH}. */
    public static final int MAX_LEVEL = 2;

    private final int baseLevel;

    TaskPriority(int baseLevel) {
        this.baseLevel = baseLevel;
    }

    /** Scheduler starting level before any age-based promotion is applied. */
    public int baseLevel() {
        return baseLevel;
    }

    /** Collapses a nullable priority to a concrete one: {@code null} → {@link #NORMAL}. */
    public static TaskPriority normalize(TaskPriority priority) {
        return priority == null ? NORMAL : priority;
    }

    /**
     * Parses a case-insensitive priority name ({@code high}/{@code normal}/{@code low}).
     * A {@code null} or blank input defaults to {@link #NORMAL}; any other
     * unrecognized value throws {@link IllegalArgumentException} with guidance.
     */
    public static TaskPriority fromString(String value) {
        if (value == null || value.isBlank()) {
            return NORMAL;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "high" -> HIGH;
            case "normal" -> NORMAL;
            case "low" -> LOW;
            default -> throw new IllegalArgumentException(
                    "Invalid priority '" + value + "'; expected one of: high, normal, low");
        };
    }
}
