package io.github.achrafaittayeb.dtp.it;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * A test clock the coordinator reads instead of the system clock, so tests that
 * exercise time-dependent behavior (priority aging) can advance time explicitly
 * and deterministically rather than waiting real milliseconds. Backed by an
 * {@link AtomicLong} because the coordinator core thread reads it while the test
 * thread advances it.
 */
final class MutableClock implements LongSupplier {

    private final AtomicLong millis;

    MutableClock(long startMillis) {
        this.millis = new AtomicLong(startMillis);
    }

    /** Moves time forward by {@code delta} milliseconds (delta must be >= 0). */
    void advance(long delta) {
        millis.addAndGet(delta);
    }

    @Override
    public long getAsLong() {
        return millis.get();
    }
}
