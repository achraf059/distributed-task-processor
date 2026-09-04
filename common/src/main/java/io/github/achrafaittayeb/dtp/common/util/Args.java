package io.github.achrafaittayeb.dtp.common.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal {@code --key value} command-line parser used by all three executables.
 * Every option can also be supplied through an environment variable
 * ({@code --worker-port} → {@code DTP_WORKER_PORT}); explicit flags win.
 */
public final class Args {

    private final Map<String, String> values = new HashMap<>();

    private Args() {
    }

    public static Args parse(String[] args) {
        Args parsed = new Args();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for option " + arg);
            }
            parsed.values.put(arg.substring(2), args[++i]);
        }
        return parsed;
    }

    public String get(String key, String defaultValue) {
        String flag = values.get(key);
        if (flag != null) {
            return flag;
        }
        String env = System.getenv("DTP_" + key.toUpperCase().replace('-', '_'));
        return env != null ? env : defaultValue;
    }

    public int getInt(String key, int defaultValue) {
        String raw = get(key, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option --" + key + " must be an integer, got: " + raw);
        }
    }

    public long getLong(String key, long defaultValue) {
        String raw = get(key, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option --" + key + " must be an integer, got: " + raw);
        }
    }
}
