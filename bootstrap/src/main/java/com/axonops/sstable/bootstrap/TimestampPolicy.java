package com.axonops.sstable.bootstrap;

/** Timestamp assignment policy selected for a writable workspace. */
enum TimestampPolicy {
    WALL_CLOCK("wall-clock"),
    AFTER_SOURCE("after-source");

    static final String MANIFEST_KEY = "timestamp.policy";
    private final String value;

    TimestampPolicy(String value) {
        this.value = value;
    }

    String value() {
        return value;
    }

    static TimestampPolicy parse(String value) throws BootstrapException {
        for (TimestampPolicy policy : values()) {
            if (policy.value.equals(value)) {
                return policy;
            }
        }
        throw new BootstrapException(BootstrapException.USAGE_EXIT_CODE,
                "--timestamp-policy must be wall-clock or after-source");
    }
}
