package com.csforge.sstable.bootstrap;

/** Supported workspace export publication modes. */
enum ExportMode {
    DELTA("delta"),
    SNAPSHOT("snapshot");

    private final String value;

    ExportMode(String value) {
        this.value = value;
    }

    String value() {
        return value;
    }

    static ExportMode parse(String value) throws BootstrapException {
        for (ExportMode mode : values()) {
            if (mode.value.equals(value)) {
                return mode;
            }
        }
        throw new BootstrapException(BootstrapException.USAGE_EXIT_CODE,
                "--mode must be delta or snapshot");
    }
}
