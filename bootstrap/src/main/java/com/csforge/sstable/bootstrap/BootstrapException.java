package com.csforge.sstable.bootstrap;

/** An actionable bootstrap failure with a stable process exit code. */
public final class BootstrapException extends Exception {
    public static final int USAGE_EXIT_CODE = 2;
    public static final int DISCOVERY_EXIT_CODE = 3;
    public static final int COMPATIBILITY_EXIT_CODE = 4;
    public static final int CHILD_EXIT_CODE = 5;

    private final int exitCode;

    public BootstrapException(int exitCode, String message) {
        super(message);
        this.exitCode = exitCode;
    }

    public BootstrapException(int exitCode, String message, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    public int exitCode() {
        return exitCode;
    }
}
