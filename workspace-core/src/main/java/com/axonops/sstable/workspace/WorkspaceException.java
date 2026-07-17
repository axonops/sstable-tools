package com.axonops.sstable.workspace;

/** A safe, user-actionable workspace validation or persistence failure. */
public final class WorkspaceException extends Exception {
    public WorkspaceException(String message) {
        super(message);
    }

    public WorkspaceException(String message, Throwable cause) {
        super(message, cause);
    }
}
