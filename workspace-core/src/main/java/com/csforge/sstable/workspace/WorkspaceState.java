package com.csforge.sstable.workspace;

/** Persisted lifecycle states for a writable SSTable workspace. */
public enum WorkspaceState {
    NEW,
    VALIDATED,
    IMPORTED,
    RUNNING,
    FLUSHED,
    EXPORTED,
    STOPPED,
    FAILED_RECOVERABLE
}
