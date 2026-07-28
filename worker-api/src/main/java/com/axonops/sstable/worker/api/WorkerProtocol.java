package com.axonops.sstable.worker.api;

/** Stable protocol identifiers shared by the controller and release workers. */
public final class WorkerProtocol {
    public static final int CURRENT_VERSION = 3;

    private WorkerProtocol() {
    }

    public enum Operation {
        HELLO,
        VALIDATE_SOURCE,
        VALIDATE_SCHEMA,
        IMPORT,
        START_NATIVE_TRANSPORT,
        STATUS,
        QUIESCE,
        FLUSH,
        INVENTORY,
        VERIFY,
        STOP
    }
}
