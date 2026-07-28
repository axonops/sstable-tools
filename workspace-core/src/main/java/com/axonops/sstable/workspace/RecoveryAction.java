package com.axonops.sstable.workspace;

/** Documented operator action for each persisted workspace state. */
public enum RecoveryAction {
    RETRY_CREATE("retry workspace create, or destroy the incomplete workspace"),
    RESUME_IMPORT("resume source validation and import"),
    START_WORKER("start the version-matched worker"),
    CHECK_RUNNING_WORKER("check worker status; run workspace recover when its PID is stale"),
    EXPORT_FLUSHED_DATA("export the flushed delta, or restart the worker"),
    VERIFY_EXPORT("verify the export, then stop or restart the workspace"),
    RESTART_OR_DESTROY("restart, export, or destroy the stopped workspace"),
    RECOVER_LAST_STABLE_STATE("inspect the failure and recover to the recorded stable state");

    private final String description;

    RecoveryAction(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
