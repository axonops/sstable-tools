package com.csforge.sstable.workspace;

/** Persisted lifecycle states for a writable SSTable workspace. */
public enum WorkspaceState {
    NEW(RecoveryAction.RETRY_CREATE),
    VALIDATED(RecoveryAction.RESUME_IMPORT),
    IMPORTED(RecoveryAction.START_WORKER),
    RUNNING(RecoveryAction.CHECK_RUNNING_WORKER),
    FLUSHED(RecoveryAction.EXPORT_FLUSHED_DATA),
    EXPORTED(RecoveryAction.VERIFY_EXPORT),
    STOPPED(RecoveryAction.RESTART_OR_DESTROY),
    FAILED_RECOVERABLE(RecoveryAction.RECOVER_LAST_STABLE_STATE);

    private final RecoveryAction recoveryAction;

    WorkspaceState(RecoveryAction recoveryAction) {
        this.recoveryAction = recoveryAction;
    }

    public RecoveryAction recoveryAction() {
        return recoveryAction;
    }

    public boolean canTransitionTo(WorkspaceState target) {
        if (target == FAILED_RECOVERABLE && this != FAILED_RECOVERABLE) {
            return true;
        }
        switch (this) {
            case NEW:
                return target == VALIDATED;
            case VALIDATED:
                return target == IMPORTED;
            case IMPORTED:
                return target == RUNNING || target == STOPPED;
            case RUNNING:
                return target == FLUSHED || target == STOPPED;
            case FLUSHED:
                return target == EXPORTED || target == RUNNING || target == STOPPED;
            case EXPORTED:
                return target == RUNNING || target == STOPPED;
            case STOPPED:
                return target == RUNNING;
            case FAILED_RECOVERABLE:
                return false;
            default:
                return false;
        }
    }
}
