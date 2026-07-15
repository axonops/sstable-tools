package com.csforge.sstable.workspace;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/** Versioned, immutable persisted state for one writable SSTable workspace. */
public final class WorkspaceManifest {
    public static final int CURRENT_FORMAT_VERSION = 1;

    private final int formatVersion;
    private final UUID workspaceId;
    private final WorkspaceState state;
    private final WorkspaceState lastStableState;
    private final String failureMessage;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final SourceInventory sourceInventory;
    private final SortedMap<String, String> schemaIdentity;
    private final SortedMap<String, String> runtimeIdentity;
    private final SortedMap<String, String> outputIdentity;
    private final List<ManifestFile> baselineInventory;
    private final List<ExportRecord> exports;

    WorkspaceManifest(int formatVersion,
                      UUID workspaceId,
                      WorkspaceState state,
                      WorkspaceState lastStableState,
                      String failureMessage,
                      Instant createdAt,
                      Instant updatedAt,
                      SourceInventory sourceInventory,
                      Map<String, String> schemaIdentity,
                      Map<String, String> runtimeIdentity,
                      Map<String, String> outputIdentity,
                      List<ManifestFile> baselineInventory,
                      List<ExportRecord> exports) throws WorkspaceException {
        if (formatVersion != CURRENT_FORMAT_VERSION) {
            throw new WorkspaceException("Unsupported workspace manifest format version "
                    + formatVersion);
        }
        if (workspaceId == null || state == null || createdAt == null || updatedAt == null
                || sourceInventory == null || schemaIdentity == null || runtimeIdentity == null
                || outputIdentity == null || baselineInventory == null || exports == null) {
            throw new WorkspaceException("Workspace manifest contains null required fields");
        }
        if (updatedAt.isBefore(createdAt)) {
            throw new WorkspaceException("Workspace manifest update time precedes creation time");
        }
        if (state == WorkspaceState.FAILED_RECOVERABLE) {
            if (lastStableState == null || lastStableState == WorkspaceState.FAILED_RECOVERABLE
                    || failureMessage == null || failureMessage.trim().isEmpty()) {
                throw new WorkspaceException("Recoverable failure must record a stable state and message");
            }
        } else if (lastStableState != null || failureMessage != null) {
            throw new WorkspaceException("Non-failed manifest must not contain failure fields");
        }

        this.formatVersion = formatVersion;
        this.workspaceId = workspaceId;
        this.state = state;
        this.lastStableState = lastStableState;
        this.failureMessage = failureMessage;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.sourceInventory = sourceInventory;
        this.schemaIdentity = immutableMap(schemaIdentity, "schema identity");
        this.runtimeIdentity = immutableMap(runtimeIdentity, "runtime identity");
        this.outputIdentity = immutableMap(outputIdentity, "output identity");
        this.baselineInventory = Collections.unmodifiableList(
                new ArrayList<>(baselineInventory));
        this.exports = Collections.unmodifiableList(new ArrayList<>(exports));
    }

    public static WorkspaceManifest create(SourceInventory sourceInventory)
            throws WorkspaceException {
        return create(UUID.randomUUID(), Instant.now(), sourceInventory);
    }

    static WorkspaceManifest create(UUID workspaceId,
                                    Instant now,
                                    SourceInventory sourceInventory) throws WorkspaceException {
        return new WorkspaceManifest(CURRENT_FORMAT_VERSION, workspaceId, WorkspaceState.NEW,
                null, null, now, now, sourceInventory,
                Collections.<String, String>emptyMap(),
                Collections.<String, String>emptyMap(),
                Collections.<String, String>emptyMap(),
                Collections.<ManifestFile>emptyList(),
                Collections.<ExportRecord>emptyList());
    }

    public WorkspaceManifest transitionTo(WorkspaceState target) throws WorkspaceException {
        return transitionTo(target, Instant.now());
    }

    WorkspaceManifest transitionTo(WorkspaceState target, Instant now)
            throws WorkspaceException {
        if (target == WorkspaceState.FAILED_RECOVERABLE) {
            throw new WorkspaceException("Use fail(message) to enter FAILED_RECOVERABLE");
        }
        if (!state.canTransitionTo(target)) {
            throw new WorkspaceException("Invalid workspace transition " + state + " -> " + target);
        }
        return copy(target, null, null, now);
    }

    public WorkspaceManifest fail(String message) throws WorkspaceException {
        return fail(message, Instant.now());
    }

    WorkspaceManifest fail(String message, Instant now) throws WorkspaceException {
        if (state == WorkspaceState.FAILED_RECOVERABLE) {
            throw new WorkspaceException("Workspace is already in FAILED_RECOVERABLE");
        }
        if (message == null || message.trim().isEmpty()) {
            throw new WorkspaceException("Failure message must not be empty");
        }
        return copy(WorkspaceState.FAILED_RECOVERABLE, state, message.trim(), now);
    }

    public WorkspaceManifest recover() throws WorkspaceException {
        return recover(Instant.now());
    }

    WorkspaceManifest recover(Instant now) throws WorkspaceException {
        if (state != WorkspaceState.FAILED_RECOVERABLE) {
            throw new WorkspaceException("Only FAILED_RECOVERABLE can be recovered");
        }
        return copy(lastStableState, null, null, now);
    }

    private WorkspaceManifest copy(WorkspaceState nextState,
                                   WorkspaceState nextStableState,
                                   String nextFailureMessage,
                                   Instant now) throws WorkspaceException {
        if (now == null || now.isBefore(updatedAt)) {
            throw new WorkspaceException("Workspace transition time precedes current update time");
        }
        return new WorkspaceManifest(formatVersion, workspaceId, nextState,
                nextStableState, nextFailureMessage, createdAt, now, sourceInventory,
                schemaIdentity, runtimeIdentity, outputIdentity, baselineInventory, exports);
    }

    private static SortedMap<String, String> immutableMap(Map<String, String> source,
                                                          String description)
            throws WorkspaceException {
        TreeMap<String, String> copy = new TreeMap<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            if (entry.getKey() == null || entry.getKey().trim().isEmpty()
                    || entry.getValue() == null) {
                throw new WorkspaceException("Invalid " + description + " entry");
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableSortedMap(copy);
    }

    public int formatVersion() {
        return formatVersion;
    }

    public UUID workspaceId() {
        return workspaceId;
    }

    public WorkspaceState state() {
        return state;
    }

    public WorkspaceState lastStableState() {
        return lastStableState;
    }

    public String failureMessage() {
        return failureMessage;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public SourceInventory sourceInventory() {
        return sourceInventory;
    }

    public SortedMap<String, String> schemaIdentity() {
        return schemaIdentity;
    }

    public SortedMap<String, String> runtimeIdentity() {
        return runtimeIdentity;
    }

    public SortedMap<String, String> outputIdentity() {
        return outputIdentity;
    }

    public List<ManifestFile> baselineInventory() {
        return baselineInventory;
    }

    public List<ExportRecord> exports() {
        return exports;
    }

    public RecoveryAction recoveryAction() {
        return state.recoveryAction();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WorkspaceManifest)) {
            return false;
        }
        WorkspaceManifest that = (WorkspaceManifest) other;
        return formatVersion == that.formatVersion && workspaceId.equals(that.workspaceId)
                && state == that.state && lastStableState == that.lastStableState
                && Objects.equals(failureMessage, that.failureMessage)
                && createdAt.equals(that.createdAt) && updatedAt.equals(that.updatedAt)
                && sourceInventory.equals(that.sourceInventory)
                && schemaIdentity.equals(that.schemaIdentity)
                && runtimeIdentity.equals(that.runtimeIdentity)
                && outputIdentity.equals(that.outputIdentity)
                && baselineInventory.equals(that.baselineInventory)
                && exports.equals(that.exports);
    }

    @Override
    public int hashCode() {
        return Objects.hash(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, updatedAt, sourceInventory, schemaIdentity,
                runtimeIdentity, outputIdentity, baselineInventory, exports);
    }
}
