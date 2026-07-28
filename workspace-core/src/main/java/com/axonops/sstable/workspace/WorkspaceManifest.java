package com.axonops.sstable.workspace;

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
        return recoverTo(lastStableState, Instant.now());
    }

    public WorkspaceManifest recoverTo(WorkspaceState target) throws WorkspaceException {
        return recoverTo(target, Instant.now());
    }

    WorkspaceManifest recover(Instant now) throws WorkspaceException {
        return recoverTo(lastStableState, now);
    }

    WorkspaceManifest recoverTo(WorkspaceState target, Instant now)
            throws WorkspaceException {
        if (state != WorkspaceState.FAILED_RECOVERABLE) {
            throw new WorkspaceException("Only FAILED_RECOVERABLE can be recovered");
        }
        if (target != lastStableState
                && (target != WorkspaceState.STOPPED
                || !lastStableState.canTransitionTo(WorkspaceState.STOPPED))) {
            throw new WorkspaceException("Cannot recover failed " + lastStableState
                    + " workspace to " + target);
        }
        return copy(target, null, null, now);
    }

    public WorkspaceManifest withRuntimeIdentity(Map<String, String> runtime,
                                                 Map<String, String> output)
            throws WorkspaceException {
        if (runtime == null || runtime.isEmpty() || output == null || output.isEmpty()) {
            throw new WorkspaceException("Runtime and output identities must not be empty");
        }
        SortedMap<String, String> nextRuntime = immutableMap(runtime, "runtime identity");
        SortedMap<String, String> nextOutput = mergeIdentity(outputIdentity, output,
                "output identity");
        if (!runtimeIdentity.isEmpty() && !runtimeIdentity.equals(nextRuntime)) {
            throw new WorkspaceException("Workspace runtime identity cannot change");
        }
        return new WorkspaceManifest(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, Instant.now(), sourceInventory, schemaIdentity,
                nextRuntime, nextOutput, baselineInventory, exports);
    }

    /** Records immutable output characteristics before a runtime is selected. */
    public WorkspaceManifest withOutputIdentity(Map<String, String> additions)
            throws WorkspaceException {
        if (additions == null || additions.isEmpty()) {
            throw new WorkspaceException("Output identity additions must not be empty");
        }
        SortedMap<String, String> merged = mergeIdentity(outputIdentity, additions,
                "output identity");
        return new WorkspaceManifest(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, Instant.now(), sourceInventory, schemaIdentity,
                runtimeIdentity, merged, baselineInventory, exports);
    }

    public WorkspaceManifest withSchemaIdentity(Map<String, String> additions)
            throws WorkspaceException {
        if (additions == null || additions.isEmpty()) {
            throw new WorkspaceException("Schema identity additions must not be empty");
        }
        SortedMap<String, String> merged = mergeIdentity(schemaIdentity, additions,
                "schema identity");
        return new WorkspaceManifest(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, Instant.now(), sourceInventory, merged,
                runtimeIdentity, outputIdentity, baselineInventory, exports);
    }

    public WorkspaceManifest withImportResult(Map<String, String> schemaAdditions,
                                              List<ManifestFile> baseline)
            throws WorkspaceException {
        if (state != WorkspaceState.VALIDATED) {
            throw new WorkspaceException("Import result requires state VALIDATED, not " + state);
        }
        if (schemaAdditions == null || schemaAdditions.isEmpty() || baseline == null
                || (!sourceInventory.sets().isEmpty() && baseline.isEmpty())) {
            throw new WorkspaceException("Import result requires schema and baseline identities");
        }
        SortedMap<String, String> merged = mergeIdentity(schemaIdentity, schemaAdditions,
                "schema identity");
        List<ManifestFile> nextBaseline = immutableFiles(baseline);
        if (!baselineInventory.isEmpty() && !baselineInventory.equals(nextBaseline)) {
            throw new WorkspaceException("Workspace baseline inventory cannot change");
        }
        return new WorkspaceManifest(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, Instant.now(), sourceInventory, merged,
                runtimeIdentity, outputIdentity, nextBaseline, exports);
    }

    public WorkspaceManifest withExport(ExportRecord export) throws WorkspaceException {
        if (state != WorkspaceState.FLUSHED) {
            throw new WorkspaceException("Export record requires state FLUSHED, not " + state);
        }
        if (export == null) {
            throw new WorkspaceException("Export record must not be null");
        }
        List<ExportRecord> nextExports = new ArrayList<>(exports);
        for (ExportRecord existing : nextExports) {
            if (existing.exportId().equals(export.exportId())
                    || existing.outputPath().equals(export.outputPath())) {
                throw new WorkspaceException("Workspace export identity or output path is "
                        + "already recorded");
            }
        }
        nextExports.add(export);
        return new WorkspaceManifest(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, Instant.now(), sourceInventory, schemaIdentity,
                runtimeIdentity, outputIdentity, baselineInventory, nextExports);
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

    private static SortedMap<String, String> mergeIdentity(Map<String, String> existing,
                                                           Map<String, String> additions,
                                                           String description)
            throws WorkspaceException {
        TreeMap<String, String> merged = new TreeMap<>(existing);
        SortedMap<String, String> validated = immutableMap(additions, description);
        for (Map.Entry<String, String> entry : validated.entrySet()) {
            String previous = merged.putIfAbsent(entry.getKey(), entry.getValue());
            if (previous != null && !previous.equals(entry.getValue())) {
                throw new WorkspaceException("Workspace " + description + " entry cannot change: "
                        + entry.getKey());
            }
        }
        return Collections.unmodifiableSortedMap(merged);
    }

    private static List<ManifestFile> immutableFiles(List<ManifestFile> files)
            throws WorkspaceException {
        List<ManifestFile> copy = new ArrayList<>(files);
        if (copy.contains(null)) {
            throw new WorkspaceException("Baseline inventory contains a null file");
        }
        Map<String, ManifestFile> unique = new TreeMap<>();
        for (ManifestFile file : copy) {
            if (unique.put(file.relativePath(), file) != null) {
                throw new WorkspaceException("Duplicate baseline file " + file.relativePath());
            }
        }
        copy.sort((left, right) -> left.relativePath().compareTo(right.relativePath()));
        return Collections.unmodifiableList(copy);
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
