package com.csforge.sstable.workspace;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Immutable record of one validated workspace export. */
public final class ExportRecord {
    private final UUID exportId;
    private final Instant createdAt;
    private final String outputFormat;
    private final List<ManifestFile> files;

    public ExportRecord(UUID exportId,
                        Instant createdAt,
                        String outputFormat,
                        List<ManifestFile> files) throws WorkspaceException {
        if (exportId == null || createdAt == null || outputFormat == null
                || outputFormat.trim().isEmpty() || files == null) {
            throw new WorkspaceException("Export record fields must not be null or empty");
        }
        this.exportId = exportId;
        this.createdAt = createdAt;
        this.outputFormat = outputFormat;
        this.files = Collections.unmodifiableList(new ArrayList<>(files));
    }

    public UUID exportId() {
        return exportId;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public String outputFormat() {
        return outputFormat;
    }

    public List<ManifestFile> files() {
        return files;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ExportRecord)) {
            return false;
        }
        ExportRecord that = (ExportRecord) other;
        return exportId.equals(that.exportId) && createdAt.equals(that.createdAt)
                && outputFormat.equals(that.outputFormat) && files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exportId, createdAt, outputFormat, files);
    }
}
