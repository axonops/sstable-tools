package com.axonops.sstable.workspace;

import java.time.Instant;
import java.nio.file.Path;
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
    private final Path outputPath;
    private final List<ManifestFile> files;

    public ExportRecord(UUID exportId,
                        Instant createdAt,
                        String outputFormat,
                        Path outputPath,
                        List<ManifestFile> files) throws WorkspaceException {
        if (exportId == null || createdAt == null || outputFormat == null
                || outputFormat.trim().isEmpty() || outputPath == null || files == null
                || files.isEmpty()) {
            throw new WorkspaceException("Export record fields must not be null or empty");
        }
        Path normalized = outputPath.toAbsolutePath().normalize();
        if (!outputPath.isAbsolute() || !outputPath.equals(normalized)) {
            throw new WorkspaceException("Export output path must be absolute and normalized: "
                    + outputPath);
        }
        List<ManifestFile> sorted = new ArrayList<>(files);
        sorted.sort((left, right) -> left.relativePath().compareTo(right.relativePath()));
        java.util.Set<String> paths = new java.util.HashSet<>();
        for (ManifestFile file : sorted) {
            if (file == null || !paths.add(file.relativePath())) {
                throw new WorkspaceException("Export record contains a null or duplicate file");
            }
        }
        this.exportId = exportId;
        this.createdAt = createdAt;
        this.outputFormat = outputFormat;
        this.outputPath = normalized;
        this.files = Collections.unmodifiableList(sorted);
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

    public Path outputPath() {
        return outputPath;
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
                && outputFormat.equals(that.outputFormat)
                && outputPath.equals(that.outputPath) && files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exportId, createdAt, outputFormat, outputPath, files);
    }
}
