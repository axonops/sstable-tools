package com.axonops.sstable.workspace;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/** Identity of a workspace-owned baseline or exported file. */
public final class ManifestFile {
    private final String relativePath;
    private final long size;
    private final String sha256;

    public ManifestFile(String relativePath, long size, String sha256)
            throws WorkspaceException {
        Path path;
        try {
            path = Paths.get(relativePath == null ? "" : relativePath);
        } catch (RuntimeException e) {
            throw new WorkspaceException("Unsafe manifest relative path: " + relativePath, e);
        }
        boolean unsafeSegment = false;
        for (Path segment : path) {
            if (".".equals(segment.toString()) || "..".equals(segment.toString())) {
                unsafeSegment = true;
            }
        }
        if (relativePath == null || relativePath.trim().isEmpty() || path.isAbsolute()
                || path.getNameCount() == 0 || unsafeSegment || !path.equals(path.normalize())
                || relativePath.indexOf('\\') >= 0
                || relativePath.matches("^[A-Za-z]:.*")) {
            throw new WorkspaceException("Unsafe manifest relative path: " + relativePath);
        }
        if (size < 0 || sha256 == null || !sha256.matches("[0-9a-f]{64}")) {
            throw new WorkspaceException("Invalid manifest file identity for " + relativePath);
        }
        this.relativePath = relativePath;
        this.size = size;
        this.sha256 = sha256;
    }

    public String relativePath() {
        return relativePath;
    }

    public long size() {
        return size;
    }

    public String sha256() {
        return sha256;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ManifestFile)) {
            return false;
        }
        ManifestFile that = (ManifestFile) other;
        return size == that.size && relativePath.equals(that.relativePath)
                && sha256.equals(that.sha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relativePath, size, sha256);
    }
}
