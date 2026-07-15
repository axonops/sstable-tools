package com.csforge.sstable.workspace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;

/** Canonical, immutable identity of one SSTable component file. */
public final class SourceComponent {
    private final String name;
    private final Path path;
    private final long size;
    private final String sha256;

    SourceComponent(String name, Path path, long size, String sha256)
            throws WorkspaceException {
        if (name == null || name.trim().isEmpty() || path == null || !path.isAbsolute()
                || !path.equals(path.normalize())) {
            throw new WorkspaceException("Invalid source component identity: " + path);
        }
        if (size < 0 || sha256 == null || !sha256.matches("[0-9a-f]{64}")) {
            throw new WorkspaceException("Invalid source component file identity for " + path);
        }
        this.name = name;
        this.path = path;
        this.size = size;
        this.sha256 = sha256;
    }

    static SourceComponent capture(String name, Path path) throws WorkspaceException {
        try {
            Path canonical = path.toRealPath();
            if (!Files.isRegularFile(canonical) || Files.isSymbolicLink(path)) {
                throw new WorkspaceException("SSTable component is not a regular file: " + path);
            }
            return new SourceComponent(name, canonical, Files.size(canonical),
                    Hashing.sha256(canonical));
        } catch (IOException e) {
            throw new WorkspaceException("Cannot resolve SSTable component " + path, e);
        }
    }

    public void verifyUnchanged() throws WorkspaceException {
        try {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(path)) {
                throw mutated("is missing or is no longer a regular non-symlink file");
            }
            if (Files.size(path) != size) {
                throw mutated("size changed from " + size + " to " + Files.size(path));
            }
            String currentHash = Hashing.sha256(path);
            if (!sha256.equals(currentHash)) {
                throw mutated("SHA-256 changed from " + sha256 + " to " + currentHash);
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot verify source component " + path, e);
        }
    }

    private WorkspaceException mutated(String detail) {
        return new WorkspaceException("Source mutation detected for " + path + ": " + detail);
    }

    public String name() {
        return name;
    }

    public Path path() {
        return path;
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
        if (!(other instanceof SourceComponent)) {
            return false;
        }
        SourceComponent that = (SourceComponent) other;
        return size == that.size && name.equals(that.name) && path.equals(that.path)
                && sha256.equals(that.sha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, size, sha256);
    }
}
