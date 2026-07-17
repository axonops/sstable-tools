package com.axonops.sstable.workspace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Canonical path confinement for every workspace-owned read or write. */
final class WorkspacePaths {
    private WorkspacePaths() {
    }

    static Path resolveInside(Path root, String relativePath) throws WorkspaceException {
        final Path relative;
        try {
            relative = Paths.get(relativePath);
        } catch (RuntimeException e) {
            throw new WorkspaceException("Invalid workspace relative path: " + relativePath, e);
        }
        boolean unsafeSegment = false;
        for (Path segment : relative) {
            if ("..".equals(segment.toString()) || ".".equals(segment.toString())) {
                unsafeSegment = true;
            }
        }
        if (relative.isAbsolute() || relative.getNameCount() == 0 || unsafeSegment
                || !relative.equals(relative.normalize())) {
            throw new WorkspaceException("Workspace path must be normalized and relative: "
                    + relativePath);
        }

        Path candidate = root.resolve(relative).normalize();
        if (!candidate.startsWith(root)) {
            throw new WorkspaceException("Workspace path escapes its root: " + relativePath);
        }

        Path current = root;
        for (Path segment : relative) {
            current = current.resolve(segment);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)
                    && Files.isSymbolicLink(current)) {
                throw new WorkspaceException("Workspace path crosses a symlink: " + current);
            }
        }

        if (Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)) {
            try {
                Path real = candidate.toRealPath();
                if (!real.startsWith(root)) {
                    throw new WorkspaceException("Workspace path resolves outside its root: "
                            + relativePath);
                }
                return real;
            } catch (IOException e) {
                throw new WorkspaceException("Cannot resolve workspace path " + candidate, e);
            }
        }
        return candidate;
    }
}
