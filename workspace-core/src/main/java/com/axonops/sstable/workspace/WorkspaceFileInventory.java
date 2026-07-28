package com.axonops.sstable.workspace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Captures checksummed identities for files in a confined workspace subtree. */
public final class WorkspaceFileInventory {
    private WorkspaceFileInventory() {
    }

    public static List<ManifestFile> capture(Path workspaceRoot, String relativeDirectory)
            throws WorkspaceException {
        return capture(workspaceRoot, relativeDirectory, false);
    }

    public static List<ManifestFile> captureAllowEmpty(Path workspaceRoot,
                                                       String relativeDirectory)
            throws WorkspaceException {
        return capture(workspaceRoot, relativeDirectory, true);
    }

    private static List<ManifestFile> capture(Path workspaceRoot,
                                              String relativeDirectory,
                                              boolean allowEmpty)
            throws WorkspaceException {
        Path root;
        Path directory;
        try {
            root = workspaceRoot.toRealPath();
            directory = WorkspacePaths.resolveInside(root, relativeDirectory);
            if (Files.isSymbolicLink(directory)
                    || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("Workspace inventory directory is missing or "
                        + "unsafe: " + directory);
            }
            directory = directory.toRealPath();
            if (!directory.startsWith(root)) {
                throw new WorkspaceException("Workspace inventory directory escapes root: "
                        + directory);
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot resolve workspace inventory directory "
                    + relativeDirectory, e);
        }

        List<Path> paths = new ArrayList<>();
        try (Stream<Path> walked = Files.walk(directory)) {
            walked.forEach(path -> {
                if (Files.isSymbolicLink(path)) {
                    throw new UnsafeInventoryPath(path);
                }
                if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    paths.add(path);
                }
            });
        } catch (UnsafeInventoryPath e) {
            throw new WorkspaceException("Workspace inventory crosses a symlink: " + e.path);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot walk workspace inventory directory "
                    + directory, e);
        }
        paths.sort(Comparator.comparing(path -> root.relativize(path).toString()));
        if (paths.isEmpty() && !allowEmpty) {
            throw new WorkspaceException("Workspace baseline inventory must not be empty: "
                    + directory);
        }

        List<ManifestFile> files = new ArrayList<>();
        Set<String> relativePaths = new HashSet<>();
        for (Path path : paths) {
            String relative = root.relativize(path).toString().replace('\\', '/');
            if (!relativePaths.add(relative)) {
                throw new WorkspaceException("Duplicate workspace baseline path " + relative);
            }
            try {
                files.add(new ManifestFile(relative, Files.size(path), Hashing.sha256(path)));
            } catch (IOException e) {
                throw new WorkspaceException("Cannot inventory workspace file " + path, e);
            }
        }
        return files;
    }

    public static void verifyUnchanged(Path workspaceRoot, List<ManifestFile> expected)
            throws WorkspaceException {
        if (expected == null || expected.isEmpty()) {
            throw new WorkspaceException("Workspace baseline inventory is missing");
        }
        final Path root;
        try {
            root = workspaceRoot.toRealPath();
        } catch (IOException e) {
            throw new WorkspaceException("Cannot resolve workspace baseline root", e);
        }
        for (ManifestFile file : expected) {
            Path path = WorkspacePaths.resolveInside(root, file.relativePath());
            try {
                if (Files.isSymbolicLink(path)
                        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new WorkspaceException("Workspace baseline file is missing or unsafe: "
                            + path);
                }
                if (Files.size(path) != file.size()
                        || !file.sha256().equals(Hashing.sha256(path))) {
                    throw new WorkspaceException("Workspace baseline file changed: " + path);
                }
            } catch (IOException e) {
                throw new WorkspaceException("Cannot verify workspace baseline file " + path, e);
            }
        }
    }

    private static final class UnsafeInventoryPath extends RuntimeException {
        private final Path path;

        private UnsafeInventoryPath(Path path) {
            this.path = path;
        }
    }
}
