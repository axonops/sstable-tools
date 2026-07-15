package com.csforge.sstable.workspace;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Locked, durable repository for one workspace manifest and owned paths. */
public final class WorkspaceRepository {
    private static final String MANIFEST_FILE = "manifest.json";
    private static final String LOCK_FILE = ".workspace.lock";
    private static final long MAX_MANIFEST_BYTES = 16L * 1024L * 1024L;
    private static final long MAX_OWNED_FILE_BYTES = 16L * 1024L * 1024L;
    private static final List<String> LAYOUT_DIRECTORIES = Arrays.asList(
            "schema", "runtime", "data", "commitlog", "hints", "saved_caches",
            "logs", "staging", "exports", "state");

    private final Path root;
    private final Path manifestPath;
    private final Path lockPath;
    private final WorkspaceManifestCodec codec;

    private WorkspaceRepository(Path root) throws WorkspaceException {
        this.root = root;
        this.manifestPath = WorkspacePaths.resolveInside(root, MANIFEST_FILE);
        this.lockPath = WorkspacePaths.resolveInside(root, LOCK_FILE);
        this.codec = new WorkspaceManifestCodec();
    }

    public static WorkspaceRepository createAt(Path root) throws WorkspaceException {
        try {
            if (Files.exists(root, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(root)) {
                throw new WorkspaceException("Workspace root must not be a symlink: " + root);
            }
            Files.createDirectories(root);
            Path canonical = root.toRealPath();
            restrictDirectory(canonical);
            return new WorkspaceRepository(canonical);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot create workspace root " + root, e);
        }
    }

    public static WorkspaceRepository open(Path root) throws WorkspaceException {
        try {
            if (Files.isSymbolicLink(root)) {
                throw new WorkspaceException("Workspace root must not be a symlink: " + root);
            }
            Path canonical = root.toRealPath();
            if (!Files.isDirectory(canonical)) {
                throw new WorkspaceException("Workspace root is not a directory: " + root);
            }
            return new WorkspaceRepository(canonical);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot open workspace root " + root, e);
        }
    }

    public WorkspaceLock acquire() throws WorkspaceException {
        final FileChannel channel;
        try {
            channel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            restrictFile(lockPath);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot open workspace lock " + lockPath, e);
        }

        final FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            closeAfterFailedLock(channel);
            throw new WorkspaceException("Workspace is already locked by this process: " + root,
                    e);
        } catch (IOException e) {
            closeAfterFailedLock(channel);
            throw new WorkspaceException("Cannot acquire workspace lock " + lockPath, e);
        }
        if (lock == null) {
            closeAfterFailedLock(channel);
            throw new WorkspaceException("Workspace is already locked by another controller: "
                    + root);
        }

        try {
            String identity = "jvm=" + ManagementFactory.getRuntimeMXBean().getName() + "\n"
                    + "acquiredAt=" + Instant.now() + "\n";
            channel.truncate(0);
            channel.position(0);
            writeFully(channel, ByteBuffer.wrap(identity.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        } catch (IOException e) {
            try {
                lock.release();
                channel.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw new WorkspaceException("Cannot persist workspace lock identity " + lockPath, e);
        }
        return new WorkspaceLock(root, channel, lock);
    }

    public boolean manifestExists() {
        return Files.isRegularFile(manifestPath, LinkOption.NOFOLLOW_LINKS);
    }

    public WorkspaceManifest load() throws WorkspaceException {
        if (Files.isSymbolicLink(manifestPath)) {
            throw new WorkspaceException("Workspace manifest must not be a symlink: "
                    + manifestPath);
        }
        try {
            if (!Files.isRegularFile(manifestPath, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("Workspace manifest does not exist: " + manifestPath);
            }
            long size = Files.size(manifestPath);
            if (size <= 0 || size > MAX_MANIFEST_BYTES) {
                throw new WorkspaceException("Workspace manifest size is invalid: " + size);
            }
            return codec.decode(Files.readAllBytes(manifestPath));
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read workspace manifest " + manifestPath, e);
        }
    }

    public void initialize(WorkspaceLock lock, WorkspaceManifest manifest)
            throws WorkspaceException {
        requireLock(lock);
        if (manifestExists()) {
            throw new WorkspaceException("Workspace is already initialized: " + root);
        }
        if (manifest.state() != WorkspaceState.NEW) {
            throw new WorkspaceException("New workspace must start in state NEW");
        }
        createLayout();
        persist(manifest);
    }

    public void save(WorkspaceLock lock, WorkspaceManifest manifest)
            throws WorkspaceException {
        requireLock(lock);
        WorkspaceManifest current = load();
        if (!current.workspaceId().equals(manifest.workspaceId())) {
            throw new WorkspaceException("Workspace UUID cannot change from "
                    + current.workspaceId() + " to " + manifest.workspaceId());
        }
        if (current.formatVersion() != manifest.formatVersion()
                || !current.createdAt().equals(manifest.createdAt())
                || !current.sourceInventory().equals(manifest.sourceInventory())) {
            throw new WorkspaceException("Immutable workspace manifest identity cannot change");
        }
        if (!isValidSuccessor(current, manifest)) {
            throw new WorkspaceException("Manifest is not a valid successor of persisted state "
                    + current.state());
        }
        persist(manifest);
    }

    public Path resolveInside(String relativePath) throws WorkspaceException {
        return WorkspacePaths.resolveInside(root, relativePath);
    }

    public void writeOwnedFile(WorkspaceLock lock, String relativePath, byte[] content)
            throws WorkspaceException {
        requireLock(lock);
        if (content == null || content.length == 0 || content.length > MAX_OWNED_FILE_BYTES) {
            throw new WorkspaceException("Workspace-owned file has invalid size: "
                    + (content == null ? -1 : content.length));
        }
        Path destination = resolveInside(relativePath);
        Path parent = destination.getParent();
        if (parent == null || !parent.startsWith(root)) {
            throw new WorkspaceException("Workspace-owned file has no safe parent: "
                    + relativePath);
        }
        try {
            Files.createDirectories(parent);
            restrictDirectory(parent);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot create workspace-owned file directory "
                    + parent, e);
        }
        writeAtomicFile(parent, destination, content);
    }

    public void deleteOwnedFile(WorkspaceLock lock, String relativePath)
            throws WorkspaceException {
        requireLock(lock);
        Path path = resolveInside(relativePath);
        try {
            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("Refusing to delete workspace directory as a file: "
                        + path);
            }
            if (Files.deleteIfExists(path)) {
                fsyncDirectory(path.getParent());
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot delete workspace-owned file " + path, e);
        }
    }

    public Path root() {
        return root;
    }

    private void createLayout() throws WorkspaceException {
        for (String directory : LAYOUT_DIRECTORIES) {
            Path path = resolveInside(directory);
            try {
                Files.createDirectory(path);
                restrictDirectory(path);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
                        || Files.isSymbolicLink(path)) {
                    throw new WorkspaceException("Workspace layout path is unsafe: " + path, e);
                }
            } catch (IOException e) {
                throw new WorkspaceException("Cannot create workspace layout directory " + path,
                        e);
            }
        }
    }

    private void persist(WorkspaceManifest manifest) throws WorkspaceException {
        byte[] encoded = codec.encode(manifest);
        writeAtomicFile(root, manifestPath, encoded);
    }

    private void writeAtomicFile(Path parent, Path destination, byte[] encoded)
            throws WorkspaceException {
        String temporaryName = "." + destination.getFileName() + "." + UUID.randomUUID()
                + ".tmp";
        Path temporary = WorkspacePaths.resolveInside(parent, temporaryName);
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                restrictFile(temporary);
                writeFully(channel, ByteBuffer.wrap(encoded));
                channel.force(true);
            }
            try {
                Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                throw new WorkspaceException("Workspace filesystem does not support atomic "
                        + "replacement in " + parent, e);
            }
            fsyncDirectory(parent);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot persist workspace-owned file " + destination,
                    e);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException ignored) {
                // A named temp file is never treated as committed state and is safe to recover later.
            }
        }
    }

    private static boolean isValidSuccessor(WorkspaceManifest current,
                                            WorkspaceManifest next) {
        if (next.updatedAt().isBefore(current.updatedAt())) {
            return false;
        }
        if (current.equals(next)) {
            return true;
        }
        if (current.state() == next.state()) {
            return current.createdAt().equals(next.createdAt());
        }
        if (next.state() == WorkspaceState.FAILED_RECOVERABLE) {
            return next.lastStableState() == current.state();
        }
        if (current.state() == WorkspaceState.FAILED_RECOVERABLE) {
            return next.state() == current.lastStableState()
                    || (next.state() == WorkspaceState.STOPPED
                    && current.lastStableState().canTransitionTo(WorkspaceState.STOPPED));
        }
        return current.state().canTransitionTo(next.state());
    }

    private void requireLock(WorkspaceLock lock) throws WorkspaceException {
        if (lock == null || !lock.isHeldFor(root)) {
            throw new WorkspaceException("An active exclusive workspace lock is required");
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void fsyncDirectory(Path directory) throws WorkspaceException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot fsync workspace directory " + directory, e);
        }
    }

    private static void closeAfterFailedLock(FileChannel channel) throws WorkspaceException {
        try {
            channel.close();
        } catch (IOException e) {
            throw new WorkspaceException("Cannot close failed workspace lock channel", e);
        }
    }

    private static void restrictDirectory(Path path) throws IOException {
        setPermissions(path, PosixFilePermissions.fromString("rwx------"));
    }

    private static void restrictFile(Path path) throws IOException {
        setPermissions(path, PosixFilePermissions.fromString("rw-------"));
    }

    private static void setPermissions(Path path, Set<PosixFilePermission> permissions)
            throws IOException {
        try {
            Files.setPosixFilePermissions(path, permissions);
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX filesystems still rely on the current user's workspace ACLs.
        }
    }
}
