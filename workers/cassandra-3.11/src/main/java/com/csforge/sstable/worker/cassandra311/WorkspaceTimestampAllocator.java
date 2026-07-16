package com.csforge.sstable.worker.cassandra311;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.exceptions.InvalidRequestException;

/** Durable monotonic timestamp source for the after-source mutation policy. */
final class WorkspaceTimestampAllocator {
    static final String WORKSPACE_PATH = "state/timestamp.properties";
    private static final Set<String> FIELDS = new HashSet<>(Arrays.asList(
            "version", "workspace.id", "policy", "source.max-timestamp-micros",
            "high-water-micros"));
    private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS = EnumSet.of(
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ,
            PosixFilePermission.OTHERS_WRITE,
            PosixFilePermission.OTHERS_EXECUTE);

    private final Path workspace;
    private final Path path;
    private final UUID workspaceId;
    private final long sourceMaximumMicros;
    private long highWaterMicros;

    static WorkspaceTimestampAllocator open(Path workspace,
                                            UUID workspaceId,
                                            long sourceMaximumMicros) {
        try {
            return new WorkspaceTimestampAllocator(workspace, workspaceId,
                    sourceMaximumMicros);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load workspace timestamp state", e);
        }
    }

    private WorkspaceTimestampAllocator(Path workspace,
                                        UUID workspaceId,
                                        long sourceMaximumMicros) throws IOException {
        this.workspace = workspace.toRealPath();
        this.path = this.workspace.resolve(WORKSPACE_PATH).normalize();
        this.workspaceId = workspaceId;
        this.sourceMaximumMicros = sourceMaximumMicros;
        if (!path.startsWith(this.workspace) || path.equals(this.workspace)
                || Files.isSymbolicLink(path)
                || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || !path.toRealPath().equals(path)) {
            throw new IOException("Workspace timestamp state is missing or unsafe: " + path);
        }
        requireOwnerOnly(path);
        State state = read(path);
        if (!workspaceId.equals(state.workspaceId)
                || !"after-source".equals(state.policy)
                || sourceMaximumMicros != state.sourceMaximumMicros
                || state.highWaterMicros < sourceMaximumMicros) {
            throw new IOException("Workspace timestamp state does not match worker inputs");
        }
        highWaterMicros = state.highWaterMicros;
    }

    synchronized long next() throws InvalidRequestException {
        if (highWaterMicros == Long.MAX_VALUE) {
            throw new InvalidRequestException(WorkspaceQueryHandler.POLICY_PREFIX
                    + "after-source timestamp space is exhausted");
        }
        long wallClockMicros = System.currentTimeMillis() * 1000L;
        long next = Math.max(highWaterMicros + 1L, wallClockMicros);
        try {
            persist(next);
        } catch (IOException e) {
            throw new InvalidRequestException(WorkspaceQueryHandler.POLICY_PREFIX
                    + "cannot persist the after-source timestamp high-water mark: "
                    + e.getMessage());
        }
        highWaterMicros = next;
        return next;
    }

    synchronized long highWaterMicros() {
        return highWaterMicros;
    }

    private void persist(long next) throws IOException {
        Path parent = path.getParent();
        Path temporary = parent.resolve("." + path.getFileName() + "." + UUID.randomUUID()
                + ".tmp");
        byte[] encoded = encode(next);
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                restrictFile(temporary);
                ByteBuffer bytes = ByteBuffer.wrap(encoded);
                while (bytes.hasRemaining()) {
                    channel.write(bytes);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                throw new IOException("Timestamp state requires atomic replacement", e);
            }
            try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private byte[] encode(long highWater) {
        String content = "version=1\n"
                + "workspace.id=" + workspaceId + "\n"
                + "policy=after-source\n"
                + "source.max-timestamp-micros=" + sourceMaximumMicros + "\n"
                + "high-water-micros=" + highWater + "\n";
        return content.getBytes(StandardCharsets.US_ASCII);
    }

    private static State read(Path path) throws IOException {
        long size = Files.size(path);
        if (size <= 0 || size > 4096) {
            throw new IOException("Workspace timestamp state has invalid size: " + size);
        }
        Properties values = new Properties();
        values.load(new ByteArrayInputStream(Files.readAllBytes(path)));
        if (!FIELDS.equals(values.stringPropertyNames())
                || !"1".equals(values.getProperty("version"))) {
            throw new IOException("Workspace timestamp state fields are invalid");
        }
        try {
            return new State(UUID.fromString(values.getProperty("workspace.id")),
                    values.getProperty("policy"),
                    Long.parseLong(values.getProperty("source.max-timestamp-micros")),
                    Long.parseLong(values.getProperty("high-water-micros")));
        } catch (IllegalArgumentException e) {
            throw new IOException("Workspace timestamp state values are invalid", e);
        }
    }

    private static void requireOwnerOnly(Path path) throws IOException {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            if (!permissions.contains(PosixFilePermission.OWNER_READ)
                    || !Collections.disjoint(permissions, FORBIDDEN_PERMISSIONS)) {
                throw new IOException("Workspace timestamp state must be owner-only: " + path);
            }
        } catch (UnsupportedOperationException ignored) {
            // The workspace ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static void restrictFile(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The workspace ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static final class State {
        private final UUID workspaceId;
        private final String policy;
        private final long sourceMaximumMicros;
        private final long highWaterMicros;

        private State(UUID workspaceId,
                      String policy,
                      long sourceMaximumMicros,
                      long highWaterMicros) {
            this.workspaceId = workspaceId;
            this.policy = policy;
            this.sourceMaximumMicros = sourceMaximumMicros;
            this.highWaterMicros = highWaterMicros;
        }
    }
}
