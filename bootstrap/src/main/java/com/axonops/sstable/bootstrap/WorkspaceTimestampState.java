package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.WorkspaceException;
import com.axonops.sstable.workspace.WorkspaceLock;
import com.axonops.sstable.workspace.WorkspaceRepository;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Initializes and validates the durable after-source timestamp high-water mark. */
final class WorkspaceTimestampState {
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

    private WorkspaceTimestampState() {
    }

    static void prepare(WorkspaceRepository repository,
                        WorkspaceLock lock,
                        UUID workspaceId,
                        TimestampPolicy policy,
                        long sourceMaximumMicros,
                        boolean mayInitialize) throws WorkspaceException {
        Path path = repository.resolveInside(WORKSPACE_PATH);
        if (policy == TimestampPolicy.WALL_CLOCK) {
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                if (!mayInitialize) {
                    throw new WorkspaceException("Workspace timestamp state is not allowed for "
                            + "the persisted wall-clock policy");
                }
                repository.deleteOwnedFile(lock, WORKSPACE_PATH);
            }
            return;
        }
        if (sourceMaximumMicros == Long.MAX_VALUE) {
            throw new WorkspaceException("after-source cannot allocate a timestamp above "
                    + "Long.MAX_VALUE source metadata");
        }

        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            validate(read(path), workspaceId, policy, sourceMaximumMicros);
            return;
        }
        if (!mayInitialize) {
            throw new WorkspaceException("Workspace timestamp state is missing for the "
                    + "persisted after-source policy");
        }
        repository.writeOwnedFile(lock, WORKSPACE_PATH,
                encode(workspaceId, policy.value(), sourceMaximumMicros,
                        sourceMaximumMicros));
    }

    static Long validatedHighWater(WorkspaceRepository repository,
                                   UUID workspaceId,
                                   TimestampPolicy policy,
                                   long sourceMaximumMicros)
            throws WorkspaceException {
        Path path = repository.resolveInside(WORKSPACE_PATH);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            if (policy == TimestampPolicy.AFTER_SOURCE) {
                throw new WorkspaceException("Workspace timestamp state is missing for the "
                        + "persisted after-source policy");
            }
            return null;
        }
        if (policy == TimestampPolicy.WALL_CLOCK) {
            throw new WorkspaceException("Workspace timestamp state is not allowed for the "
                    + "persisted wall-clock policy");
        }
        State state = read(path);
        validate(state, workspaceId, policy, sourceMaximumMicros);
        return state.highWaterMicros;
    }

    private static State read(Path path) throws WorkspaceException {
        try {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(path)
                    || Files.size(path) <= 0 || Files.size(path) > 4096) {
                throw new WorkspaceException("Workspace timestamp state is missing or unsafe: "
                        + path);
            }
            requireOwnerOnly(path);
            Properties values = new Properties();
            values.load(new ByteArrayInputStream(Files.readAllBytes(path)));
            if (!FIELDS.equals(values.stringPropertyNames())
                    || !"1".equals(values.getProperty("version"))) {
                throw new WorkspaceException("Workspace timestamp state fields are invalid");
            }
            return new State(UUID.fromString(values.getProperty("workspace.id")),
                    values.getProperty("policy"),
                    Long.parseLong(values.getProperty("source.max-timestamp-micros")),
                    Long.parseLong(values.getProperty("high-water-micros")));
        } catch (IOException | IllegalArgumentException e) {
            throw new WorkspaceException("Cannot read workspace timestamp state", e);
        }
    }

    private static void validate(State state,
                                 UUID workspaceId,
                                 TimestampPolicy policy,
                                 long sourceMaximumMicros) throws WorkspaceException {
        if (!workspaceId.equals(state.workspaceId)
                || !policy.value().equals(state.policy)
                || sourceMaximumMicros != state.sourceMaximumMicros
                || state.highWaterMicros < sourceMaximumMicros) {
            throw new WorkspaceException("Workspace timestamp state does not match the "
                    + "persisted workspace policy and source bound");
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

    private static byte[] encode(UUID workspaceId,
                                 String policy,
                                 long sourceMaximumMicros,
                                 long highWaterMicros) {
        String content = "version=1\n"
                + "workspace.id=" + workspaceId + "\n"
                + "policy=" + policy + "\n"
                + "source.max-timestamp-micros=" + sourceMaximumMicros + "\n"
                + "high-water-micros=" + highWaterMicros + "\n";
        return content.getBytes(StandardCharsets.US_ASCII);
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
