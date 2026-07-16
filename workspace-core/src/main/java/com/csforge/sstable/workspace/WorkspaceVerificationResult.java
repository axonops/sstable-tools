package com.csforge.sstable.workspace;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Durable Cassandra verification evidence bound to one flush inventory. */
public final class WorkspaceVerificationResult {
    public static final String WORKSPACE_PATH = "state/verification-result.properties";
    private static final long MAX_BYTES = 64L * 1024L;
    private static final Set<String> FIELDS = new HashSet<>(Arrays.asList(
            "version", "workspace.id", "release", "keyspace", "table",
            "flush.sha256", "verified.at", "live.sstables", "delta.sstables",
            "logical.rows", "formats", "delta.descriptors"));
    private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS = EnumSet.of(
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ,
            PosixFilePermission.OTHERS_WRITE,
            PosixFilePermission.OTHERS_EXECUTE);

    private final UUID workspaceId;
    private final String release;
    private final String keyspace;
    private final String table;
    private final String flushSha256;
    private final Instant verifiedAt;
    private final int liveSstables;
    private final int deltaSstables;
    private final long logicalRows;
    private final List<String> formats;
    private final List<String> deltaDescriptors;

    public WorkspaceVerificationResult(UUID workspaceId,
                                       String release,
                                       String keyspace,
                                       String table,
                                       String flushSha256,
                                       Instant verifiedAt,
                                       int liveSstables,
                                       int deltaSstables,
                                       long logicalRows,
                                       List<String> formats,
                                       List<String> deltaDescriptors)
            throws WorkspaceException {
        if (workspaceId == null || !safeName(release) || !safeName(keyspace)
                || !safeName(table) || flushSha256 == null
                || !flushSha256.matches("[0-9a-f]{64}") || verifiedAt == null
                || liveSstables < 1 || deltaSstables < 0 || deltaSstables > liveSstables
                || logicalRows < 0 || formats == null || deltaDescriptors == null
                || deltaSstables != deltaDescriptors.size()) {
            throw new WorkspaceException("Verification result contains invalid fields");
        }
        this.formats = sortedTokens(formats, "SSTable format");
        this.deltaDescriptors = sortedTokens(deltaDescriptors, "delta descriptor");
        if (deltaSstables > 0 && this.formats.isEmpty()) {
            throw new WorkspaceException("Generated SSTables require a format version");
        }
        this.workspaceId = workspaceId;
        this.release = release;
        this.keyspace = keyspace;
        this.table = table;
        this.flushSha256 = flushSha256;
        this.verifiedAt = verifiedAt;
        this.liveSstables = liveSstables;
        this.deltaSstables = deltaSstables;
        this.logicalRows = logicalRows;
    }

    public void writeAtomically(Path workspaceRoot) throws WorkspaceException {
        Path path = workspaceRoot.toAbsolutePath().normalize().resolve(WORKSPACE_PATH);
        try {
            Path root = workspaceRoot.toRealPath();
            path = WorkspacePaths.resolveInside(root, WORKSPACE_PATH);
            Path parent = path.getParent();
            if (parent == null || Files.isSymbolicLink(parent)
                    || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)
                    || !parent.toRealPath().startsWith(root)) {
                throw new WorkspaceException("Verification result directory is unsafe: "
                        + parent);
            }
            byte[] encoded = encode();
            if (encoded.length == 0 || encoded.length > MAX_BYTES) {
                throw new WorkspaceException("Verification result is too large");
            }
            writeAtomicFile(parent, path, encoded);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot persist workspace verification result "
                    + path, e);
        }
    }

    public static WorkspaceVerificationResult read(Path workspaceRoot)
            throws WorkspaceException {
        Path path = workspaceRoot.toAbsolutePath().normalize().resolve(WORKSPACE_PATH);
        try {
            Path root = workspaceRoot.toRealPath();
            path = WorkspacePaths.resolveInside(root, WORKSPACE_PATH);
            if (Files.isSymbolicLink(path)
                    || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("Workspace verification result is missing or "
                        + "unsafe: " + path);
            }
            long size = Files.size(path);
            if (size <= 0 || size > MAX_BYTES) {
                throw new WorkspaceException("Workspace verification result has invalid size: "
                        + size);
            }
            requireOwnerOnly(path);
            Properties values = new Properties();
            values.load(new ByteArrayInputStream(Files.readAllBytes(path)));
            if (!FIELDS.equals(values.stringPropertyNames())
                    || !"1".equals(values.getProperty("version"))) {
                throw new WorkspaceException("Workspace verification result fields are invalid");
            }
            return new WorkspaceVerificationResult(
                    UUID.fromString(required(values, "workspace.id")),
                    required(values, "release"),
                    required(values, "keyspace"),
                    required(values, "table"),
                    required(values, "flush.sha256"),
                    Instant.parse(required(values, "verified.at")),
                    integer(values, "live.sstables"),
                    integer(values, "delta.sstables"),
                    longValue(values, "logical.rows"),
                    tokens(values.getProperty("formats")),
                    tokens(values.getProperty("delta.descriptors")));
        } catch (IOException | IllegalArgumentException e) {
            throw new WorkspaceException("Cannot read workspace verification result "
                    + path, e);
        }
    }

    public void requireIdentity(UUID expectedWorkspaceId,
                                String expectedRelease,
                                String expectedKeyspace,
                                String expectedTable,
                                String expectedFlushSha256) throws WorkspaceException {
        if (!workspaceId.equals(expectedWorkspaceId) || !release.equals(expectedRelease)
                || !keyspace.equals(expectedKeyspace) || !table.equals(expectedTable)
                || !flushSha256.equals(expectedFlushSha256)) {
            throw new WorkspaceException("Workspace verification result identity does not "
                    + "match the committed flush");
        }
    }

    private byte[] encode() {
        String content = "version=1\n"
                + "workspace.id=" + workspaceId + "\n"
                + "release=" + release + "\n"
                + "keyspace=" + keyspace + "\n"
                + "table=" + table + "\n"
                + "flush.sha256=" + flushSha256 + "\n"
                + "verified.at=" + verifiedAt + "\n"
                + "live.sstables=" + liveSstables + "\n"
                + "delta.sstables=" + deltaSstables + "\n"
                + "logical.rows=" + logicalRows + "\n"
                + "formats=" + String.join(",", formats) + "\n"
                + "delta.descriptors=" + String.join(",", deltaDescriptors) + "\n";
        return content.getBytes(StandardCharsets.US_ASCII);
    }

    private static List<String> sortedTokens(List<String> values, String description)
            throws WorkspaceException {
        List<String> sorted = new ArrayList<>(values);
        Set<String> unique = new HashSet<>();
        for (String value : sorted) {
            if (!safeToken(value) || !unique.add(value)) {
                throw new WorkspaceException("Invalid or duplicate " + description + " "
                        + value);
            }
        }
        Collections.sort(sorted);
        return Collections.unmodifiableList(sorted);
    }

    private static boolean safeName(String value) {
        return value != null && !value.trim().isEmpty()
                && value.indexOf('\n') < 0 && value.indexOf('\r') < 0
                && value.indexOf('=') < 0;
    }

    private static boolean safeToken(String value) {
        return safeName(value) && value.indexOf(',') < 0;
    }

    private static List<String> tokens(String value) {
        return value == null || value.isEmpty()
                ? Collections.<String>emptyList() : Arrays.asList(value.split(",", -1));
    }

    private static String required(Properties values, String name) throws IOException {
        String value = values.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IOException("Missing verification result field " + name);
        }
        return value;
    }

    private static int integer(Properties values, String name) throws IOException {
        long value = longValue(values, name);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IOException("Verification result field is outside integer range " + name);
        }
        return (int) value;
    }

    private static long longValue(Properties values, String name) throws IOException {
        try {
            return Long.parseLong(required(values, name));
        } catch (NumberFormatException e) {
            throw new IOException("Verification result field must be an integer " + name, e);
        }
    }

    private static void writeAtomicFile(Path parent, Path destination, byte[] bytes)
            throws IOException {
        Path temporary = parent.resolve("." + destination.getFileName() + "."
                + UUID.randomUUID() + ".tmp");
        try {
            try (FileChannel channel = FileChannel.open(temporary,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                restrictFile(temporary);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                throw new IOException("Verification result requires atomic replacement", e);
            }
            try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static void requireOwnerOnly(Path path) throws IOException {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            if (!permissions.contains(PosixFilePermission.OWNER_READ)
                    || !Collections.disjoint(permissions, FORBIDDEN_PERMISSIONS)) {
                throw new IOException("Workspace verification result must be owner-only: "
                        + path);
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

    public Instant verifiedAt() {
        return verifiedAt;
    }

    public int liveSstables() {
        return liveSstables;
    }

    public int deltaSstables() {
        return deltaSstables;
    }

    public long logicalRows() {
        return logicalRows;
    }

    public List<String> formats() {
        return formats;
    }

    public List<String> deltaDescriptors() {
        return deltaDescriptors;
    }
}
