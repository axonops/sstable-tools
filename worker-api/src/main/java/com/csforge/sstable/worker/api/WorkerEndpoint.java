package com.csforge.sstable.worker.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Strict worker-owned endpoint and process health record. */
public final class WorkerEndpoint {
    private static final long MAX_BYTES = 64L * 1024L;
    private static final Set<String> FIELDS = new HashSet<>(Arrays.asList(
            "protocol", "workspace.id", "status", "pid", "release",
            "native.address", "native.port", "control.address", "control.port",
            "started.at", "updated.at", "message"));

    public enum Status {
        RUNNING,
        STOPPING,
        STOPPED,
        FAILED
    }

    private final int protocol;
    private final UUID workspaceId;
    private final Status status;
    private final long pid;
    private final String release;
    private final String nativeAddress;
    private final int nativePort;
    private final String controlAddress;
    private final int controlPort;
    private final Instant startedAt;
    private final Instant updatedAt;
    private final String message;

    public WorkerEndpoint(int protocol,
                          UUID workspaceId,
                          Status status,
                          long pid,
                          String release,
                          String nativeAddress,
                          int nativePort,
                          String controlAddress,
                          int controlPort,
                          Instant startedAt,
                          Instant updatedAt,
                          String message) {
        if (protocol != WorkerProtocol.CURRENT_VERSION || workspaceId == null
                || status == null || pid < -1 || isBlank(release)
                || isBlank(nativeAddress) || !"127.0.0.1".equals(nativeAddress)
                || nativePort < 1 || nativePort > 65535
                || isBlank(controlAddress) || !"127.0.0.1".equals(controlAddress)
                || controlPort < 1 || controlPort > 65535 || startedAt == null
                || updatedAt == null || updatedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("Invalid worker endpoint record");
        }
        this.protocol = protocol;
        this.workspaceId = workspaceId;
        this.status = status;
        this.pid = pid;
        this.release = release;
        this.nativeAddress = nativeAddress;
        this.nativePort = nativePort;
        this.controlAddress = controlAddress;
        this.controlPort = controlPort;
        this.startedAt = startedAt;
        this.updatedAt = updatedAt;
        this.message = message == null ? "" : singleLine(message);
    }

    public WorkerEndpoint withStatus(Status next, String nextMessage) {
        return new WorkerEndpoint(protocol, workspaceId, next, pid, release,
                nativeAddress, nativePort, controlAddress, controlPort,
                startedAt, Instant.now(), nextMessage);
    }

    public void writeAtomically(Path path) throws IOException {
        byte[] encoded = encode();
        Path parent = path.getParent();
        if (parent == null || Files.isSymbolicLink(parent)) {
            throw new IOException("Unsafe worker endpoint directory " + parent);
        }
        Files.createDirectories(parent);
        Path temporary = parent.resolve("." + path.getFileName() + "." + UUID.randomUUID()
                + ".tmp");
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
                throw new IOException("Worker endpoint requires atomic replacement", e);
            }
            try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    public static WorkerEndpoint read(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(path)) {
            throw new IOException("Worker endpoint is not a regular file: " + path);
        }
        long size = Files.size(path);
        if (size <= 0 || size > MAX_BYTES) {
            throw new IOException("Worker endpoint has invalid size: " + size);
        }
        Properties values = new Properties();
        values.load(new ByteArrayInputStream(Files.readAllBytes(path)));
        if (!FIELDS.equals(values.stringPropertyNames())) {
            throw new IOException("Worker endpoint fields are missing or unknown");
        }
        try {
            return new WorkerEndpoint(
                    integer(values, "protocol"),
                    UUID.fromString(required(values, "workspace.id")),
                    Status.valueOf(required(values, "status")),
                    Long.parseLong(required(values, "pid")),
                    required(values, "release"),
                    required(values, "native.address"),
                    integer(values, "native.port"),
                    required(values, "control.address"),
                    integer(values, "control.port"),
                    Instant.parse(required(values, "started.at")),
                    Instant.parse(required(values, "updated.at")),
                    values.getProperty("message", ""));
        } catch (IllegalArgumentException e) {
            throw new IOException("Worker endpoint contains invalid values", e);
        }
    }

    private byte[] encode() throws IOException {
        Properties values = new Properties();
        values.setProperty("protocol", Integer.toString(protocol));
        values.setProperty("workspace.id", workspaceId.toString());
        values.setProperty("status", status.name());
        values.setProperty("pid", Long.toString(pid));
        values.setProperty("release", release);
        values.setProperty("native.address", nativeAddress);
        values.setProperty("native.port", Integer.toString(nativePort));
        values.setProperty("control.address", controlAddress);
        values.setProperty("control.port", Integer.toString(controlPort));
        values.setProperty("started.at", startedAt.toString());
        values.setProperty("updated.at", updatedAt.toString());
        values.setProperty("message", message);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        values.store(output, "sstable-tools worker endpoint");
        return output.toByteArray();
    }

    private static String required(Properties values, String name) throws IOException {
        String value = values.getProperty(name);
        if (isBlank(value)) {
            throw new IOException("Missing worker endpoint field " + name);
        }
        return value;
    }

    private static int integer(Properties values, String name) throws IOException {
        try {
            return Integer.parseInt(required(values, name));
        } catch (NumberFormatException e) {
            throw new IOException("Worker endpoint field " + name + " must be an integer", e);
        }
    }

    private static void restrictFile(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The workspace ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String singleLine(String value) {
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    public int protocol() {
        return protocol;
    }

    public UUID workspaceId() {
        return workspaceId;
    }

    public Status status() {
        return status;
    }

    public long pid() {
        return pid;
    }

    public String release() {
        return release;
    }

    public String nativeAddress() {
        return nativeAddress;
    }

    public int nativePort() {
        return nativePort;
    }

    public String controlAddress() {
        return controlAddress;
    }

    public int controlPort() {
        return controlPort;
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public String message() {
        return message;
    }
}
