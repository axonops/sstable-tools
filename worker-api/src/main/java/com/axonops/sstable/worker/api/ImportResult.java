package com.axonops.sstable.worker.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Strict atomic handoff from a release worker to the Cassandra-free controller. */
public final class ImportResult {
    public static final String WORKSPACE_PATH = "state/import-result.properties";
    private static final long MAX_BYTES = 64L * 1024L;
    private static final Set<String> FIELDS = new HashSet<>(Arrays.asList(
            "protocol", "workspace.id", "release", "keyspace", "table", "table.id",
            "partitioner", "table.directory", "source.sets", "live.sstables",
            "logical.rows", "source.max-timestamp-micros", "auto.compaction.disabled",
            "native.transport.started"));

    private final int protocol;
    private final UUID workspaceId;
    private final String release;
    private final String keyspace;
    private final String table;
    private final UUID tableId;
    private final String partitioner;
    private final String tableDirectory;
    private final int sourceSets;
    private final int liveSstables;
    private final long logicalRows;
    private final long sourceMaxTimestampMicros;
    private final boolean autoCompactionDisabled;
    private final boolean nativeTransportStarted;

    public ImportResult(int protocol,
                        UUID workspaceId,
                        String release,
                        String keyspace,
                        String table,
                        UUID tableId,
                        String partitioner,
                        String tableDirectory,
                        int sourceSets,
                        int liveSstables,
                        long logicalRows,
                        long sourceMaxTimestampMicros,
                        boolean autoCompactionDisabled,
                        boolean nativeTransportStarted) {
        Path relative;
        try {
            relative = Paths.get(tableDirectory == null ? "" : tableDirectory);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid import table directory", e);
        }
        if (protocol != WorkerProtocol.CURRENT_VERSION || workspaceId == null
                || isBlank(release) || isBlank(keyspace) || isBlank(table) || tableId == null
                || isBlank(partitioner) || relative.isAbsolute() || relative.getNameCount() < 3
                || !"data".equals(relative.getName(0).toString())
                || !relative.equals(relative.normalize()) || tableDirectory.indexOf('\\') >= 0
                || sourceSets < 1 || liveSstables < sourceSets || logicalRows < 0
                || !autoCompactionDisabled || nativeTransportStarted) {
            throw new IllegalArgumentException("Invalid import result");
        }
        this.protocol = protocol;
        this.workspaceId = workspaceId;
        this.release = release;
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.partitioner = partitioner;
        this.tableDirectory = tableDirectory;
        this.sourceSets = sourceSets;
        this.liveSstables = liveSstables;
        this.logicalRows = logicalRows;
        this.sourceMaxTimestampMicros = sourceMaxTimestampMicros;
        this.autoCompactionDisabled = autoCompactionDisabled;
        this.nativeTransportStarted = nativeTransportStarted;
    }

    public void writeAtomically(Path path) throws IOException {
        byte[] encoded = encode();
        Path parent = path.getParent();
        if (parent == null || Files.isSymbolicLink(parent)) {
            throw new IOException("Unsafe import result directory " + parent);
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
                throw new IOException("Import result requires atomic replacement", e);
            }
            try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    public static ImportResult read(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(path)) {
            throw new IOException("Import result is not a regular file: " + path);
        }
        long size = Files.size(path);
        if (size <= 0 || size > MAX_BYTES) {
            throw new IOException("Import result has invalid size: " + size);
        }
        Properties values = new Properties();
        values.load(new ByteArrayInputStream(Files.readAllBytes(path)));
        if (!FIELDS.equals(values.stringPropertyNames())) {
            throw new IOException("Import result fields are missing or unknown");
        }
        try {
            return new ImportResult(integer(values, "protocol"),
                    UUID.fromString(required(values, "workspace.id")),
                    required(values, "release"), required(values, "keyspace"),
                    required(values, "table"), UUID.fromString(required(values, "table.id")),
                    required(values, "partitioner"), required(values, "table.directory"),
                    integer(values, "source.sets"), integer(values, "live.sstables"),
                    Long.parseLong(required(values, "logical.rows")),
                    Long.parseLong(required(values, "source.max-timestamp-micros")),
                    bool(values, "auto.compaction.disabled"),
                    bool(values, "native.transport.started"));
        } catch (IllegalArgumentException e) {
            throw new IOException("Import result contains invalid values", e);
        }
    }

    private byte[] encode() throws IOException {
        Properties values = new Properties();
        values.setProperty("protocol", Integer.toString(protocol));
        values.setProperty("workspace.id", workspaceId.toString());
        values.setProperty("release", release);
        values.setProperty("keyspace", keyspace);
        values.setProperty("table", table);
        values.setProperty("table.id", tableId.toString());
        values.setProperty("partitioner", partitioner);
        values.setProperty("table.directory", tableDirectory);
        values.setProperty("source.sets", Integer.toString(sourceSets));
        values.setProperty("live.sstables", Integer.toString(liveSstables));
        values.setProperty("logical.rows", Long.toString(logicalRows));
        values.setProperty("source.max-timestamp-micros",
                Long.toString(sourceMaxTimestampMicros));
        values.setProperty("auto.compaction.disabled",
                Boolean.toString(autoCompactionDisabled));
        values.setProperty("native.transport.started", Boolean.toString(nativeTransportStarted));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        values.store(output, "sstable-tools import result");
        return output.toByteArray();
    }

    private static String required(Properties values, String name) throws IOException {
        String value = values.getProperty(name);
        if (isBlank(value)) {
            throw new IOException("Missing import result field " + name);
        }
        return value;
    }

    private static int integer(Properties values, String name) throws IOException {
        try {
            return Integer.parseInt(required(values, name));
        } catch (NumberFormatException e) {
            throw new IOException("Import result field " + name + " must be an integer", e);
        }
    }

    private static boolean bool(Properties values, String name) throws IOException {
        String value = required(values, name);
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new IOException("Import result field " + name + " must be a boolean");
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void restrictFile(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The workspace ACL remains the boundary on non-POSIX filesystems.
        }
    }

    public int protocol() {
        return protocol;
    }

    public UUID workspaceId() {
        return workspaceId;
    }

    public String release() {
        return release;
    }

    public String keyspace() {
        return keyspace;
    }

    public String table() {
        return table;
    }

    public UUID tableId() {
        return tableId;
    }

    public String partitioner() {
        return partitioner;
    }

    public String tableDirectory() {
        return tableDirectory;
    }

    public int sourceSets() {
        return sourceSets;
    }

    public int liveSstables() {
        return liveSstables;
    }

    public long logicalRows() {
        return logicalRows;
    }

    public long sourceMaxTimestampMicros() {
        return sourceMaxTimestampMicros;
    }

    public boolean autoCompactionDisabled() {
        return autoCompactionDisabled;
    }

    public boolean nativeTransportStarted() {
        return nativeTransportStarted;
    }
}
