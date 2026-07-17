package com.axonops.sstable.workspace;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Durable, worker-produced inventory for one quiesced table flush. */
public final class WorkspaceFlushResult {
    public static final String WORKSPACE_PATH = "state/flush-result.json";
    private static final int FORMAT_VERSION = 1;
    private static final long MAX_BYTES = 16L * 1024L * 1024L;
    private static final Gson WRITER = new GsonBuilder()
            .disableHtmlEscaping()
            .setPrettyPrinting()
            .create();
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
    private final String tableDirectory;
    private final Instant flushedAt;
    private final List<ManifestFile> files;

    public WorkspaceFlushResult(UUID workspaceId,
                                String release,
                                String keyspace,
                                String table,
                                String tableDirectory,
                                Instant flushedAt,
                                List<ManifestFile> files) throws WorkspaceException {
        if (workspaceId == null || invalidName(release) || invalidName(keyspace)
                || invalidName(table) || flushedAt == null || files == null
                || files.isEmpty()) {
            throw new WorkspaceException("Flush result contains invalid required fields");
        }
        String directory = validateRelativeDirectory(tableDirectory);
        List<ManifestFile> sorted = new ArrayList<>(files);
        sorted.sort((left, right) -> left.relativePath().compareTo(right.relativePath()));
        Set<String> paths = new HashSet<>();
        for (ManifestFile file : sorted) {
            if (file == null || !paths.add(file.relativePath())
                    || !file.relativePath().startsWith(directory + "/")) {
                throw new WorkspaceException("Flush result contains an invalid table file");
            }
        }
        this.workspaceId = workspaceId;
        this.release = release;
        this.keyspace = keyspace;
        this.table = table;
        this.tableDirectory = directory;
        this.flushedAt = flushedAt;
        this.files = Collections.unmodifiableList(sorted);
    }

    public static WorkspaceFlushResult capture(Path workspaceRoot,
                                               UUID workspaceId,
                                               String release,
                                               String keyspace,
                                               String table,
                                               String tableDirectory)
            throws WorkspaceException {
        return new WorkspaceFlushResult(workspaceId, release, keyspace, table,
                tableDirectory, Instant.now(),
                WorkspaceFileInventory.capture(workspaceRoot, tableDirectory));
    }

    public void writeAtomically(Path workspaceRoot) throws WorkspaceException {
        final Path root;
        final Path path;
        try {
            root = workspaceRoot.toRealPath();
            path = WorkspacePaths.resolveInside(root, WORKSPACE_PATH);
            Path parent = path.getParent();
            if (parent == null || Files.isSymbolicLink(parent)
                    || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)
                    || !parent.toRealPath().startsWith(root)) {
                throw new WorkspaceException("Flush result directory is missing or unsafe: "
                        + parent);
            }
            byte[] encoded = encode();
            if (encoded.length == 0 || encoded.length > MAX_BYTES) {
                throw new WorkspaceException("Workspace flush result is too large: "
                        + encoded.length);
            }
            writeAtomicFile(parent, path, encoded);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot persist workspace flush result", e);
        }
    }

    public static WorkspaceFlushResult read(Path workspaceRoot) throws WorkspaceException {
        Path path = workspaceRoot.toAbsolutePath().normalize().resolve(WORKSPACE_PATH);
        try {
            Path root = workspaceRoot.toRealPath();
            path = WorkspacePaths.resolveInside(root, WORKSPACE_PATH);
            if (Files.isSymbolicLink(path)
                    || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("Workspace flush result is missing or unsafe: "
                        + path);
            }
            long size = Files.size(path);
            if (size <= 0 || size > MAX_BYTES) {
                throw new WorkspaceException("Workspace flush result has invalid size: "
                        + size);
            }
            requireOwnerOnly(path);
            return decode(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read workspace flush result " + path, e);
        }
    }

    public void requireIdentity(UUID expectedWorkspaceId,
                                String expectedRelease,
                                String expectedKeyspace,
                                String expectedTable,
                                String expectedTableDirectory) throws WorkspaceException {
        if (!workspaceId.equals(expectedWorkspaceId) || !release.equals(expectedRelease)
                || !keyspace.equals(expectedKeyspace) || !table.equals(expectedTable)
                || !tableDirectory.equals(validateRelativeDirectory(expectedTableDirectory))) {
            throw new WorkspaceException("Workspace flush result identity does not match the "
                    + "running worker and imported table");
        }
    }

    public void verifyCompleteInventory(Path workspaceRoot) throws WorkspaceException {
        List<ManifestFile> current = WorkspaceFileInventory.capture(
                workspaceRoot, tableDirectory);
        if (!files.equals(current)) {
            throw new WorkspaceException("Workspace flushed table inventory changed");
        }
    }

    public List<ManifestFile> deltaFiles(List<ManifestFile> baseline)
            throws WorkspaceException {
        if (baseline == null || baseline.isEmpty()) {
            throw new WorkspaceException("Workspace baseline inventory is missing");
        }
        Map<String, ManifestFile> current = new TreeMap<>();
        for (ManifestFile file : files) {
            current.put(file.relativePath(), file);
        }
        Set<String> baselinePaths = new HashSet<>();
        for (ManifestFile file : baseline) {
            ManifestFile flushed = current.get(file.relativePath());
            if (!file.equals(flushed)) {
                throw new WorkspaceException("Workspace baseline is missing or changed after "
                        + "flush: " + file.relativePath());
            }
            baselinePaths.add(file.relativePath());
        }
        List<ManifestFile> delta = new ArrayList<>();
        for (ManifestFile file : files) {
            if (!baselinePaths.contains(file.relativePath())) {
                delta.add(file);
            }
        }
        return Collections.unmodifiableList(delta);
    }

    public String sha256() {
        return Hashing.sha256(encode());
    }

    private byte[] encode() {
        JsonObject root = new JsonObject();
        root.addProperty("formatVersion", FORMAT_VERSION);
        root.addProperty("workspaceId", workspaceId.toString());
        root.addProperty("release", release);
        root.addProperty("keyspace", keyspace);
        root.addProperty("table", table);
        root.addProperty("tableDirectory", tableDirectory);
        root.addProperty("flushedAt", flushedAt.toString());
        JsonArray fileValues = new JsonArray();
        for (ManifestFile file : files) {
            JsonObject value = new JsonObject();
            value.addProperty("relativePath", file.relativePath());
            value.addProperty("size", file.size());
            value.addProperty("sha256", file.sha256());
            fileValues.add(value);
        }
        root.add("files", fileValues);
        return (WRITER.toJson(root) + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static WorkspaceFlushResult decode(byte[] bytes) throws WorkspaceException {
        JsonElement parsed;
        try (Reader input = new StringReader(new String(bytes,
                java.nio.charset.StandardCharsets.UTF_8));
             JsonReader reader = new JsonReader(input)) {
            reader.setStrictness(Strictness.STRICT);
            parsed = JsonParser.parseReader(reader);
        } catch (IOException | JsonParseException | IllegalStateException e) {
            throw new WorkspaceException("Malformed workspace flush result JSON", e);
        }
        if (!parsed.isJsonObject()) {
            throw new WorkspaceException("Workspace flush result root must be an object");
        }
        JsonObject root = parsed.getAsJsonObject();
        requireOnly(root, "formatVersion", "workspaceId", "release", "keyspace", "table",
                "tableDirectory", "flushedAt", "files");
        if (requiredLong(root, "formatVersion") != FORMAT_VERSION) {
            throw new WorkspaceException("Unsupported workspace flush result format");
        }
        final UUID workspaceId;
        final Instant flushedAt;
        try {
            workspaceId = UUID.fromString(requiredString(root, "workspaceId"));
            flushedAt = Instant.parse(requiredString(root, "flushedAt"));
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new WorkspaceException("Workspace flush result identity is invalid", e);
        }
        JsonElement fileElement = root.get("files");
        if (fileElement == null || !fileElement.isJsonArray()
                || fileElement.getAsJsonArray().size() == 0) {
            throw new WorkspaceException("Workspace flush result files must be a non-empty array");
        }
        List<ManifestFile> files = new ArrayList<>();
        for (JsonElement element : fileElement.getAsJsonArray()) {
            if (!element.isJsonObject()) {
                throw new WorkspaceException("Workspace flush result file must be an object");
            }
            JsonObject file = element.getAsJsonObject();
            requireOnly(file, "relativePath", "size", "sha256");
            files.add(new ManifestFile(requiredString(file, "relativePath"),
                    requiredLong(file, "size"), requiredString(file, "sha256")));
        }
        return new WorkspaceFlushResult(workspaceId, requiredString(root, "release"),
                requiredString(root, "keyspace"), requiredString(root, "table"),
                requiredString(root, "tableDirectory"), flushedAt, files);
    }

    private static void requireOnly(JsonObject object, String... fields)
            throws WorkspaceException {
        Set<String> expected = new HashSet<>(Arrays.asList(fields));
        if (!expected.equals(object.keySet())) {
            throw new WorkspaceException("Workspace flush result fields are missing or unknown");
        }
    }

    private static String requiredString(JsonObject object, String name)
            throws WorkspaceException {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isString()
                || value.getAsString().trim().isEmpty()) {
            throw new WorkspaceException("Workspace flush result field " + name
                    + " must be a non-empty string");
        }
        return value.getAsString();
    }

    private static long requiredLong(JsonObject object, String name)
            throws WorkspaceException {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isNumber()) {
            throw new WorkspaceException("Workspace flush result field " + name
                    + " must be an integer");
        }
        try {
            String number = value.getAsString();
            if (number.contains(".") || number.contains("e") || number.contains("E")) {
                throw new NumberFormatException("not an integer");
            }
            return Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw new WorkspaceException("Workspace flush result field " + name
                    + " must be an integer", e);
        }
    }

    private static String validateRelativeDirectory(String value) throws WorkspaceException {
        final Path path;
        try {
            path = Paths.get(value == null ? "" : value);
        } catch (RuntimeException e) {
            throw new WorkspaceException("Unsafe flush table directory: " + value, e);
        }
        if (value == null || value.trim().isEmpty() || path.isAbsolute()
                || path.getNameCount() == 0 || !path.equals(path.normalize())
                || value.indexOf('\\') >= 0 || value.matches("^[A-Za-z]:.*")) {
            throw new WorkspaceException("Unsafe flush table directory: " + value);
        }
        for (Path segment : path) {
            if (".".equals(segment.toString()) || "..".equals(segment.toString())) {
                throw new WorkspaceException("Unsafe flush table directory: " + value);
            }
        }
        return value;
    }

    private static boolean invalidName(String value) {
        return value == null || value.trim().isEmpty()
                || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0;
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
                throw new IOException("Flush result requires atomic replacement", e);
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
                throw new IOException("Workspace flush result must be owner-only: " + path);
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

    public String tableDirectory() {
        return tableDirectory;
    }

    public Instant flushedAt() {
        return flushedAt;
    }

    public List<ManifestFile> files() {
        return files;
    }
}
