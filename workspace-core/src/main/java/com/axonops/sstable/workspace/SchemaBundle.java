package com.axonops.sstable.workspace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable identity and content of a user-supplied CQL schema bundle. */
public final class SchemaBundle {
    public static final String WORKSPACE_PATH = "schema/schema.cql";
    private static final long MAX_BYTES = 16L * 1024L * 1024L;

    private final Path source;
    private final long size;
    private final String sha256;
    private final byte[] content;

    private SchemaBundle(Path source, long size, String sha256, byte[] content) {
        this.source = source;
        this.size = size;
        this.sha256 = sha256;
        this.content = content;
    }

    public static SchemaBundle capture(Path path) throws WorkspaceException {
        if (path == null) {
            throw new WorkspaceException("Schema bundle path is required");
        }
        try {
            if (Files.isSymbolicLink(path)) {
                throw new WorkspaceException("Schema bundle must not be a symlink: " + path);
            }
            Path canonical = path.toRealPath();
            BasicFileAttributes before = Files.readAttributes(canonical,
                    BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!before.isRegularFile() || before.size() <= 0 || before.size() > MAX_BYTES) {
                throw new WorkspaceException("Schema bundle must be a non-empty regular file no "
                        + "larger than " + MAX_BYTES + " bytes: " + canonical);
            }
            byte[] content = Files.readAllBytes(canonical);
            BasicFileAttributes after = Files.readAttributes(canonical,
                    BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (content.length != before.size() || before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())
                    || !Objects.equals(before.fileKey(), after.fileKey())) {
                throw new WorkspaceException("Schema bundle changed while it was being read: "
                        + canonical);
            }
            requireUtf8(content, canonical);
            return new SchemaBundle(canonical, content.length, Hashing.sha256(content), content);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot capture schema bundle " + path, e);
        }
    }

    public void verifyUnchanged() throws WorkspaceException {
        SchemaBundle current = capture(source);
        if (size != current.size || !sha256.equals(current.sha256)) {
            throw new WorkspaceException("Schema bundle changed after workspace capture: "
                    + source);
        }
    }

    public Map<String, String> identity() {
        Map<String, String> identity = new LinkedHashMap<>();
        identity.put("bundle.source", source.toString());
        identity.put("bundle.size", Long.toString(size));
        identity.put("bundle.sha256", sha256);
        return identity;
    }

    public byte[] content() {
        return Arrays.copyOf(content, content.length);
    }

    public Path source() {
        return source;
    }

    public long size() {
        return size;
    }

    public String sha256() {
        return sha256;
    }

    private static void requireUtf8(byte[] content, Path path) throws WorkspaceException {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(content));
        } catch (CharacterCodingException e) {
            throw new WorkspaceException("Schema bundle is not valid UTF-8: " + path, e);
        }
    }
}
