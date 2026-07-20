package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.ExportRecord;
import com.axonops.sstable.workspace.Hashing;
import com.axonops.sstable.workspace.ManifestFile;
import com.axonops.sstable.workspace.SchemaBundle;
import com.axonops.sstable.workspace.SourceComponent;
import com.axonops.sstable.workspace.SstableSet;
import com.axonops.sstable.workspace.WorkspaceException;
import com.axonops.sstable.workspace.WorkspaceFlushResult;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import com.axonops.sstable.workspace.WorkspaceVerificationResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Builds and atomically publishes a verified workspace SSTable export. */
final class WorkspaceExportPublisher {
    private static final String MANIFEST_NAME = "export-manifest.json";
    private static final String PUBLICATION_MARKER = ".sstable-tools-export";
    private static final String SCHEMA_NAME = "schema.cql";
    private static final String SSTABLE_DIRECTORY = "sstables";
    private static final Gson WRITER = new GsonBuilder()
            .disableHtmlEscaping()
            .setPrettyPrinting()
            .create();
    private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS =
            new HashSet<>(Arrays.asList(
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_WRITE,
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ,
                    PosixFilePermission.OTHERS_WRITE,
                    PosixFilePermission.OTHERS_EXECUTE));

    ExportRecord publish(WorkspaceRepository repository,
                         WorkspaceManifest manifest,
                         WorkspaceFlushResult flush,
                         WorkspaceVerificationResult verification,
                         ExportMode mode,
                         Path requestedOutput) throws WorkspaceException {
        Path output = canonicalOutput(repository, manifest, requestedOutput);
        UUID exportId = deterministicId(manifest, flush, mode, output);
        List<ManifestFile> selected = mode == ExportMode.DELTA
                ? flush.deltaFiles(manifest.baselineInventory()) : flush.files();
        if (selected.isEmpty()) {
            throw new WorkspaceException("Delta export contains no generated SSTables");
        }
        DescriptorInventory descriptors = validateDescriptorSets(repository, selected);
        if (mode == ExportMode.DELTA
                && !descriptors.descriptors.equals(
                new HashSet<>(verification.deltaDescriptors()))) {
            throw new WorkspaceException("Delta export descriptors do not match Cassandra "
                    + "verification evidence");
        }
        int expectedDescriptors = mode == ExportMode.DELTA
                ? verification.deltaSstables() : verification.liveSstables();
        if (descriptors.descriptors.size() != expectedDescriptors) {
            throw new WorkspaceException("Export descriptor count does not match Cassandra "
                    + "verification evidence");
        }

        byte[] marker = publicationMarker(exportId, manifest, flush, mode);
        List<ManifestFile> payload = expectedPayload(repository, selected);
        payload.add(new ManifestFile(PUBLICATION_MARKER, marker.length,
                Hashing.sha256(marker)));
        Collections.sort(payload,
                (left, right) -> left.relativePath().compareTo(right.relativePath()));
        byte[] exportManifest = exportManifest(manifest, flush, verification, mode,
                exportId, payload);
        List<ManifestFile> publishedFiles = new ArrayList<>(payload);
        publishedFiles.add(new ManifestFile(MANIFEST_NAME, exportManifest.length,
                Hashing.sha256(exportManifest)));
        ExportRecord record = new ExportRecord(exportId, verification.verifiedAt(),
                mode.value(), output, publishedFiles);

        if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            verifyPublished(output, record);
            return record;
        }
        publishNew(repository, selected, output, marker, exportManifest, record);
        return record;
    }

    void verifyRecorded(ExportRecord record) throws WorkspaceException {
        verifyPublished(record.outputPath(), record);
    }

    /** Publishes verified delta components beside the explicitly selected source SSTables. */
    List<String> publishDeltaAdjacent(WorkspaceRepository repository,
                                      WorkspaceManifest manifest,
                                      WorkspaceFlushResult flush,
                                      WorkspaceVerificationResult verification,
                                      CassandraInstallation installation)
            throws WorkspaceException {
        manifest.sourceInventory().verifyUnchanged();
        Path target = singleSourceDirectory(manifest);
        List<ManifestFile> selected = flush.deltaFiles(manifest.baselineInventory());
        DescriptorInventory inventory = validateDescriptorSets(repository, selected);
        if (!inventory.descriptors.equals(new HashSet<>(verification.deltaDescriptors()))
                || inventory.descriptors.size() != verification.deltaSstables()) {
            throw new WorkspaceException("Direct publication descriptors do not match Cassandra "
                    + "verification evidence");
        }

        Map<String, List<ManifestFile>> byDescriptor = filesByDescriptor(selected,
                inventory.descriptors);
        boolean uuidIdentifiers = "5.0".equals(installation.version().releaseLine())
                && uuidSstableIdentifiersEnabled(installation.conf());
        Map<String, String> targetDescriptors = allocateDescriptors(target,
                new ArrayList<>(byDescriptor.keySet()), uuidIdentifiers);
        publishComponentSets(repository, target, byDescriptor, targetDescriptors);
        manifest.sourceInventory().verifyUnchanged();
        List<String> result = new ArrayList<>(targetDescriptors.values());
        Collections.sort(result);
        return result;
    }

    Path resolveOutput(WorkspaceRepository repository,
                       WorkspaceManifest manifest,
                       Path requested) throws WorkspaceException {
        return canonicalOutput(repository, manifest, requested);
    }

    private static Path singleSourceDirectory(WorkspaceManifest manifest)
            throws WorkspaceException {
        Path target = null;
        for (SstableSet set : manifest.sourceInventory().sets()) {
            if (target == null) {
                target = set.directory();
            } else if (!target.equals(set.directory())) {
                throw new WorkspaceException("Direct cqlsh requires all --sstables to be from "
                        + "one table directory");
            }
        }
        if (target == null || Files.isSymbolicLink(target)
                || !Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new WorkspaceException("Direct cqlsh source directory is unsafe: " + target);
        }
        try {
            return target.toRealPath();
        } catch (IOException e) {
            throw new WorkspaceException("Cannot resolve direct cqlsh source directory " + target,
                    e);
        }
    }

    private static Map<String, List<ManifestFile>> filesByDescriptor(
            List<ManifestFile> selected, Set<String> descriptors) throws WorkspaceException {
        Map<String, List<ManifestFile>> result = new TreeMap<>();
        for (String descriptor : descriptors) {
            result.put(descriptor, new ArrayList<ManifestFile>());
        }
        for (ManifestFile file : selected) {
            String name = fileName(file.relativePath());
            String owner = null;
            for (String descriptor : descriptors) {
                if (name.startsWith(descriptor + "-")) {
                    if (owner != null) {
                        throw new WorkspaceException("Ambiguous flushed component name: " + name);
                    }
                    owner = descriptor;
                }
            }
            if (owner == null) {
                throw new WorkspaceException("Flushed component has no descriptor: " + name);
            }
            result.get(owner).add(file);
        }
        return result;
    }

    private static Map<String, String> allocateDescriptors(Path target,
                                                             List<String> descriptors,
                                                             boolean uuidIdentifiers)
            throws WorkspaceException {
        Set<String> occupied = tocDescriptors(target);
        Map<String, String> result = new TreeMap<>();
        long nextNumeric = uuidIdentifiers ? -1L : nextNumericIdentifier(occupied);
        for (String descriptor : descriptors) {
            DescriptorParts parts = DescriptorParts.parse(descriptor);
            final String identifier;
            if (uuidIdentifiers) {
                String candidate;
                do {
                    candidate = UUID.randomUUID().toString();
                } while (occupied.contains(parts.version + "-" + candidate + "-" + parts.format));
                identifier = candidate;
            } else {
                identifier = Long.toString(nextNumeric++);
            }
            String allocated = parts.version + "-" + identifier + "-" + parts.format;
            if (!occupied.add(allocated)) {
                throw new WorkspaceException("SSTable identifier collision: " + allocated);
            }
            result.put(descriptor, allocated);
        }
        return result;
    }

    private static Set<String> tocDescriptors(Path target) throws WorkspaceException {
        Set<String> result = new HashSet<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(target, "*-TOC.txt")) {
            for (Path entry : entries) {
                if (Files.isSymbolicLink(entry) || !Files.isRegularFile(entry,
                        LinkOption.NOFOLLOW_LINKS)) {
                    throw new WorkspaceException("Unsafe SSTable TOC in target directory: " + entry);
                }
                String name = entry.getFileName().toString();
                result.add(name.substring(0, name.length() - "-TOC.txt".length()));
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect SSTable identifiers in " + target, e);
        }
        return result;
    }

    private static long nextNumericIdentifier(Set<String> occupied)
            throws WorkspaceException {
        long maximum = 0L;
        for (String descriptor : occupied) {
            int first = descriptor.indexOf('-');
            int last = descriptor.lastIndexOf('-');
            if (first <= 0 || last <= first) {
                continue;
            }
            String identifier = descriptor.substring(first + 1, last);
            try {
                maximum = Math.max(maximum, Long.parseLong(identifier));
            } catch (NumberFormatException ignored) {
                // A UUID descriptor does not participate in sequence allocation.
            }
        }
        if (maximum == Long.MAX_VALUE) {
            throw new WorkspaceException("SSTable numeric identifier space is exhausted");
        }
        return maximum + 1L;
    }

    private static boolean uuidSstableIdentifiersEnabled(Path conf) throws WorkspaceException {
        Path yaml = conf.resolve("cassandra.yaml");
        if (!Files.isRegularFile(yaml, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(yaml)) {
            throw new WorkspaceException("Cassandra 5.0 configuration is missing cassandra.yaml: "
                    + yaml);
        }
        try {
            for (String raw : Files.readAllLines(yaml, StandardCharsets.UTF_8)) {
                String line = raw.split("#", 2)[0].trim();
                if (line.startsWith("uuid_sstable_identifiers_enabled:")) {
                    String value = line.substring(line.indexOf(':') + 1).trim();
                    if ("true".equalsIgnoreCase(value)) {
                        return true;
                    }
                    if ("false".equalsIgnoreCase(value)) {
                        return false;
                    }
                    throw new WorkspaceException("Invalid uuid_sstable_identifiers_enabled "
                            + "value in " + yaml);
                }
            }
            return false;
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read Cassandra configuration " + yaml, e);
        }
    }

    private static void publishComponentSets(WorkspaceRepository repository,
                                             Path target,
                                             Map<String, List<ManifestFile>> source,
                                             Map<String, String> renamed)
            throws WorkspaceException {
        List<Path> staging = new ArrayList<>();
        List<Path> published = new ArrayList<>();
        boolean complete = false;
        try {
            for (Map.Entry<String, List<ManifestFile>> entry : source.entrySet()) {
                String original = entry.getKey();
                String replacement = renamed.get(original);
                for (ManifestFile file : entry.getValue()) {
                    String name = fileName(file.relativePath());
                    String component = name.substring((original + "-").length());
                    Path destination = target.resolve(replacement + "-" + component);
                    if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                        throw new WorkspaceException("Target SSTable component already exists: "
                                + destination);
                    }
                    Path temporary = target.resolve(".sstable-tools-" + replacement + "-"
                            + component + ".tmp");
                    if (Files.exists(temporary, LinkOption.NOFOLLOW_LINKS)) {
                        throw new WorkspaceException("Stale direct publication staging file: "
                                + temporary);
                    }
                    copyFile(repository.resolveInside(file.relativePath()), temporary, file);
                    staging.add(temporary);
                }
            }
            for (Map.Entry<String, List<ManifestFile>> entry : source.entrySet()) {
                String original = entry.getKey();
                String replacement = renamed.get(original);
                List<ManifestFile> ordered = new ArrayList<>(entry.getValue());
                Collections.sort(ordered, (left, right) -> {
                    try {
                        boolean leftToc = fileName(left.relativePath()).endsWith("-TOC.txt");
                        boolean rightToc = fileName(right.relativePath()).endsWith("-TOC.txt");
                        return leftToc == rightToc ? left.relativePath().compareTo(right.relativePath())
                                : (leftToc ? 1 : -1);
                    } catch (WorkspaceException e) {
                        throw new IllegalStateException(e);
                    }
                });
                for (ManifestFile file : ordered) {
                    String name = fileName(file.relativePath());
                    String component = name.substring((original + "-").length());
                    Path temporary = target.resolve(".sstable-tools-" + replacement + "-"
                            + component + ".tmp");
                    Path destination = target.resolve(replacement + "-" + component);
                    try {
                        Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
                    } catch (AtomicMoveNotSupportedException e) {
                        throw new WorkspaceException("Direct publication requires atomic file "
                                + "renames in " + target, e);
                    }
                    staging.remove(temporary);
                    published.add(destination);
                }
            }
            forceDirectory(target);
            complete = true;
        } catch (IOException e) {
            throw new WorkspaceException("Cannot publish SSTable delta beside " + target, e);
        } finally {
            for (Path path : staging) {
                deleteDirectPublicationFile(path);
            }
            if (!complete) {
                for (Path path : published) {
                    deleteDirectPublicationFile(path);
                }
            }
        }
    }

    private static void deleteDirectPublicationFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // The direct command reports failure and leaves the private workspace for recovery.
        }
    }

    private static final class DescriptorParts {
        private final String version;
        private final String format;

        private DescriptorParts(String version, String format) {
            this.version = version;
            this.format = format;
        }

        private static DescriptorParts parse(String descriptor) throws WorkspaceException {
            int first = descriptor.indexOf('-');
            int last = descriptor.lastIndexOf('-');
            if (first <= 0 || last <= first || last == descriptor.length() - 1) {
                throw new WorkspaceException("Invalid flushed SSTable descriptor: " + descriptor);
            }
            return new DescriptorParts(descriptor.substring(0, first),
                    descriptor.substring(last + 1));
        }
    }

    private static Path canonicalOutput(WorkspaceRepository repository,
                                        WorkspaceManifest manifest,
                                        Path requested) throws WorkspaceException {
        if (requested == null) {
            throw new WorkspaceException("Export output path is required");
        }
        Path absolute = requested.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        Path name = absolute.getFileName();
        if (parent == null || name == null || name.toString().isEmpty()) {
            throw new WorkspaceException("Export output path must name a directory");
        }
        try {
            if (Files.isSymbolicLink(parent)
                    || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("Export output parent must be an existing, "
                        + "non-symlink directory: " + parent);
            }
            Path canonicalParent = parent.toRealPath();
            Path output = canonicalParent.resolve(name).normalize();
            if (!output.getParent().equals(canonicalParent)) {
                throw new WorkspaceException("Export output path escapes its parent: " + output);
            }
            requireNoOverlap(output, repository.root(), "workspace");
            for (SstableSet set : manifest.sourceInventory().sets()) {
                requireNoOverlap(output, set.directory(), "source SSTable directory");
            }
            if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(output)
                        || !Files.isDirectory(output, LinkOption.NOFOLLOW_LINKS)
                        || !output.toRealPath().equals(output)) {
                    throw new WorkspaceException("Existing export output is unsafe: " + output);
                }
            }
            return output;
        } catch (IOException e) {
            throw new WorkspaceException("Cannot resolve export output " + requested, e);
        }
    }

    private static void requireNoOverlap(Path output, Path protectedPath, String description)
            throws WorkspaceException {
        Path normalized = protectedPath.toAbsolutePath().normalize();
        if (output.startsWith(normalized) || normalized.startsWith(output)) {
            throw new WorkspaceException("Export output overlaps the " + description + ": "
                    + normalized);
        }
    }

    private static UUID deterministicId(WorkspaceManifest manifest,
                                        WorkspaceFlushResult flush,
                                        ExportMode mode,
                                        Path output) {
        String identity = manifest.workspaceId() + "\n" + flush.sha256() + "\n"
                + mode.value() + "\n" + output;
        return UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] publicationMarker(UUID exportId,
                                            WorkspaceManifest manifest,
                                            WorkspaceFlushResult flush,
                                            ExportMode mode) {
        String content = "formatVersion=1\n"
                + "exportId=" + exportId + "\n"
                + "workspaceId=" + manifest.workspaceId() + "\n"
                + "flushSha256=" + flush.sha256() + "\n"
                + "mode=" + mode.value() + "\n";
        return content.getBytes(StandardCharsets.US_ASCII);
    }

    private static DescriptorInventory validateDescriptorSets(
            WorkspaceRepository repository,
            List<ManifestFile> selected) throws WorkspaceException {
        Map<String, ManifestFile> byName = new TreeMap<>();
        for (ManifestFile file : selected) {
            String name = fileName(file.relativePath());
            if (byName.put(name, file) != null) {
                throw new WorkspaceException("Export would flatten duplicate SSTable component "
                        + name);
            }
        }
        Set<String> expectedNames = new HashSet<>();
        Set<String> descriptors = new HashSet<>();
        for (Map.Entry<String, ManifestFile> entry : byName.entrySet()) {
            String name = entry.getKey();
            if (!name.endsWith("-TOC.txt")) {
                continue;
            }
            String descriptor = name.substring(0, name.length() - "-TOC.txt".length());
            if (descriptor.isEmpty() || !descriptors.add(descriptor)) {
                throw new WorkspaceException("Invalid or duplicate SSTable descriptor "
                        + descriptor);
            }
            Path toc = repository.resolveInside(entry.getValue().relativePath());
            List<String> components;
            try {
                components = Files.readAllLines(toc, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new WorkspaceException("Cannot read flushed SSTable TOC " + toc, e);
            }
            Set<String> declared = new HashSet<>();
            for (String line : components) {
                String component = line.trim();
                if (component.isEmpty()) {
                    continue;
                }
                if (!component.matches("[A-Za-z0-9][A-Za-z0-9+_-]*(?:\\.[A-Za-z0-9]+)+")
                        || component.contains("..") || !declared.add(component)) {
                    throw new WorkspaceException("Unsafe or duplicate component '" + component
                            + "' in " + toc);
                }
                expectedNames.add(descriptor + "-" + component);
            }
            if (!declared.contains("TOC.txt") || !declared.contains("Data.db")
                    || !declared.contains("Statistics.db")
                    || !containsDigest(declared)) {
                throw new WorkspaceException("Export descriptor " + descriptor
                        + " lacks required TOC, data, statistics, or digest components");
            }
        }
        if (descriptors.isEmpty() || !expectedNames.equals(byName.keySet())) {
            Set<String> missing = new HashSet<>(expectedNames);
            missing.removeAll(byName.keySet());
            Set<String> extra = new HashSet<>(byName.keySet());
            extra.removeAll(expectedNames);
            throw new WorkspaceException("Export does not contain complete TOC-declared "
                    + "SSTable sets; missing=" + missing + ", extra=" + extra);
        }
        return new DescriptorInventory(descriptors);
    }

    private static boolean containsDigest(Set<String> components) {
        for (String component : components) {
            if (component.startsWith("Digest.")) {
                return true;
            }
        }
        return false;
    }

    private static String fileName(String relativePath) throws WorkspaceException {
        Path path = java.nio.file.Paths.get(relativePath);
        Path name = path.getFileName();
        if (name == null || name.toString().isEmpty()) {
            throw new WorkspaceException("SSTable component has no filename: " + relativePath);
        }
        return name.toString();
    }

    private static List<ManifestFile> expectedPayload(WorkspaceRepository repository,
                                                       List<ManifestFile> selected)
            throws WorkspaceException {
        List<ManifestFile> result = new ArrayList<>();
        Path schema = repository.resolveInside(SchemaBundle.WORKSPACE_PATH);
        requireRegularFile(schema, "workspace schema");
        try {
            result.add(new ManifestFile(SCHEMA_NAME, Files.size(schema), Hashing.sha256(schema)));
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect workspace schema " + schema, e);
        }
        for (ManifestFile source : selected) {
            Path path = repository.resolveInside(source.relativePath());
            requireIdentity(path, source);
            result.add(new ManifestFile(SSTABLE_DIRECTORY + "/"
                    + fileName(source.relativePath()), source.size(), source.sha256()));
        }
        Collections.sort(result,
                (left, right) -> left.relativePath().compareTo(right.relativePath()));
        return result;
    }

    private static byte[] exportManifest(WorkspaceManifest manifest,
                                         WorkspaceFlushResult flush,
                                         WorkspaceVerificationResult verification,
                                         ExportMode mode,
                                         UUID exportId,
                                         List<ManifestFile> payload) {
        JsonObject root = new JsonObject();
        root.addProperty("formatVersion", 1);
        root.addProperty("exportId", exportId.toString());
        root.addProperty("createdAt", verification.verifiedAt().toString());
        root.addProperty("mode", mode.value());
        root.addProperty("workspaceId", manifest.workspaceId().toString());

        JsonObject table = new JsonObject();
        table.addProperty("cassandraRelease", flush.release());
        table.addProperty("keyspace", flush.keyspace());
        table.addProperty("table", flush.table());
        table.addProperty("tableId", manifest.schemaIdentity().get("table.id"));
        table.addProperty("partitioner", manifest.schemaIdentity().get("partitioner"));
        table.addProperty("sourceMaxTimestampMicros",
                manifest.schemaIdentity().get(SourceTimestampStatus.MANIFEST_KEY));
        table.addProperty("timestampPolicy",
                manifest.schemaIdentity().get(TimestampPolicy.MANIFEST_KEY));
        root.add("table", table);
        root.addProperty("flushSha256", flush.sha256());

        JsonObject verified = new JsonObject();
        verified.addProperty("verifiedAt", verification.verifiedAt().toString());
        verified.addProperty("liveSstables", verification.liveSstables());
        verified.addProperty("deltaSstables", verification.deltaSstables());
        verified.addProperty("logicalRows", verification.logicalRows());
        verified.add("formats", strings(verification.formats()));
        verified.add("deltaDescriptors", strings(verification.deltaDescriptors()));
        root.add("verification", verified);
        root.add("runtimeIdentity", stringMap(manifest.runtimeIdentity()));
        root.add("requiredSources", mode == ExportMode.DELTA
                ? sourceRequirements(manifest) : new JsonArray());
        root.add("files", files(payload));
        return (WRITER.toJson(root) + "\n").getBytes(StandardCharsets.UTF_8);
    }

    private static JsonArray sourceRequirements(WorkspaceManifest manifest) {
        JsonArray result = new JsonArray();
        for (SstableSet set : manifest.sourceInventory().sets()) {
            for (SourceComponent component : set.components()) {
                JsonObject item = new JsonObject();
                item.addProperty("descriptor", set.descriptor());
                item.addProperty("formatVersion", set.formatVersion());
                item.addProperty("component", component.name());
                item.addProperty("path", component.path().toString());
                item.addProperty("size", component.size());
                item.addProperty("sha256", component.sha256());
                result.add(item);
            }
        }
        return result;
    }

    private static JsonArray files(List<ManifestFile> inventory) {
        JsonArray result = new JsonArray();
        for (ManifestFile file : inventory) {
            JsonObject item = new JsonObject();
            item.addProperty("relativePath", file.relativePath());
            item.addProperty("size", file.size());
            item.addProperty("sha256", file.sha256());
            result.add(item);
        }
        return result;
    }

    private static JsonArray strings(List<String> values) {
        JsonArray result = new JsonArray();
        for (String value : values) {
            result.add(value);
        }
        return result;
    }

    private static JsonObject stringMap(Map<String, String> values) {
        JsonObject result = new JsonObject();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            result.addProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static void publishNew(WorkspaceRepository repository,
                                   List<ManifestFile> selected,
                                   Path output,
                                   byte[] markerBytes,
                                   byte[] manifestBytes,
                                   ExportRecord record) throws WorkspaceException {
        Path temporary = output.getParent().resolve("." + output.getFileName() + "."
                + record.exportId() + ".tmp");
        boolean created = false;
        try {
            if (Files.exists(temporary, LinkOption.NOFOLLOW_LINKS)) {
                deleteStaleOwnedStaging(temporary, markerBytes, record);
            }
            Files.createDirectory(temporary);
            created = true;
            restrictDirectory(temporary);
            writeFile(temporary.resolve(PUBLICATION_MARKER), markerBytes,
                    find(record.files(), PUBLICATION_MARKER));
            forceDirectory(temporary);
            Path sstables = temporary.resolve(SSTABLE_DIRECTORY);
            Files.createDirectory(sstables);
            restrictDirectory(sstables);

            copyFile(repository.resolveInside(SchemaBundle.WORKSPACE_PATH),
                    temporary.resolve(SCHEMA_NAME), find(record.files(), SCHEMA_NAME));
            ExportFailpoint.hit("after-first-copy");
            for (ManifestFile source : selected) {
                String targetName = SSTABLE_DIRECTORY + "/" + fileName(source.relativePath());
                copyFile(repository.resolveInside(source.relativePath()),
                        temporary.resolve(targetName), find(record.files(), targetName));
            }
            ExportFailpoint.hit("after-files-copied");
            writeFile(temporary.resolve(MANIFEST_NAME), manifestBytes,
                    find(record.files(), MANIFEST_NAME));
            forceDirectory(sstables);
            forceDirectory(temporary);
            ExportFailpoint.hit("after-export-manifest");
            try {
                Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE);
                created = false;
            } catch (AtomicMoveNotSupportedException e) {
                throw new WorkspaceException("Export publication requires an atomic directory "
                        + "rename on the destination filesystem", e);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                verifyPublished(output, record);
                return;
            }
            forceDirectory(output.getParent());
            ExportFailpoint.hit("after-rename");
            verifyPublished(output, record);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot publish export " + output, e);
        } finally {
            if (created) {
                deleteOwnedTree(temporary);
            }
        }
    }

    private static ManifestFile find(List<ManifestFile> files, String name)
            throws WorkspaceException {
        for (ManifestFile file : files) {
            if (name.equals(file.relativePath())) {
                return file;
            }
        }
        throw new WorkspaceException("Export inventory is missing " + name);
    }

    private static void deleteStaleOwnedStaging(Path root,
                                                byte[] markerBytes,
                                                ExportRecord record)
            throws WorkspaceException {
        if (Files.isSymbolicLink(root)
                || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new WorkspaceException("Export staging path is not a safe directory: "
                    + root);
        }
        requireOwnerOnly(root);
        Path marker = root.resolve(PUBLICATION_MARKER);
        requireIdentity(marker, markerBytes.length, Hashing.sha256(markerBytes));
        Set<String> allowedFiles = new HashSet<>();
        for (ManifestFile file : record.files()) {
            allowedFiles.add(file.relativePath());
        }
        List<Path> paths = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.walk(root)) {
            stream.forEach(paths::add);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect stale export staging " + root, e);
        }
        for (Path path : paths) {
            if (path.equals(root)) {
                continue;
            }
            if (Files.isSymbolicLink(path)) {
                throw new WorkspaceException("Stale export staging contains a symlink: "
                        + path);
            }
            String relative = root.relativize(path).toString().replace('\\', '/');
            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                if (!SSTABLE_DIRECTORY.equals(relative)) {
                    throw new WorkspaceException("Stale export staging contains an unexpected "
                            + "directory: " + path);
                }
                requireOwnerOnly(path);
            } else if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                if (!allowedFiles.contains(relative)) {
                    throw new WorkspaceException("Stale export staging contains an unexpected "
                            + "file: " + path);
                }
                requireOwnerOnly(path);
            } else {
                throw new WorkspaceException("Stale export staging contains an unsafe entry: "
                        + path);
            }
        }
        paths.sort(Collections.reverseOrder());
        try {
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
            forceDirectory(root.getParent());
        } catch (IOException e) {
            throw new WorkspaceException("Cannot remove stale export staging " + root, e);
        }
    }

    private static void copyFile(Path source, Path target, ManifestFile expected)
            throws WorkspaceException, IOException {
        requireRegularFile(source, "export source");
        requireIdentity(source, expected.size(), expected.sha256());
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ);
             FileChannel output = FileChannel.open(target, StandardOpenOption.CREATE_NEW,
                     StandardOpenOption.WRITE)) {
            restrictFile(target);
            long position = 0;
            while (position < expected.size()) {
                long copied = input.transferTo(position, expected.size() - position, output);
                if (copied <= 0) {
                    throw new IOException("Copy made no progress for " + source);
                }
                position += copied;
            }
            if (input.size() != expected.size()) {
                throw new WorkspaceException("Export source changed while copying: " + source);
            }
            output.force(true);
        }
        requireIdentity(target, expected.size(), expected.sha256());
    }

    private static void writeFile(Path target, byte[] content, ManifestFile expected)
            throws WorkspaceException, IOException {
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            restrictFile(target);
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        requireIdentity(target, expected.size(), expected.sha256());
    }

    private static void verifyPublished(Path output, ExportRecord record)
            throws WorkspaceException {
        if (Files.isSymbolicLink(output)
                || !Files.isDirectory(output, LinkOption.NOFOLLOW_LINKS)) {
            throw new WorkspaceException("Published export is missing or unsafe: " + output);
        }
        requireOwnerOnly(output);
        Map<String, ManifestFile> expected = new TreeMap<>();
        for (ManifestFile file : record.files()) {
            expected.put(file.relativePath(), file);
        }
        Map<String, ManifestFile> actual = new TreeMap<>();
        try {
            collectPublished(output, output, actual);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inventory published export " + output, e);
        }
        if (!expected.equals(actual)) {
            throw new WorkspaceException("Existing export output does not match the requested "
                    + "verified publication: " + output);
        }
    }

    private static void collectPublished(Path root,
                                         Path directory,
                                         Map<String, ManifestFile> files)
            throws IOException, WorkspaceException {
        requireOwnerOnly(directory);
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                if (Files.isSymbolicLink(entry)) {
                    throw new WorkspaceException("Published export contains a symlink: " + entry);
                }
                if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                    if (!entry.equals(root.resolve(SSTABLE_DIRECTORY))) {
                        throw new WorkspaceException("Published export contains an unexpected "
                                + "directory: " + entry);
                    }
                    collectPublished(root, entry, files);
                } else if (Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                    requireOwnerOnly(entry);
                    String relative = root.relativize(entry).toString().replace('\\', '/');
                    ManifestFile file = new ManifestFile(relative, Files.size(entry),
                            Hashing.sha256(entry));
                    if (files.put(relative, file) != null) {
                        throw new WorkspaceException("Duplicate published export file "
                                + relative);
                    }
                } else {
                    throw new WorkspaceException("Published export contains an unsafe entry: "
                            + entry);
                }
            }
        }
    }

    private static void requireIdentity(Path path, ManifestFile expected)
            throws WorkspaceException {
        requireIdentity(path, expected.size(), expected.sha256());
    }

    private static void requireIdentity(Path path, long size, String sha256)
            throws WorkspaceException {
        requireRegularFile(path, "export file");
        try {
            long actualSize = Files.size(path);
            String actualHash = Hashing.sha256(path);
            if (actualSize != size || !actualHash.equals(sha256)) {
                throw new WorkspaceException("Export file identity changed: " + path);
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect export file " + path, e);
        }
    }

    private static void requireRegularFile(Path path, String description)
            throws WorkspaceException {
        if (Files.isSymbolicLink(path)
                || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new WorkspaceException(description + " is missing or unsafe: " + path);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void restrictDirectory(Path directory) throws IOException {
        try {
            Files.setPosixFilePermissions(directory,
                    PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException ignored) {
            // The parent ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static void restrictFile(Path file) throws IOException {
        try {
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The parent ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static void requireOwnerOnly(Path path) throws WorkspaceException {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            if (!Collections.disjoint(permissions, FORBIDDEN_PERMISSIONS)) {
                throw new WorkspaceException("Published export must be owner-only: " + path);
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect export permissions " + path, e);
        } catch (UnsupportedOperationException ignored) {
            // The parent ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static void deleteOwnedTree(Path root) {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try {
            List<Path> paths = new ArrayList<>();
            try (java.util.stream.Stream<Path> stream = Files.walk(root)) {
                stream.forEach(paths::add);
            }
            paths.sort(Collections.reverseOrder());
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        } catch (IOException ignored) {
            // Preserve the publication failure; the UUID-named staging tree is recoverable.
        }
    }

    private static final class DescriptorInventory {
        private final Set<String> descriptors;

        private DescriptorInventory(Set<String> descriptors) {
            this.descriptors = Collections.unmodifiableSet(new HashSet<>(descriptors));
        }
    }
}
