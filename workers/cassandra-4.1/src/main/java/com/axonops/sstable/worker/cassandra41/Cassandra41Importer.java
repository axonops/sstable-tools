package com.axonops.sstable.worker.cassandra41;

import com.axonops.sstable.worker.api.ImportOptions;
import com.axonops.sstable.worker.api.ImportResult;
import com.axonops.sstable.worker.api.WorkerProtocol;
import com.axonops.sstable.workspace.SourceComponent;
import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.SstableSet;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/** Validates immutable source sets, stages copies, and imports them through Cassandra. */
final class Cassandra41Importer {
    private static final String EARLIEST_ROW_STORAGE_VERSION = "ma";

    private Cassandra41Importer() {
    }

    static ImportResult run(ImportOptions options) throws Exception {
        WorkspaceRepository repository = WorkspaceRepository.open(options.workspaceRoot());
        WorkspaceManifest manifest = repository.load();
        if (!options.workspaceId().equals(manifest.workspaceId())) {
            throw new IllegalStateException("Workspace manifest identity does not match import "
                    + "request");
        }
        SourceInventory source = manifest.sourceInventory();
        source.verifyUnchanged();
        CqlSchemaBundle schema = CqlSchemaBundle.install(
                options.workspaceRoot().resolve("schema/schema.cql"));
        TableMetadata metadata = Schema.instance.getTableMetadata(
                schema.keyspace(), schema.table());
        if (metadata == null) {
            throw new IllegalStateException("Installed schema metadata is unavailable");
        }
        ColumnFamilyStore cfs = Keyspace.open(schema.keyspace())
                .getColumnFamilyStore(schema.table());
        Path tableDirectory = cfs.getDirectories().getDirectoryForNewSSTables().toPath()
                .toAbsolutePath().normalize();
        requireOwnedTableDirectory(options.workspaceRoot(), tableDirectory);
        try {
            if (!cfs.getLiveSSTables().isEmpty()) {
                throw new IllegalStateException("Import target already contains live SSTables");
            }

            List<ValidatedSet> validated = validateAll(source, metadata);
            source.verifyUnchanged();
            cfs.disableAutoCompaction();
            if (!cfs.isAutoCompactionDisabled()) {
                throw new IllegalStateException("Cassandra did not disable automatic compaction");
            }

            for (int i = 0; i < validated.size(); i++) {
                importOne(options.workspaceRoot(), cfs, validated.get(i), tableDirectory,
                        nextGeneration(tableDirectory), i);
            }
            source.verifyUnchanged();

            if (cfs.verify(Verifier.options().extendedVerification(true).build())
                    != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                throw new IllegalStateException("Cassandra extended SSTable verification failed");
            }
            int liveSstables = cfs.getLiveSSTables().size();
            if (liveSstables < validated.size()) {
                throw new IllegalStateException("Cassandra loaded " + liveSstables
                        + " SSTables from " + validated.size() + " source sets");
            }
            long logicalRows = logicalRows(schema.keyspace(), schema.table());
            Path canonicalTable = tableDirectory.toRealPath();
            String relativeTable = options.workspaceRoot().relativize(canonicalTable)
                    .toString().replace(File.separatorChar, '/');
            return new ImportResult(WorkerProtocol.CURRENT_VERSION, options.workspaceId(),
                    FBUtilities.getReleaseVersionString(), schema.keyspace(), schema.table(),
                    metadata.id.asUUID(), metadata.partitioner.getClass().getCanonicalName(),
                    relativeTable, validated.size(), liveSstables, logicalRows,
                    maximumSourceTimestamp(validated),
                    cfs.isAutoCompactionDisabled(), false, null);
        } catch (Exception failure) {
            try {
                deleteTree(tableDirectory);
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static List<ValidatedSet> validateAll(SourceInventory source,
                                                   TableMetadata metadata) throws Exception {
        List<ValidatedSet> result = new ArrayList<>();
        for (SstableSet set : source.sets()) {
            set.verifyUnchanged();
            Version compatibleVersion = requireCompatibleVersion(
                    set.formatVersion(), set.format());
            Descriptor descriptor = descriptor(set);
            if (!descriptor.isCompatible()
                    || descriptor.formatType != SSTableFormat.Type.BIG
                    || !compatibleVersion.toString().equals(descriptor.version.toString())) {
                throw unsupportedFormat(set.formatVersion(), set.format());
            }
            Set<Component> components = components(set);
            requireComponent(components, Component.DATA, set);
            requireComponent(components, Component.PRIMARY_INDEX, set);
            requireComponent(components, Component.STATS, set);
            requireComponent(components, Component.TOC, set);
            requireComponent(components, Component.DIGEST, set);
            long maxTimestamp = validateHeader(descriptor, metadata);
            try (DataIntegrityMetadata.FileDigestValidator digest =
                         DataIntegrityMetadata.fileDigestValidator(descriptor)) {
                digest.validate();
            }
            SSTableReader reader = SSTableReader.open(descriptor, components,
                    TableMetadataRef.forOfflineTools(metadata), true, true);
            try {
                try (ISSTableScanner scanner = reader.getScanner()) {
                    while (scanner.hasNext()) {
                        try (UnfilteredRowIterator partition = scanner.next()) {
                            while (partition.hasNext()) {
                                partition.next();
                            }
                        }
                    }
                }
            } finally {
                reader.selfRef().release();
            }
            set.verifyUnchanged();
            result.add(new ValidatedSet(set, descriptor, components, maxTimestamp));
        }
        return result;
    }

    static Version requireCompatibleVersion(String version, String format) {
        if (!"big".equalsIgnoreCase(format)) {
            throw unsupportedFormat(version, format);
        }
        final Version candidate;
        try {
            candidate = BigFormat.instance.getVersion(version);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unsupported Cassandra 4.1 SSTable format "
                    + version + "-" + format, e);
        }
        if (candidate == null || !candidate.isCompatible()
                || version.compareTo(EARLIEST_ROW_STORAGE_VERSION) < 0
                || version.compareTo(BigFormat.latestVersion.toString()) > 0
                || !version.equals(candidate.toString())) {
            throw unsupportedFormat(version, format);
        }
        return candidate;
    }

    private static IllegalArgumentException unsupportedFormat(String version, String format) {
        return new IllegalArgumentException("Unsupported Cassandra 4.1 SSTable format "
                + version + "-" + format);
    }

    private static long validateHeader(Descriptor descriptor, TableMetadata metadata)
            throws IOException {
        Map<MetadataType, MetadataComponent> values = descriptor.getMetadataSerializer()
                .deserialize(descriptor, EnumSet.of(
                        MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER));
        ValidationMetadata validation = (ValidationMetadata) values.get(
                MetadataType.VALIDATION);
        String expectedPartitioner = metadata.partitioner.getClass().getCanonicalName();
        if (validation != null && validation.partitioner != null
                && !expectedPartitioner.equals(validation.partitioner)) {
            throw new IllegalArgumentException("SSTable partitioner does not match schema: "
                    + descriptor);
        }
        SerializationHeader.Component header = (SerializationHeader.Component) values.get(
                MetadataType.HEADER);
        if (header == null
                || !metadata.partitionKeyType.equals(header.getKeyType())
                || !metadata.comparator.subtypes().equals(header.getClusteringTypes())
                || !storedColumnsMatch(columnTypes(metadata, true),
                        header.getStaticColumns())
                || !storedColumnsMatch(columnTypes(metadata, false),
                        header.getRegularColumns())) {
            throw new IllegalArgumentException("SSTable serialization header does not exactly "
                    + "match the declared table schema: " + descriptor);
        }
        StatsMetadata stats = (StatsMetadata) values.get(MetadataType.STATS);
        if (stats == null) {
            throw new IllegalArgumentException("SSTable statistics metadata is missing: "
                    + descriptor);
        }
        return stats.maxTimestamp;
    }

    static boolean storedColumnsMatch(Map<ByteBuffer, AbstractType<?>> declared,
                                      Map<ByteBuffer, AbstractType<?>> stored) {
        for (Map.Entry<ByteBuffer, AbstractType<?>> column : stored.entrySet()) {
            AbstractType<?> declaredType = declared.get(column.getKey());
            if (declaredType == null || !declaredType.equals(column.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static long maximumSourceTimestamp(List<ValidatedSet> validated) {
        long maximum = 0L;
        for (ValidatedSet set : validated) {
            maximum = Math.max(maximum, set.maxTimestamp);
        }
        return maximum;
    }

    private static Map<ByteBuffer, AbstractType<?>> columnTypes(TableMetadata metadata,
                                                                 boolean statics) {
        Map<ByteBuffer, AbstractType<?>> result = new LinkedHashMap<>();
        Iterable<ColumnMetadata> columns = statics
                ? metadata.staticColumns()
                : metadata.regularColumns();
        for (ColumnMetadata column : columns) {
            result.put(column.name.bytes, column.type);
        }
        return result;
    }

    private static void importOne(Path workspace,
                                  ColumnFamilyStore cfs,
                                  ValidatedSet source,
                                  Path targetDirectory,
                                  int generation,
                                  int index) throws Exception {
        Descriptor target = new Descriptor(source.descriptor.version,
                new org.apache.cassandra.io.util.File(targetDirectory),
                cfs.keyspace.getName(), cfs.name, new SequenceBasedSSTableId(generation),
                source.descriptor.formatType);
        Path staging = workspace.resolve("staging/import-" + UUID.randomUUID()
                + "/set-" + index).normalize();
        if (!staging.startsWith(workspace) || Files.exists(staging, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Unsafe import staging directory " + staging);
        }
        Files.createDirectories(staging);
        Map<Component, Path> staged = new HashMap<>();
        List<Path> published = new ArrayList<>();
        try {
            for (SourceComponent component : source.set.components()) {
                Component cassandraComponent = component(source.set, component);
                Path destination = staging.resolve(
                        new File(target.filenameFor(cassandraComponent)).getName());
                Files.copy(component.path(), destination, StandardCopyOption.COPY_ATTRIBUTES);
                try (FileChannel channel = FileChannel.open(destination,
                        StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
                staged.put(cassandraComponent, destination);
            }
            source.set.verifyUnchanged();
            for (Map.Entry<Component, Path> entry : staged.entrySet()) {
                Path destination = new File(target.filenameFor(entry.getKey())).toPath();
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                    throw new FileAlreadyExistsException(destination.toString());
                }
            }
            Files.createDirectories(targetDirectory);
            for (Map.Entry<Component, Path> entry : staged.entrySet()) {
                Path destination = new File(target.filenameFor(entry.getKey())).toPath();
                try {
                    Files.move(entry.getValue(), destination, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    throw new IOException("Import publication requires atomic component moves", e);
                }
                published.add(destination);
            }
            int before = cfs.getLiveSSTables().size();
            cfs.loadNewSSTables();
            int after = cfs.getLiveSSTables().size();
            if (after != before + 1) {
                throw new IllegalStateException("Cassandra did not load exactly one staged "
                        + "SSTable set");
            }
            source.set.verifyUnchanged();
        } catch (Exception e) {
            for (Path path : published) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        } finally {
            deleteTree(staging.getParent());
        }
    }

    private static Descriptor descriptor(SstableSet set) {
        return Descriptor.fromFilename(new org.apache.cassandra.io.util.File(
                set.directory().toFile(), set.descriptor() + "-Data.db"));
    }

    private static Set<Component> components(SstableSet set) {
        Set<Component> result = new HashSet<>();
        for (SourceComponent source : set.components()) {
            Component component = component(set, source);
            if (!result.add(component)) {
                throw new IllegalArgumentException("Duplicate Cassandra component in "
                        + set.descriptor());
            }
        }
        return result;
    }

    private static Component component(SstableSet set, SourceComponent source) {
        return Component.parse(source.name());
    }

    private static void requireComponent(Set<Component> components,
                                         Component required,
                                         SstableSet set) {
        if (required == null || !components.contains(required)) {
            throw new IllegalArgumentException("SSTable " + set.descriptor()
                    + " is missing required component "
                    + (required == null ? "digest" : required.name()));
        }
    }

    static int nextGeneration(Path directory) throws IOException {
        int maximum = 0;
        if (Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(
                    directory)) {
                for (Path path : entries) {
                    String[] parts = path.getFileName().toString().split("-", 4);
                    if (parts.length != 4) {
                        continue;
                    }
                    try {
                        int generation = Integer.parseInt(parts[1]);
                        if (generation > 0) {
                            maximum = Math.max(maximum, generation);
                        }
                    } catch (NumberFormatException ignored) {
                        // Non-SSTable files do not reserve a generation.
                    }
                }
            }
        }
        if (maximum == Integer.MAX_VALUE) {
            throw new IOException("SSTable generation space is exhausted");
        }
        return maximum + 1;
    }

    private static long logicalRows(String keyspace, String table) {
        UntypedResultSet result = QueryProcessor.executeInternal("SELECT COUNT(*) FROM "
                + quoteIdentifier(keyspace) + "." + quoteIdentifier(table));
        if (result == null || result.isEmpty()) {
            throw new IllegalStateException("Cassandra did not return an import row count");
        }
        return result.one().getLong("count");
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private static void requireOwnedTableDirectory(Path workspace, Path tableDirectory)
            throws IOException {
        Path data = workspace.resolve("data").toRealPath();
        if (!tableDirectory.startsWith(data) || tableDirectory.equals(data)) {
            throw new IOException("Cassandra selected a table directory outside the workspace: "
                    + tableDirectory);
        }
        Path current = data;
        for (Path part : data.relativize(tableDirectory)) {
            current = current.resolve(part);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)
                    && Files.isSymbolicLink(current)) {
                throw new IOException("Cassandra table directory crosses a symlink: " + current);
            }
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (root == null || !Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException failure)
                    throws IOException {
                if (failure != null) {
                    throw failure;
                }
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static final class ValidatedSet {
        private final SstableSet set;
        private final Descriptor descriptor;
        private final Set<Component> components;
        private final long maxTimestamp;

        private ValidatedSet(SstableSet set,
                             Descriptor descriptor,
                             Set<Component> components,
                             long maxTimestamp) {
            this.set = set;
            this.descriptor = descriptor;
            this.components = components;
            this.maxTimestamp = maxTimestamp;
        }
    }
}
