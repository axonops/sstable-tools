package com.axonops.sstable.worker.cassandra50;

import com.axonops.sstable.worker.api.ImportOptions;
import com.axonops.sstable.worker.api.ImportResult;
import com.axonops.sstable.worker.api.ImportRuntimeAdapter;
import com.axonops.sstable.worker.api.LinkageVerifier;
import com.axonops.sstable.worker.api.SandboxHandle;
import com.axonops.sstable.worker.api.SandboxOptions;
import com.axonops.sstable.worker.api.SandboxRuntimeAdapter;
import com.axonops.sstable.workspace.WorkspaceFlushResult;
import com.axonops.sstable.workspace.ManifestFile;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import com.axonops.sstable.workspace.WorkspaceVerificationResult;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 5.0 API. */
public final class Cassandra50Runtime implements SandboxRuntimeAdapter, ImportRuntimeAdapter {
    public Cassandra50Runtime() {
    }

    @Override
    public String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }

    @Override
    public void verifyLinkage() {
        LinkageVerifier.requirePublicStaticMethod(FBUtilities.class,
                "getReleaseVersionString", String.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class, "activate", void.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class,
                "startNativeTransport", void.class);
        LinkageVerifier.requirePublicStaticField(QueryProcessor.class,
                "instance", QueryProcessor.class);
        LinkageVerifier.requireAssignable(QueryHandler.class, QueryProcessor.class);
        LinkageVerifier.requireAssignable(QueryHandler.class, WorkspaceQueryHandler.class);
        LinkageVerifier.requirePublicMethod(QueryHandler.class, "parse",
                org.apache.cassandra.cql3.CQLStatement.class, String.class,
                QueryState.class, QueryOptions.class);
        LinkageVerifier.requirePublicMethod(QueryHandler.class, "prepare",
                org.apache.cassandra.transport.messages.ResultMessage.Prepared.class,
                String.class, ClientState.class, java.util.Map.class);
        LinkageVerifier.requirePublicMethod(ColumnFamilyStore.class,
                "disableAutoCompaction", void.class);
        LinkageVerifier.requirePublicMethod(ColumnFamilyStore.class,
                "forceBlockingFlush", org.apache.cassandra.db.commitlog.CommitLogPosition.class,
                ColumnFamilyStore.FlushReason.class);
        LinkageVerifier.requirePublicMethod(ColumnFamilyStore.class, "verify",
                CompactionManager.AllSSTableOpStatus.class, IVerifier.Options.class);
    }

    @Override
    public SandboxHandle startSandbox(SandboxOptions options) throws Exception {
        requireIsolationProperties(options.configurationFile(), true);
        DatabaseDescriptor.daemonInitialization();
        validateConfiguration(options.workspaceRoot(), true, options.nativePort());

        CassandraDaemon daemon = new CassandraDaemon(true);
        boolean activated = false;
        try {
            daemon.activate();
            activated = true;
            installLocalRingState();
            if (!daemon.isNativeTransportRunning()) {
                throw new IllegalStateException("Cassandra 5.0 native transport did not start");
            }
            if (Gossiper.instance.isEnabled()) {
                throw new IllegalStateException("Isolated Cassandra worker started gossip");
            }
            String keyspace = System.getProperty(WorkspaceQueryHandler.KEYSPACE_PROPERTY);
            String table = System.getProperty(WorkspaceQueryHandler.TABLE_PROPERTY);
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();
            List<ColumnFamilyStore> stores = Collections.singletonList(cfs);
            CompactionManager.instance.interruptCompactionForCFs(stores, reader -> true, true);
            CompactionManager.instance.waitForCessation(stores, reader -> true);
            if (!cfs.isAutoCompactionDisabled()
                    || CompactionManager.instance.isCompacting(stores, reader -> true)) {
                throw new IllegalStateException("Workspace table compaction did not stop");
            }
            return new Cassandra50SandboxHandle(daemon, options, installedVersion(), keyspace,
                    table, requireSingleTableDirectory(options.workspaceRoot(), cfs), cfs);
        } catch (Exception e) {
            if (activated) {
                daemon.deactivate();
            }
            throw e;
        }
    }

    @Override
    public ImportResult importSstables(ImportOptions options) throws Exception {
        requireIsolationProperties(options.configurationFile(), false);
        DatabaseDescriptor.daemonInitialization();
        validateConfiguration(options.workspaceRoot(), false, -1);

        CassandraDaemon daemon = new CassandraDaemon(true);
        boolean activated = false;
        try {
            daemon.activate();
            activated = true;
            if (daemon.isNativeTransportRunning()) {
                throw new IllegalStateException("Import worker started native transport");
            }
            if (Gossiper.instance.isEnabled()) {
                throw new IllegalStateException("Import worker started gossip");
            }
            return Cassandra50Importer.run(options);
        } finally {
            if (activated) {
                StorageService.instance.drain();
            }
        }
    }

    private static void requireIsolationProperties(Path configurationFile,
                                                   boolean startNativeTransport) {
        requireFalse("cassandra.start_gossip");
        requireFalse("cassandra.join_ring");
        requireFalse("cassandra.load_ring_state");
        requireFalse("cassandra.start_rpc");
        if (!Boolean.toString(startNativeTransport).equalsIgnoreCase(
                System.getProperty("cassandra.start_native_transport"))) {
            throw new IllegalStateException("cassandra.start_native_transport must be "
                    + startNativeTransport);
        }
        if (System.getProperty("cassandra.jmx.remote.port") != null
                || System.getProperty("cassandra.jmx.local.port") != null
                || System.getProperty("com.sun.management.jmxremote.port") != null) {
            throw new IllegalStateException("JMX must be disabled for the import worker");
        }
        String configured = System.getProperty("cassandra.config");
        if (configured == null) {
            throw new IllegalStateException("cassandra.config is required");
        }
        try {
            Path configuredPath = Paths.get(URI.create(configured)).toRealPath();
            if (!configuredPath.equals(configurationFile.toRealPath())) {
                throw new IllegalStateException("cassandra.config does not reference the "
                        + "workspace configuration");
            }
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalStateException("Invalid cassandra.config " + configured, e);
        }
        requireQueryGuard(startNativeTransport);
    }

    private static void requireQueryGuard(boolean startNativeTransport) {
        String handler = System.getProperty(WorkspaceQueryHandler.HANDLER_PROPERTY);
        String keyspace = System.getProperty(WorkspaceQueryHandler.KEYSPACE_PROPERTY);
        String table = System.getProperty(WorkspaceQueryHandler.TABLE_PROPERTY);
        String sourceMaximum = System.getProperty(
                WorkspaceQueryHandler.SOURCE_MAX_TIMESTAMP_PROPERTY);
        if (startNativeTransport) {
            if (!WorkspaceQueryHandler.class.getName().equals(handler)
                    || keyspace == null || keyspace.isEmpty() || table == null || table.isEmpty()
                    || sourceMaximum == null || sourceMaximum.isEmpty()) {
                throw new IllegalStateException("Native transport requires the Cassandra 5.0 "
                        + "workspace query guard and imported table target");
            }
        } else if (handler != null || keyspace != null || table != null || sourceMaximum != null) {
            throw new IllegalStateException("Import workers must not expose a native query handler");
        }
    }

    private static void validateConfiguration(Path root,
                                              boolean startNativeTransport,
                                              int nativePort) throws IOException {
        requireLoopback("listen_address", DatabaseDescriptor.getListenAddress());
        requireLoopback("rpc_address", DatabaseDescriptor.getRpcAddress());
        requireLoopback("broadcast_address", DatabaseDescriptor.getBroadcastAddress());
        requireLoopback("broadcast_rpc_address", DatabaseDescriptor.getBroadcastRpcAddress());
        if (DatabaseDescriptor.startNativeTransport() != startNativeTransport
                || startNativeTransport
                && DatabaseDescriptor.getNativeTransportPort() != nativePort) {
            throw new IllegalStateException("Native transport configuration does not match "
                    + "the allocated endpoint");
        }
        for (String dataDirectory : DatabaseDescriptor.getAllDataFileLocations()) {
            requireOwnedPath(root, Paths.get(dataDirectory), "data_file_directories");
        }
        requireOwnedPath(root, Paths.get(DatabaseDescriptor.getCommitLogLocation()),
                "commitlog_directory");
        requireOwnedPath(root, Paths.get(DatabaseDescriptor.getSavedCachesLocation()),
                "saved_caches_directory");
        requireOwnedPath(root, DatabaseDescriptor.getHintsDirectory().toPath(),
                "hints_directory");
        requireOwnedPath(root, Paths.get(DatabaseDescriptor.getCDCLogLocation()),
                "cdc_raw_directory");
    }

    private static void requireFalse(String name) {
        if (!"false".equalsIgnoreCase(System.getProperty(name))) {
            throw new IllegalStateException(name + " must be explicitly false");
        }
    }

    private static void requireLoopback(String name, InetAddress address) {
        if (address == null || !address.isLoopbackAddress()) {
            throw new IllegalStateException(name + " must resolve to loopback, not " + address);
        }
    }

    private static void requireOwnedPath(Path root, Path configured, String name)
            throws IOException {
        Path path = configured.toAbsolutePath().normalize();
        if (!path.startsWith(root) || path.equals(root)) {
            throw new IllegalStateException(name + " escapes the workspace: " + path);
        }
        Path current = root;
        for (Path segment : root.relativize(path)) {
            current = current.resolve(segment);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)
                    && Files.isSymbolicLink(current)) {
                throw new IllegalStateException(name + " crosses a symlink: " + current);
            }
        }
    }

    private static void installLocalRingState() {
        Collection<String> configuredTokens = DatabaseDescriptor.getInitialTokens();
        if (configuredTokens.size() != 1) {
            throw new IllegalStateException("Isolated Cassandra 5.0 worker requires exactly "
                    + "one configured initial token");
        }
        List<Token> tokens = new ArrayList<>(1);
        for (String configuredToken : configuredTokens) {
            tokens.add(DatabaseDescriptor.getPartitioner().getTokenFactory()
                    .fromString(configuredToken));
        }
        Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
        StorageService.instance.getTokenMetadata().updateHostId(
                SystemKeyspace.getOrInitializeLocalHostId(),
                FBUtilities.getBroadcastAddressAndPort());
        StorageService.instance.setTokens(tokens);
        if (Gossiper.instance.isEnabled()) {
            throw new IllegalStateException("Local ring initialization started gossip");
        }
    }

    private static String requireSingleTableDirectory(Path workspace, ColumnFamilyStore cfs)
            throws IOException {
        List<org.apache.cassandra.io.util.File> directories = cfs.getDirectories()
                .getCFDirectories();
        if (directories.size() != 1) {
            throw new IllegalStateException("Workspace table must use exactly one data directory");
        }
        Path root = workspace.toRealPath();
        Path directory = directories.get(0).toPath().toRealPath();
        if (!directory.startsWith(root) || directory.equals(root)) {
            throw new IllegalStateException("Workspace table directory escapes workspace");
        }
        return root.relativize(directory).toString().replace(File.separatorChar, '/');
    }

    private static final class Cassandra50SandboxHandle implements SandboxHandle {
        private final CassandraDaemon daemon;
        private final SandboxOptions options;
        private final String release;
        private final String keyspace;
        private final String table;
        private final String tableDirectory;
        private final ColumnFamilyStore cfs;
        private final AtomicBoolean stopped = new AtomicBoolean();
        private volatile boolean flushed;

        private Cassandra50SandboxHandle(CassandraDaemon daemon, SandboxOptions options,
                                         String release, String keyspace, String table,
                                         String tableDirectory, ColumnFamilyStore cfs) {
            this.daemon = daemon;
            this.options = options;
            this.release = release;
            this.keyspace = keyspace;
            this.table = table;
            this.tableDirectory = tableDirectory;
            this.cfs = cfs;
        }

        @Override public String nativeAddress() { return "127.0.0.1"; }
        @Override public int nativePort() { return options.nativePort(); }
        @Override public boolean isRunning() {
            return !stopped.get() && !flushed && daemon.isNativeTransportRunning()
                    && !Gossiper.instance.isEnabled();
        }

        @Override public synchronized void flush() throws Exception {
            if (flushed) return;
            if (!isRunning()) throw new IllegalStateException("Workspace sandbox is not running");
            daemon.stopNativeTransport();
            WorkspaceQueryHandler.quiesceAndAwaitRequests();
            if (daemon.isNativeTransportRunning() || !WorkspaceQueryHandler.isQuiesced()) {
                throw new IllegalStateException("Native transport did not quiesce");
            }
            if (!cfs.isAutoCompactionDisabled()
                    || CompactionManager.instance.isCompacting(
                    Collections.singletonList(cfs), reader -> true)) {
                throw new IllegalStateException("Workspace compaction is active");
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            WorkspaceFlushResult.capture(options.workspaceRoot(), options.workspaceId(), release,
                    keyspace, table, tableDirectory).writeAtomically(options.workspaceRoot());
            if (Gossiper.instance.isEnabled()) {
                throw new IllegalStateException("Workspace flush started gossip");
            }
            flushed = true;
        }

        @Override public boolean isFlushed() {
            return !stopped.get() && flushed && !daemon.isNativeTransportRunning()
                    && WorkspaceQueryHandler.isQuiesced() && !Gossiper.instance.isEnabled();
        }

        @Override public synchronized void verify() throws Exception {
            if (!isFlushed()) throw new IllegalStateException("Workspace table must be flushed first");
            if (cfs.verify(IVerifier.options().extendedVerification(true).build())
                    != CompactionManager.AllSSTableOpStatus.SUCCESSFUL) {
                throw new IllegalStateException("Cassandra extended SSTable verification failed");
            }
            WorkspaceManifest manifest = WorkspaceRepository.open(options.workspaceRoot()).load();
            WorkspaceFlushResult flush = WorkspaceFlushResult.read(options.workspaceRoot());
            flush.requireIdentity(options.workspaceId(), release, keyspace, table, tableDirectory);
            flush.verifyCompleteInventory(options.workspaceRoot());
            List<ManifestFile> deltaFiles = flush.deltaFiles(manifest.baselineInventory());
            Set<String> deltaDescriptors = descriptorNames(deltaFiles);
            Set<SSTableReader> readers = cfs.getLiveSSTables();
            Set<String> liveDescriptors = new HashSet<>();
            Set<String> formats = new TreeSet<>();
            for (SSTableReader reader : readers) {
                String descriptor = descriptorName(reader.descriptor);
                liveDescriptors.add(descriptor);
                if (deltaDescriptors.contains(descriptor)) {
                    if (!reader.descriptor.version.isLatestVersion()
                            || reader.getSSTableMetadata().repairedAt
                            != ActiveRepairService.UNREPAIRED_SSTABLE) {
                        throw new IllegalStateException("Generated SSTable has invalid format "
                                + "or repair state");
                    }
                    formats.add(reader.descriptor.version.toString());
                }
            }
            if (!liveDescriptors.equals(descriptorNames(flush.files()))) {
                throw new IllegalStateException("Flush inventory does not match Cassandra live SSTables");
            }
            WorkspaceVerificationResult result = new WorkspaceVerificationResult(options.workspaceId(),
                    release, keyspace, table, flush.sha256(), java.time.Instant.now(),
                    readers.size(), deltaDescriptors.size(), logicalRows(),
                    new ArrayList<>(formats), new ArrayList<>(deltaDescriptors));
            result.writeAtomically(options.workspaceRoot());
        }

        @Override public boolean isVerified() {
            try {
                WorkspaceVerificationResult result = WorkspaceVerificationResult.read(
                        options.workspaceRoot());
                WorkspaceFlushResult flush = WorkspaceFlushResult.read(options.workspaceRoot());
                result.requireIdentity(options.workspaceId(), release, keyspace, table, flush.sha256());
                return result.liveSstables() == cfs.getLiveSSTables().size();
            } catch (Exception e) { return false; }
        }

        private long logicalRows() {
            UntypedResultSet result = QueryProcessor.executeInternal("SELECT COUNT(*) FROM \""
                    + keyspace.replace("\"", "\"\"") + "\".\""
                    + table.replace("\"", "\"\"") + "\"");
            if (result == null || result.isEmpty()) {
                throw new IllegalStateException("Cassandra did not return a verification row count");
            }
            return result.one().getLong("count");
        }

        private Set<String> descriptorNames(List<ManifestFile> files) {
            Set<String> descriptors = new HashSet<>();
            for (ManifestFile file : files) {
                descriptors.add(descriptorName(Descriptor.fromFile(
                        new org.apache.cassandra.io.util.File(
                                options.workspaceRoot().resolve(file.relativePath())))));
            }
            return descriptors;
        }

        private static String descriptorName(Descriptor descriptor) {
            return descriptor.baseFile().name();
        }

        @Override public void stop() throws Exception {
            if (stopped.compareAndSet(false, true)) daemon.deactivate();
        }
    }
}
