package com.csforge.sstable.worker.cassandra311;

import com.csforge.sstable.worker.api.ImportOptions;
import com.csforge.sstable.worker.api.ImportResult;
import com.csforge.sstable.worker.api.ImportRuntimeAdapter;
import com.csforge.sstable.worker.api.LinkageVerifier;
import com.csforge.sstable.worker.api.SandboxHandle;
import com.csforge.sstable.worker.api.SandboxOptions;
import com.csforge.sstable.worker.api.SandboxRuntimeAdapter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/** Isolated managed-daemon adapter compiled against Cassandra 3.11.19. */
public final class Cassandra311Runtime implements SandboxRuntimeAdapter, ImportRuntimeAdapter {
    public Cassandra311Runtime() {
    }

    @Override
    public String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }

    @Override
    public void verifyLinkage() {
        verifySharedRuntimeContract();
    }

    @Override
    public SandboxHandle startSandbox(SandboxOptions options) throws Exception {
        requireIsolationProperties(options.configurationFile(), true);
        DatabaseDescriptor.daemonInitialization();
        validateConfiguration(options.workspaceRoot(), true, options.nativePort());

        CassandraDaemon daemon = new CassandraDaemon(true);
        daemon.activate();
        installLocalRingState();
        if (!daemon.isNativeTransportRunning()) {
            throw new IllegalStateException("Cassandra 3.11 native transport did not start");
        }
        if (Gossiper.instance.isEnabled() || MessagingService.instance().isListening()) {
            daemon.stop();
            throw new IllegalStateException("Isolated Cassandra worker started internode services");
        }
        return new Cassandra311SandboxHandle(daemon, options.nativePort());
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
            installLocalRingState();
            if (daemon.isNativeTransportRunning()) {
                throw new IllegalStateException("Import worker started native transport");
            }
            if (Gossiper.instance.isEnabled() || MessagingService.instance().isListening()) {
                throw new IllegalStateException("Import worker started internode services");
            }
            return Cassandra311Importer.run(options);
        } finally {
            if (activated) {
                // Cassandra 3.11 cannot destroy an uninitialized native service; the import
                // worker exits immediately after draining, so there is no client transport
                // to stop here.
                StorageService.instance.drain();
            }
        }
    }

    private static void installLocalRingState() {
        Collection<String> configuredTokens = DatabaseDescriptor.getInitialTokens();
        if (configuredTokens.size() != 1) {
            throw new IllegalStateException("Isolated Cassandra 3.11 worker requires exactly "
                    + "one configured initial token");
        }
        List<Token> tokens = new ArrayList<>(1);
        for (String configuredToken : configuredTokens) {
            tokens.add(DatabaseDescriptor.getPartitioner().getTokenFactory()
                    .fromString(configuredToken));
        }

        Gossiper.instance.maybeInitializeLocalState(
                SystemKeyspace.incrementAndGetGeneration());
        StorageService.instance.getTokenMetadata().updateHostId(
                SystemKeyspace.getOrInitializeLocalHostId(),
                FBUtilities.getBroadcastAddress());
        StorageService.instance.setTokens(tokens);
        if (Gossiper.instance.isEnabled() || MessagingService.instance().isListening()) {
            throw new IllegalStateException("Local ring initialization started internode "
                    + "services");
        }
    }

    private static void verifySharedRuntimeContract() {
        LinkageVerifier.requirePublicStaticMethod(FBUtilities.class,
                "getReleaseVersionString", String.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class, "activate", void.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class,
                "startNativeTransport", void.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class,
                "stopNativeTransport", void.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class,
                "isNativeTransportRunning", boolean.class);
        LinkageVerifier.requirePublicStaticField(QueryProcessor.class,
                "instance", QueryProcessor.class);
        LinkageVerifier.requireAssignable(QueryHandler.class, QueryProcessor.class);
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
            throw new IllegalStateException("JMX must be disabled for the isolated worker");
        }
        requireQueryGuard(startNativeTransport);
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
    }

    private static void requireQueryGuard(boolean startNativeTransport) {
        String handler = System.getProperty(WorkspaceQueryHandler.HANDLER_PROPERTY);
        String keyspace = System.getProperty(WorkspaceQueryHandler.KEYSPACE_PROPERTY);
        String table = System.getProperty(WorkspaceQueryHandler.TABLE_PROPERTY);
        if (startNativeTransport) {
            if (!WorkspaceQueryHandler.class.getName().equals(handler)
                    || keyspace == null || keyspace.isEmpty()
                    || table == null || table.isEmpty()) {
                throw new IllegalStateException("Native transport requires the workspace query "
                        + "guard and an imported table target");
            }
        } else if (handler != null || keyspace != null || table != null) {
            throw new IllegalStateException("Import workers must not expose a native query "
                    + "handler target");
        }
    }

    private static void validateConfiguration(Path root,
                                              boolean startNativeTransport,
                                              int nativePort) throws IOException {
        requireLoopback("listen_address", DatabaseDescriptor.getListenAddress());
        requireLoopback("rpc_address", DatabaseDescriptor.getRpcAddress());
        requireLoopback("broadcast_address", FBUtilities.getBroadcastAddress());
        requireLoopback("broadcast_rpc_address", FBUtilities.getBroadcastRpcAddress());
        if (DatabaseDescriptor.startRpc()) {
            throw new IllegalStateException("Thrift RPC must be disabled");
        }
        if (startNativeTransport
                && !(DatabaseDescriptor.getAuthenticator()
                instanceof WorkspaceAuthenticator)) {
            throw new IllegalStateException("Native transport requires the workspace "
                    + "authenticator");
        }
        if (startNativeTransport
                && !(DatabaseDescriptor.getRoleManager() instanceof WorkspaceRoleManager)) {
            throw new IllegalStateException("Native transport requires the workspace role "
                    + "manager");
        }
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

    private static final class Cassandra311SandboxHandle implements SandboxHandle {
        private final CassandraDaemon daemon;
        private final int nativePort;
        private final AtomicBoolean stopped = new AtomicBoolean();

        private Cassandra311SandboxHandle(CassandraDaemon daemon, int nativePort) {
            this.daemon = daemon;
            this.nativePort = nativePort;
        }

        @Override
        public String nativeAddress() {
            return "127.0.0.1";
        }

        @Override
        public int nativePort() {
            return nativePort;
        }

        @Override
        public boolean isRunning() {
            return !stopped.get() && daemon.isNativeTransportRunning()
                    && !Gossiper.instance.isEnabled()
                    && !MessagingService.instance().isListening();
        }

        @Override
        public void stop() throws Exception {
            if (stopped.compareAndSet(false, true)) {
                daemon.stop();
                StorageService.instance.drain();
            }
        }
    }
}
