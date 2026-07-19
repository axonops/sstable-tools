package com.axonops.sstable.worker.cassandra50;

import com.axonops.sstable.worker.api.LinkageVerifier;
import com.axonops.sstable.worker.api.ImportOptions;
import com.axonops.sstable.worker.api.ImportResult;
import com.axonops.sstable.worker.api.ImportRuntimeAdapter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 5.0 API. */
public final class Cassandra50Runtime implements ImportRuntimeAdapter {
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
    }

    @Override
    public ImportResult importSstables(ImportOptions options) throws Exception {
        requireIsolationProperties(options.configurationFile());
        DatabaseDescriptor.daemonInitialization();
        validateConfiguration(options.workspaceRoot());
        CassandraDaemon daemon = new CassandraDaemon(true);
        boolean activated = false;
        try {
            daemon.activate();
            activated = true;
            if (daemon.isNativeTransportRunning()) {
                throw new IllegalStateException("Import worker started native transport");
            }
            if (org.apache.cassandra.gms.Gossiper.instance.isEnabled()) {
                throw new IllegalStateException("Import worker started gossip");
            }
            return Cassandra50Importer.run(options);
        } finally {
            if (activated) {
                StorageService.instance.drain();
            }
        }
    }

    private static void requireIsolationProperties(Path configurationFile) {
        requireFalse("cassandra.start_gossip");
        requireFalse("cassandra.join_ring");
        requireFalse("cassandra.load_ring_state");
        requireFalse("cassandra.start_rpc");
        requireFalse("cassandra.start_native_transport");
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
            if (!Paths.get(URI.create(configured)).toRealPath()
                    .equals(configurationFile.toRealPath())) {
                throw new IllegalStateException("cassandra.config does not reference the "
                        + "workspace configuration");
            }
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalStateException("Invalid cassandra.config " + configured, e);
        }
    }

    private static void validateConfiguration(Path root) throws IOException {
        requireLoopback("listen_address", DatabaseDescriptor.getListenAddress());
        requireLoopback("rpc_address", DatabaseDescriptor.getRpcAddress());
        requireLoopback("broadcast_address", DatabaseDescriptor.getBroadcastAddress());
        requireLoopback("broadcast_rpc_address", DatabaseDescriptor.getBroadcastRpcAddress());
        if (DatabaseDescriptor.startNativeTransport()) {
            throw new IllegalStateException("Cassandra native transport must be disabled");
        }
        for (String directory : DatabaseDescriptor.getAllDataFileLocations()) {
            requireOwnedPath(root, Paths.get(directory), "data_file_directories");
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
}
