package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.WorkspaceException;
import com.axonops.sstable.workspace.WorkspaceLock;
import com.axonops.sstable.workspace.WorkspaceRepository;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;

/** Generates the minimal fail-closed Cassandra 3.11 sandbox configuration. */
final class Cassandra311SandboxConfig {
    static final String CONFIG_PATH = "runtime/cassandra.yaml";
    static final String CONTROL_TOKEN_PATH = "state/control.token";
    static final String CQLSHRC_PATH = "state/cqlshrc";
    static final String ENDPOINT_PATH = "state/worker.properties";
    static final String NATIVE_USERNAME = "sstable_workspace";

    private Cassandra311SandboxConfig() {
    }

    static void write(WorkspaceRepository repository,
                      WorkspaceLock lock,
                      UUID workspaceId,
                      int nativePort,
                      String controlToken,
                      String nativePassword) throws WorkspaceException {
        write(repository, lock, workspaceId, nativePort, controlToken, nativePassword, "3.11");
    }

    static void write(WorkspaceRepository repository,
                      WorkspaceLock lock,
                      UUID workspaceId,
                      int nativePort,
                      String controlToken,
                      String nativePassword,
                      String releaseLine) throws WorkspaceException {
        if (nativePort < 1 || nativePort > 65535
                || controlToken == null || !controlToken.matches("[0-9a-f]{64}")
                || nativePassword == null || !nativePassword.matches("[0-9a-f]{64}")) {
            throw new WorkspaceException("Invalid Cassandra 3.11 sandbox endpoint inputs");
        }
        writeConfiguration(repository, lock, workspaceId, nativePort, true, releaseLine);
        repository.writeOwnedFile(lock, CONTROL_TOKEN_PATH,
                (controlToken + "\n").getBytes(StandardCharsets.US_ASCII));
        String cqlshrc = "[authentication]\n"
                + "username = " + NATIVE_USERNAME + "\n"
                + "password = " + nativePassword + "\n";
        repository.writeOwnedFile(lock, CQLSHRC_PATH,
                cqlshrc.getBytes(StandardCharsets.US_ASCII));
    }

    static void writeImport(WorkspaceRepository repository,
                            WorkspaceLock lock,
                            UUID workspaceId,
                            int nativePort) throws WorkspaceException {
        writeImport(repository, lock, workspaceId, nativePort, "3.11");
    }

    static void writeImport(WorkspaceRepository repository,
                            WorkspaceLock lock,
                            UUID workspaceId,
                            int nativePort,
                            String releaseLine) throws WorkspaceException {
        if (nativePort < 1 || nativePort > 65535) {
            throw new WorkspaceException("Invalid Cassandra 3.11 import endpoint input");
        }
        repository.deleteOwnedFile(lock, CQLSHRC_PATH);
        repository.deleteOwnedFile(lock, WorkspaceTimestampState.WORKSPACE_PATH);
        writeConfiguration(repository, lock, workspaceId, nativePort, false, releaseLine);
    }

    private static void writeConfiguration(WorkspaceRepository repository,
                                           WorkspaceLock lock,
                                           UUID workspaceId,
                                           int nativePort,
                                           boolean startNativeTransport,
                                           String releaseLine)
            throws WorkspaceException {
        boolean supportsThrift = "3.11".equals(releaseLine);
        boolean supportsCommitlogBatchWindow = !"5.0".equals(releaseLine);
        Path root = repository.root();
        boolean cassandra311 = "3.11".equals(releaseLine);
        String authenticator = startNativeTransport && cassandra311
                ? "com.axonops.sstable.worker.cassandra311.WorkspaceAuthenticator"
                : "AllowAllAuthenticator";
        String roleManager = startNativeTransport && cassandra311
                ? "com.axonops.sstable.worker.cassandra311.WorkspaceRoleManager"
                : "4.0".equals(releaseLine)
                ? "com.axonops.sstable.worker.cassandra40.OfflineRoleManager"
                : "4.1".equals(releaseLine)
                ? "com.axonops.sstable.worker.cassandra41.OfflineRoleManager"
                : "5.0".equals(releaseLine)
                ? "com.axonops.sstable.worker.cassandra50.OfflineRoleManager"
                : "CassandraRoleManager";
        String yaml = "cluster_name: 'sstable-tools-" + workspaceId + "'\n"
                + "authenticator: " + authenticator + "\n"
                + "authorizer: AllowAllAuthorizer\n"
                + "role_manager: " + roleManager + "\n"
                + "internode_authenticator: "
                + "org.apache.cassandra.auth.AllowAllInternodeAuthenticator\n"
                + "partitioner: org.apache.cassandra.dht.Murmur3Partitioner\n"
                + "auto_bootstrap: false\n"
                + "num_tokens: 1\n"
                + "initial_token: '0'\n"
                + "hinted_handoff_enabled: false\n"
                + "max_hint_window_in_ms: 0\n"
                + "seed_provider:\n"
                + "  - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n"
                + "    parameters:\n"
                + "      - seeds: '127.0.0.1'\n"
                + "listen_address: 127.0.0.1\n"
                + "broadcast_address: 127.0.0.1\n"
                + "storage_port: 17000\n"
                + "ssl_storage_port: 17001\n"
                + "rpc_address: 127.0.0.1\n"
                + "broadcast_rpc_address: 127.0.0.1\n"
                + (supportsThrift ? "start_rpc: false\n"
                + "rpc_port: 19160\n" : "")
                + "start_native_transport: " + startNativeTransport + "\n"
                + "native_transport_port: " + nativePort + "\n"
                + "native_transport_max_threads: 16\n"
                + "endpoint_snitch: SimpleSnitch\n"
                + "dynamic_snitch: false\n"
                + "internode_compression: none\n"
                + "commitlog_sync: batch\n"
                + (supportsCommitlogBatchWindow
                ? "commitlog_sync_batch_window_in_ms: 2\n" : "")
                + "commitlog_total_space_in_mb: 64\n"
                + "commitlog_segment_size_in_mb: 16\n"
                + "data_file_directories:\n"
                + "  - " + quote(root.resolve("data")) + "\n"
                + "commitlog_directory: " + quote(root.resolve("commitlog")) + "\n"
                + "hints_directory: " + quote(root.resolve("hints")) + "\n"
                + "saved_caches_directory: " + quote(root.resolve("saved_caches")) + "\n"
                + "cdc_enabled: false\n"
                + "cdc_raw_directory: " + quote(root.resolve("runtime/cdc_raw")) + "\n"
                + "cdc_total_space_in_mb: 64\n"
                + "disk_failure_policy: stop\n"
                + "commit_failure_policy: stop\n"
                + "auto_snapshot: false\n"
                + "incremental_backups: false\n"
                + "concurrent_reads: 2\n"
                + "concurrent_writes: 2\n"
                + "concurrent_counter_writes: 2\n"
                + "concurrent_materialized_view_writes: 2\n"
                + "concurrent_compactors: 1\n"
                + "memtable_flush_writers: 1\n"
                + "memtable_heap_space_in_mb: 64\n"
                + "memtable_offheap_space_in_mb: 64\n"
                + "file_cache_size_in_mb: 64\n"
                + "key_cache_size_in_mb: 0\n"
                + "row_cache_size_in_mb: 0\n"
                + "counter_cache_size_in_mb: 0\n"
                + "compaction_throughput_mb_per_sec: 0\n"
                + "min_free_space_per_drive_in_mb: 1\n";
        repository.writeOwnedFile(lock, CONFIG_PATH,
                yaml.getBytes(StandardCharsets.UTF_8));
    }

    private static String quote(Path path) {
        return "'" + path.toString().replace("'", "''") + "'";
    }
}
