package com.axonops.sstable.worker.cassandra311;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Complete Cassandra daemon used to prove same-host worker coexistence. */
final class Cassandra311ProductionFixture {
    static final int STORAGE_PORT = 7000;
    static final int NATIVE_PORT = 9042;
    static final String CLUSTER_NAME = "sstable-tools-production-fixture";
    private static final long START_TIMEOUT_SECONDS = 120;
    private static final long STOP_TIMEOUT_SECONDS = 60;

    private final Process process;
    private final Path output;
    private final Path error;

    private Cassandra311ProductionFixture(Process process, Path output, Path error) {
        this.process = process;
        this.output = output;
        this.error = error;
    }

    static Cassandra311ProductionFixture start(Path cassandraHome,
                                               Path javaHome,
                                               Path root) throws Exception {
        prepareDirectories(root);
        Path configuration = root.resolve("conf/cassandra.yaml");
        Files.write(configuration, yaml(root).getBytes(StandardCharsets.UTF_8));
        Path output = root.resolve("logs/production.out");
        Path error = root.resolve("logs/production.err");

        List<String> command = new ArrayList<>();
        command.add(javaHome.resolve("bin/java").toString());
        command.add("-Xms512m");
        command.add("-Xmx512m");
        command.add("-javaagent:" + cassandraHome.resolve("lib/jamm-0.3.0.jar"));
        command.add("-Dcassandra.config=" + configuration.toUri());
        command.add("-Dcassandra.storagedir=" + root);
        command.add("-Dcassandra.logdir=" + root.resolve("logs"));
        command.add("-Dcassandra-foreground=true");
        command.add("-Dcassandra.start_gossip=true");
        command.add("-Dcassandra.join_ring=true");
        command.add("-Dcassandra.load_ring_state=false");
        command.add("-Dcassandra.start_rpc=false");
        command.add("-Dcassandra.start_native_transport=true");
        command.add("-Dcassandra.size_recorder_interval=0");
        command.add("-Djava.io.tmpdir=" + root.resolve("runtime/tmp"));
        command.add("-cp");
        command.add(classpath(cassandraHome));
        command.add("org.apache.cassandra.service.CassandraDaemon");

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectOutput(output.toFile());
        builder.redirectError(error.toFile());
        builder.environment().put("CASSANDRA_HOME", cassandraHome.toString());
        builder.environment().put("CASSANDRA_CONF", root.resolve("conf").toString());
        builder.environment().put("JAVA_HOME", javaHome.toString());
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        builder.environment().remove("_JAVA_OPTIONS");
        Process process = builder.start();
        Cassandra311ProductionFixture fixture =
                new Cassandra311ProductionFixture(process, output, error);
        try {
            fixture.awaitPort(STORAGE_PORT, "internode messaging");
            fixture.awaitPort(NATIVE_PORT, "native transport");
            return fixture;
        } catch (Exception e) {
            fixture.stopForcibly();
            throw e;
        }
    }

    void assertRunning(String context) throws IOException {
        if (!process.isAlive()) {
            throw new AssertionError(context + "; production Cassandra exited with "
                    + process.exitValue() + logs());
        }
    }

    void stop() throws Exception {
        if (!process.isAlive()) {
            throw new AssertionError("Production Cassandra exited before fixture shutdown"
                    + logs());
        }
        process.destroy();
        if (!process.waitFor(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            stopForcibly();
            throw new AssertionError("Production Cassandra did not stop after SIGTERM" + logs());
        }
        if (process.exitValue() != 143 && process.exitValue() != 0) {
            throw new AssertionError("Production Cassandra stopped with exit code "
                    + process.exitValue() + logs());
        }
    }

    private void awaitPort(int port, String endpoint) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(START_TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new AssertionError("Production Cassandra exited during startup with "
                        + process.exitValue() + logs());
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(
                        InetAddress.getByName("127.0.0.1"), port), 250);
                return;
            } catch (IOException notReady) {
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Timed out waiting for production Cassandra " + endpoint
                + " on port " + port + logs());
    }

    private void stopForcibly() throws InterruptedException {
        if (process.isAlive()) {
            process.destroyForcibly();
            process.waitFor(30, TimeUnit.SECONDS);
        }
    }

    private String logs() throws IOException {
        return "\nproduction.out:\n" + read(output)
                + "\nproduction.err:\n" + read(error);
    }

    private static String read(Path path) throws IOException {
        return Files.isRegularFile(path)
                ? new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
                : "<not created>";
    }

    private static void prepareDirectories(Path root) throws IOException {
        Files.createDirectories(root.resolve("conf"));
        Files.createDirectories(root.resolve("data"));
        Files.createDirectories(root.resolve("commitlog"));
        Files.createDirectories(root.resolve("hints"));
        Files.createDirectories(root.resolve("saved_caches"));
        Files.createDirectories(root.resolve("logs"));
        Files.createDirectories(root.resolve("runtime/cdc_raw"));
        Files.createDirectories(root.resolve("runtime/tmp"));
    }

    private static String classpath(Path cassandraHome) throws IOException {
        List<Path> entries = new ArrayList<>();
        entries.add(cassandraHome.resolve("conf"));
        try (Stream<Path> paths = Files.list(cassandraHome.resolve("lib"))) {
            entries.addAll(paths.filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList()));
        }
        return entries.stream().map(Path::toString)
                .collect(Collectors.joining(File.pathSeparator));
    }

    private static String yaml(Path root) {
        return "cluster_name: '" + CLUSTER_NAME + "'\n"
                + "authenticator: AllowAllAuthenticator\n"
                + "authorizer: AllowAllAuthorizer\n"
                + "role_manager: CassandraRoleManager\n"
                + "internode_authenticator: "
                + "org.apache.cassandra.auth.AllowAllInternodeAuthenticator\n"
                + "partitioner: org.apache.cassandra.dht.Murmur3Partitioner\n"
                + "auto_bootstrap: false\n"
                + "num_tokens: 1\n"
                + "initial_token: '-9223372036854775808'\n"
                + "hinted_handoff_enabled: false\n"
                + "max_hint_window_in_ms: 0\n"
                + "seed_provider:\n"
                + "  - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n"
                + "    parameters:\n"
                + "      - seeds: '127.0.0.1'\n"
                + "listen_address: 127.0.0.1\n"
                + "broadcast_address: 127.0.0.1\n"
                + "storage_port: " + STORAGE_PORT + "\n"
                + "ssl_storage_port: 7001\n"
                + "start_rpc: false\n"
                + "rpc_address: 127.0.0.1\n"
                + "broadcast_rpc_address: 127.0.0.1\n"
                + "rpc_port: 9160\n"
                + "start_native_transport: true\n"
                + "native_transport_port: " + NATIVE_PORT + "\n"
                + "native_transport_max_threads: 16\n"
                + "endpoint_snitch: SimpleSnitch\n"
                + "dynamic_snitch: false\n"
                + "internode_compression: none\n"
                + "commitlog_sync: batch\n"
                + "commitlog_sync_batch_window_in_ms: 2\n"
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
    }

    private static String quote(Path path) {
        return "'" + path.toAbsolutePath().normalize().toString().replace("'", "''") + "'";
    }
}
