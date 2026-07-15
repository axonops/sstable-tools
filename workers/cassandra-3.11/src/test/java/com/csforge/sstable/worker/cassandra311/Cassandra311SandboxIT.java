package com.csforge.sstable.worker.cassandra311;

import com.csforge.sstable.worker.api.WorkerEndpoint;
import com.csforge.sstable.workspace.SourceInventory;
import com.csforge.sstable.workspace.WorkspaceLock;
import com.csforge.sstable.workspace.WorkspaceManifest;
import com.csforge.sstable.workspace.WorkspaceRepository;
import com.csforge.sstable.workspace.WorkspaceState;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** End-to-end smoke test against an unpacked final Cassandra 3.11 distribution. */
public class Cassandra311SandboxIT {
    private static final long COMMAND_TIMEOUT_SECONDS = 180;

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void startsCqlshAndDrainsIsolatedNode() throws Exception {
        Path cassandraHome = requiredDirectory("sandbox.it.cassandra.home");
        Path javaHome = requiredDirectory("sandbox.it.java.home");
        Path toolJar = requiredFile("sandbox.it.jar");
        Path source = createSstableSource();
        Path workspace = temporary.newFolder("workspace").toPath();
        Path productionRoot = temporary.newFolder("production").toPath();
        initializeImportedWorkspace(source, workspace);
        Map<String, String> installationBefore = fileMetadata(cassandraHome);
        Cassandra311ProductionFixture production = Cassandra311ProductionFixture.start(
                cassandraHome, javaHome, productionRoot);
        Path cqlsh = cassandraHome.resolve("bin/cqlsh");
        assertProductionVersion(cqlsh);
        assertProductionIsolated(cqlsh, "before worker startup");

        CommandResult start = null;
        boolean workerRunning = false;
        try {
            start = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(start.output + workerError(workspace), 0, start.exitCode);
            workerRunning = true;
            String[] nativeEndpoint = property(start.output, "worker.native").split(":", 2);
            Assert.assertEquals(start.output, 2, nativeEndpoint.length);

            CommandResult version = runCqlsh(Arrays.asList(
                    cqlsh.toString(), nativeEndpoint[0], nativeEndpoint[1], "-e",
                    "SHOW VERSION"));
            Assert.assertEquals(version.output, 0, version.exitCode);
            Assert.assertTrue(version.output, version.output.contains("Cassandra 3.11.19"));
            Assert.assertTrue(version.output, version.output.contains("Native protocol v4"));

            CommandResult mutate = runCqlsh(Arrays.asList(
                    cqlsh.toString(), nativeEndpoint[0], nativeEndpoint[1], "-e",
                    "CREATE KEYSPACE sandbox_it WITH replication = "
                            + "{'class': 'SimpleStrategy', 'replication_factor': 1}; "
                            + "CREATE TABLE sandbox_it.items "
                            + "(id int PRIMARY KEY, value text); "
                            + "INSERT INTO sandbox_it.items (id, value) VALUES (1, 'before'); "
                            + "UPDATE sandbox_it.items SET value = 'after' WHERE id = 1;"));
            Assert.assertEquals(mutate.output, 0, mutate.exitCode);

            CommandResult status = run(command(controllerJava(), toolJar,
                    "workspace", "status", workspace.toString()));
            Assert.assertEquals(status.output, 0, status.exitCode);
            Assert.assertTrue(status.output, status.output.contains("workspace.state=RUNNING"));
            Assert.assertTrue(status.output, status.output.contains("worker.status=RUNNING"));

            WorkerEndpoint firstEndpoint = WorkerEndpoint.read(
                    workspace.resolve("state/worker.properties"));
            assertWorkerEndpoint(firstEndpoint, nativeEndpoint);
            CommandResult kill = run(Arrays.asList("/bin/kill", "-KILL",
                    Long.toString(firstEndpoint.pid())));
            Assert.assertEquals(kill.output, 0, kill.exitCode);
            waitForProcessExit(firstEndpoint.pid());
            workerRunning = false;
            production.assertRunning("Worker SIGKILL affected production Cassandra");
            assertProductionIsolated(cqlsh, "after worker SIGKILL");

            CommandResult failedStatus = run(command(controllerJava(), toolJar,
                    "workspace", "status", workspace.toString()));
            Assert.assertNotEquals(failedStatus.output, 0, failedStatus.exitCode);
            Assert.assertEquals(WorkspaceState.FAILED_RECOVERABLE,
                    WorkspaceRepository.open(workspace).load().state());

            CommandResult recover = run(command(controllerJava(), toolJar,
                    "workspace", "recover", workspace.toString()));
            Assert.assertEquals(recover.output, 0, recover.exitCode);
            Assert.assertTrue(recover.output,
                    recover.output.contains("workspace.state=STOPPED"));

            start = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(start.output + workerError(workspace), 0, start.exitCode);
            workerRunning = true;
            nativeEndpoint = property(start.output, "worker.native").split(":", 2);
            WorkerEndpoint restartedEndpoint = WorkerEndpoint.read(
                    workspace.resolve("state/worker.properties"));
            assertWorkerEndpoint(restartedEndpoint, nativeEndpoint);
            CommandResult replayed = runCqlsh(Arrays.asList(
                    cqlsh.toString(), nativeEndpoint[0], nativeEndpoint[1], "-e",
                    "SELECT value FROM sandbox_it.items WHERE id = 1;"));
            Assert.assertEquals(replayed.output, 0, replayed.exitCode);
            Assert.assertTrue(replayed.output, replayed.output.contains("after"));

            CommandResult stop = run(command(controllerJava(), toolJar,
                    "workspace", "stop", workspace.toString()));
            Assert.assertEquals(stop.output, 0, stop.exitCode);
            Assert.assertTrue(stop.output, stop.output.contains("workspace.state=STOPPED"));
            workerRunning = false;
            production.assertRunning("Graceful worker stop affected production Cassandra");
            assertProductionIsolated(cqlsh, "after graceful worker stop");
        } finally {
            try {
                if (workerRunning && start != null && start.exitCode == 0) {
                    CommandResult stop = run(command(controllerJava(), toolJar,
                            "workspace", "stop", workspace.toString()));
                    Assert.assertEquals(stop.output, 0, stop.exitCode);
                    Assert.assertTrue(stop.output,
                            stop.output.contains("workspace.state=STOPPED"));
                }
            } finally {
                production.stop();
            }
        }

        WorkerEndpoint endpoint = WorkerEndpoint.read(
                workspace.resolve("state/worker.properties"));
        Assert.assertEquals(WorkerEndpoint.Status.STOPPED, endpoint.status());
        SourceInventory.capture(Collections.singletonList(source)).verifyUnchanged();
        Assert.assertEquals("Cassandra installation was modified", installationBefore,
                fileMetadata(cassandraHome));
    }

    private static void assertProductionVersion(Path cqlsh) throws Exception {
        CommandResult version = runCqlsh(Arrays.asList(cqlsh.toString(), "127.0.0.1",
                Integer.toString(Cassandra311ProductionFixture.NATIVE_PORT), "-e",
                "SHOW VERSION"));
        Assert.assertEquals(version.output, 0, version.exitCode);
        Assert.assertTrue(version.output, version.output.contains("Cassandra 3.11.19"));
        Assert.assertTrue(version.output, version.output.contains("Native protocol v4"));
    }

    private static void assertProductionIsolated(Path cqlsh, String phase) throws Exception {
        CommandResult query = runCqlsh(Arrays.asList(cqlsh.toString(), "127.0.0.1",
                Integer.toString(Cassandra311ProductionFixture.NATIVE_PORT), "-e",
                "SELECT cluster_name FROM system.local; SELECT peer FROM system.peers;"));
        Assert.assertEquals(phase + ":\n" + query.output, 0, query.exitCode);
        Assert.assertTrue(phase + ":\n" + query.output,
                query.output.contains(Cassandra311ProductionFixture.CLUSTER_NAME));
        Assert.assertTrue(phase + ":\n" + query.output,
                query.output.contains("(0 rows)"));
    }

    private static void assertWorkerEndpoint(WorkerEndpoint endpoint,
                                             String[] reportedNativeEndpoint) {
        Assert.assertEquals("127.0.0.1", endpoint.nativeAddress());
        Assert.assertEquals("127.0.0.1", endpoint.controlAddress());
        Assert.assertEquals(reportedNativeEndpoint[0], endpoint.nativeAddress());
        Assert.assertEquals(Integer.parseInt(reportedNativeEndpoint[1]), endpoint.nativePort());
        Assert.assertNotEquals(Cassandra311ProductionFixture.STORAGE_PORT,
                endpoint.nativePort());
        Assert.assertNotEquals(Cassandra311ProductionFixture.NATIVE_PORT,
                endpoint.nativePort());
        Assert.assertNotEquals(Cassandra311ProductionFixture.STORAGE_PORT,
                endpoint.controlPort());
        Assert.assertNotEquals(Cassandra311ProductionFixture.NATIVE_PORT,
                endpoint.controlPort());
        Assert.assertNotEquals(endpoint.nativePort(), endpoint.controlPort());
    }

    private Path createSstableSource() throws IOException {
        Path source = temporary.newFolder("source").toPath();
        Files.write(source.resolve("mc-1-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("mc-1-big-Data.db"),
                "data".getBytes(StandardCharsets.UTF_8));
        Files.write(source.resolve("mc-1-big-Statistics.db"),
                "statistics".getBytes(StandardCharsets.UTF_8));
        return source;
    }

    private static void initializeImportedWorkspace(Path source, Path workspace)
            throws Exception {
        WorkspaceRepository repository = WorkspaceRepository.createAt(workspace);
        WorkspaceManifest manifest = WorkspaceManifest.create(
                SourceInventory.capture(Collections.singletonList(source)));
        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            manifest = manifest.transitionTo(WorkspaceState.VALIDATED);
            repository.save(lock, manifest);
            repository.save(lock, manifest.transitionTo(WorkspaceState.IMPORTED));
        }
    }

    private static Path requiredDirectory(String property) {
        Path value = Paths.get(requiredProperty(property)).toAbsolutePath().normalize();
        Assert.assertTrue(property + " is not a directory: " + value,
                Files.isDirectory(value));
        return value;
    }

    private static Path requiredFile(String property) {
        Path value = Paths.get(requiredProperty(property)).toAbsolutePath().normalize();
        Assert.assertTrue(property + " is not a file: " + value, Files.isRegularFile(value));
        return value;
    }

    private static String requiredProperty(String property) {
        String value = System.getProperty(property);
        Assert.assertNotNull("Missing system property " + property, value);
        Assert.assertFalse("Empty system property " + property, value.trim().isEmpty());
        return value;
    }

    private static String controllerJava() {
        return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static String property(String output, String key) {
        String prefix = key + "=";
        for (String line : output.split("\\R")) {
            if (line.startsWith(prefix)) {
                return line.substring(prefix.length());
            }
        }
        Assert.fail("Missing " + key + " in command output:\n" + output);
        return "";
    }

    private static java.util.List<String> command(String first, Path jar, String... arguments) {
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add(first);
        command.add("-jar");
        command.add(jar.toString());
        command.addAll(Arrays.asList(arguments));
        return command;
    }

    private static CommandResult run(java.util.List<String> command) throws Exception {
        return run(command, Collections.emptyMap());
    }

    private static CommandResult runCqlsh(java.util.List<String> command) throws Exception {
        return run(command, Collections.singletonMap("PYTHONDONTWRITEBYTECODE", "1"));
    }

    private static CommandResult run(java.util.List<String> command,
                                     Map<String, String> environment) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        builder.environment().remove("_JAVA_OPTIONS");
        builder.environment().putAll(environment);
        Process process = builder.start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Thread reader = new Thread(() -> copy(process.getInputStream(), output),
                "sandbox-it-output");
        reader.start();
        if (!process.waitFor(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            reader.join(TimeUnit.SECONDS.toMillis(10));
            Assert.fail("Command timed out: " + command + "\n" + utf8(output));
        }
        reader.join(TimeUnit.SECONDS.toMillis(10));
        return new CommandResult(process.exitValue(), utf8(output));
    }

    private static void copy(InputStream input, ByteArrayOutputStream output) {
        byte[] buffer = new byte[8192];
        try (InputStream source = input) {
            int read;
            while ((read = source.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String utf8(ByteArrayOutputStream output) {
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String workerError(Path workspace) {
        Path error = workspace.resolve("logs/worker.err");
        try {
            return Files.isRegularFile(error)
                    ? "\nworker.err:\n" + new String(Files.readAllBytes(error),
                    StandardCharsets.UTF_8)
                    : "\nworker.err was not created";
        } catch (IOException e) {
            return "\nworker.err could not be read: " + e;
        }
    }

    private static void waitForProcessExit(long pid) throws Exception {
        Path process = Paths.get("/proc", Long.toString(pid));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (Files.exists(process) && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        Assert.assertFalse("Worker process did not exit after SIGKILL: " + pid,
                Files.exists(process));
    }

    private static Map<String, String> fileMetadata(Path root) throws Exception {
        Map<String, String> files = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                try {
                    files.put(root.relativize(path).toString(), Files.size(path) + ":"
                            + Files.getLastModifiedTime(path).toMillis());
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        return files;
    }

    private static final class CommandResult {
        private final int exitCode;
        private final String output;

        private CommandResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
