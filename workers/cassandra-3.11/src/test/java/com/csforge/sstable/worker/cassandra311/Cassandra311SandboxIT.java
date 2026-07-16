package com.csforge.sstable.worker.cassandra311;

import com.csforge.sstable.worker.api.WorkerEndpoint;
import com.csforge.sstable.workspace.SourceInventory;
import com.csforge.sstable.workspace.WorkspaceRepository;
import com.csforge.sstable.workspace.WorkspaceState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
        Path fixtureDirectory = requiredDirectory("sandbox.it.fixture.directory");
        Path source = createSstableSource(fixtureDirectory);
        SourceInventory capturedSource = SourceInventory.capture(
                Collections.singletonList(source));
        Path schema = createSchemaBundle();
        Path workspace = temporary.newFolder("workspace").toPath();
        Path productionRoot = temporary.newFolder("production").toPath();
        Map<String, String> installationBefore = fileMetadata(cassandraHome);
        Cassandra311ProductionFixture production = Cassandra311ProductionFixture.start(
                cassandraHome, javaHome, productionRoot);
        Path cqlsh = cassandraHome.resolve("bin/cqlsh");
        assertProductionVersion(cqlsh);
        assertProductionIsolated(cqlsh, "before worker startup");

        CommandResult start = null;
        boolean workerRunning = false;
        try {
            Path mismatchWorkspace = temporary.newFolder("mismatch-workspace").toPath();
            Path mismatchSchema = createSchemaBundle("text");
            CommandResult mismatchCreate = run(command(controllerJava(), toolJar,
                    "workspace", "create", mismatchWorkspace.toString(),
                    "--sstables", source.toString(), "--schema",
                    mismatchSchema.toString()));
            Assert.assertEquals(mismatchCreate.output, 0, mismatchCreate.exitCode);
            CommandResult mismatchImport = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", mismatchWorkspace.toString()));
            Assert.assertNotEquals(mismatchImport.output, 0, mismatchImport.exitCode);
            Assert.assertEquals(WorkspaceState.FAILED_RECOVERABLE,
                    WorkspaceRepository.open(mismatchWorkspace).load().state());
            Assert.assertEquals("Schema mismatch left imported data components", 0,
                    countDataComponents(mismatchWorkspace.resolve("data/blog")));
            Assert.assertTrue(importError(mismatchWorkspace),
                    importError(mismatchWorkspace).contains(
                            "serialization header does not exactly match"));
            capturedSource.verifyUnchanged();
            production.assertRunning("Rejected worker import affected production Cassandra");

            Path corruptSource = createSstableSource(fixtureDirectory,
                    "corrupt-source", "ma-2-big-");
            Path corruptData = corruptSource.resolve("ma-2-big-Data.db");
            byte[] corruptBytes = Files.readAllBytes(corruptData);
            corruptBytes[corruptBytes.length / 2] ^= 0x01;
            Files.write(corruptData, corruptBytes);
            SourceInventory capturedCorrupt = SourceInventory.capture(
                    Collections.singletonList(corruptSource));
            assertRejectedImport(toolJar, cassandraHome, javaHome,
                    "corrupt-workspace", corruptSource, capturedCorrupt,
                    createSchemaBundle("bigint"), "Corrupted SSTable :");

            Path missingIndexSource = createSstableSource(fixtureDirectory,
                    "missing-index-source", "ma-2-big-");
            Path missingIndexToc = missingIndexSource.resolve("ma-2-big-TOC.txt");
            java.util.List<String> components = Files.readAllLines(
                    missingIndexToc, StandardCharsets.UTF_8);
            Assert.assertTrue("Fixture TOC does not declare Index.db",
                    components.remove("Index.db"));
            Files.write(missingIndexToc, components, StandardCharsets.UTF_8);
            Files.delete(missingIndexSource.resolve("ma-2-big-Index.db"));
            SourceInventory capturedMissingIndex = SourceInventory.capture(
                    Collections.singletonList(missingIndexSource));
            assertRejectedImport(toolJar, cassandraHome, javaHome,
                    "missing-index-workspace", missingIndexSource, capturedMissingIndex,
                    createSchemaBundle("bigint"), "missing required component Index.db");

            Path unsupportedSource = createSstableSource(fixtureDirectory,
                    "unsupported-source", "ma-2-big-");
            renameDescriptor(unsupportedSource, "ma-2-big-", "zz-2-big-");
            SourceInventory capturedUnsupported = SourceInventory.capture(
                    Collections.singletonList(unsupportedSource));
            assertRejectedImport(toolJar, cassandraHome, javaHome,
                    "unsupported-workspace", unsupportedSource, capturedUnsupported,
                    createSchemaBundle("bigint"),
                    "Unsupported Cassandra 3.11 SSTable format zz-big");
            production.assertRunning("Failure-matrix imports affected production Cassandra");

            String collisionTableId = "11111111-1111-1111-1111-111111111111";
            Path collisionWorkspace = temporary.newFolder("collision-workspace").toPath();
            Path collisionSchema = createSchemaBundle("bigint", collisionTableId);
            CommandResult collisionCreate = run(command(controllerJava(), toolJar,
                    "workspace", "create", collisionWorkspace.toString(),
                    "--sstables", source.toString(), "--schema",
                    collisionSchema.toString()));
            Assert.assertEquals(collisionCreate.output, 0, collisionCreate.exitCode);
            Path collisionDirectory = Files.createDirectories(collisionWorkspace.resolve(
                    "data/blog/users-" + collisionTableId.replace("-", "")));
            Files.write(collisionDirectory.resolve("ma-1-big-TOC.txt"),
                    "collision".getBytes(StandardCharsets.UTF_8));
            CommandResult collisionImport = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", collisionWorkspace.toString()));
            Assert.assertNotEquals(collisionImport.output, 0, collisionImport.exitCode);
            Assert.assertEquals(WorkspaceState.FAILED_RECOVERABLE,
                    WorkspaceRepository.open(collisionWorkspace).load().state());
            Assert.assertFalse("Collision cleanup left a partial table directory",
                    Files.exists(collisionDirectory));
            CommandResult collisionRecover = run(command(controllerJava(), toolJar,
                    "workspace", "recover", collisionWorkspace.toString()));
            Assert.assertEquals(collisionRecover.output, 0, collisionRecover.exitCode);
            CommandResult collisionRetry = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", collisionWorkspace.toString()));
            Assert.assertEquals(collisionRetry.output + importError(collisionWorkspace),
                    0, collisionRetry.exitCode);
            Assert.assertEquals(WorkspaceState.IMPORTED,
                    WorkspaceRepository.open(collisionWorkspace).load().state());
            capturedSource.verifyUnchanged();
            production.assertRunning("Collision import affected production Cassandra");

            Path mbSource = createSstableSource(fixtureDirectory, "mb-source", "mb-1-big-");
            SourceInventory capturedMb = SourceInventory.capture(
                    Collections.singletonList(mbSource));
            Path mbSchema = createCompositeSchemaBundle();
            Path mbWorkspace = temporary.newFolder("mb-workspace").toPath();
            CommandResult mbCreate = run(command(controllerJava(), toolJar,
                    "workspace", "create", mbWorkspace.toString(),
                    "--sstables", mbSource.toString(), "--schema", mbSchema.toString()));
            Assert.assertEquals(mbCreate.output, 0, mbCreate.exitCode);
            CommandResult mbImport = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", mbWorkspace.toString()));
            Assert.assertNotEquals(mbImport.output, 0, mbImport.exitCode);
            Assert.assertTrue(importError(mbWorkspace),
                    importError(mbWorkspace).contains("partitioner does not match"));
            Assert.assertEquals(0, countDataComponents(mbWorkspace.resolve("data/fixture")));
            capturedMb.verifyUnchanged();

            Path mcSource = createSstableSource(fixtureDirectory, "mc-source", "mc-1-big-");
            SourceInventory capturedMc = SourceInventory.capture(
                    Collections.singletonList(mcSource));
            Path mcSchema = createTemperatureSchemaBundle();
            Path mcWorkspace = temporary.newFolder("mc-workspace").toPath();
            CommandResult mcCreate = run(command(controllerJava(), toolJar,
                    "workspace", "create", mcWorkspace.toString(),
                    "--sstables", mcSource.toString(), "--schema", mcSchema.toString()));
            Assert.assertEquals(mcCreate.output, 0, mcCreate.exitCode);
            CommandResult mcImport = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", mcWorkspace.toString()));
            Assert.assertEquals(mcImport.output + importError(mcWorkspace),
                    0, mcImport.exitCode);
            Assert.assertTrue(mcImport.output,
                    mcImport.output.contains("import.table=fixture.temperature_by_day"));
            Assert.assertTrue(mcImport.output,
                    mcImport.output.contains("import.logicalRows=4"));
            Assert.assertTrue(mcImport.output,
                    mcImport.output.contains("import.liveSstables=1"));
            capturedMc.verifyUnchanged();
            production.assertRunning("SSTable format-range import affected production Cassandra");

            CommandResult create = run(command(controllerJava(), toolJar,
                    "workspace", "create", workspace.toString(),
                    "--sstables", source.toString(), "--schema", schema.toString()));
            Assert.assertEquals(create.output, 0, create.exitCode);
            Assert.assertTrue(create.output,
                    create.output.contains("workspace.state=VALIDATED"));

            CommandResult imported = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", workspace.toString()));
            Assert.assertEquals(imported.output + importError(workspace),
                    0, imported.exitCode);
            Assert.assertTrue(imported.output,
                    imported.output.contains("workspace.state=IMPORTED"));
            Assert.assertTrue(imported.output,
                    imported.output.contains("import.table=blog.users"));
            Assert.assertTrue(imported.output,
                    imported.output.contains("import.logicalRows=1"));
            production.assertRunning("Worker import affected production Cassandra");
            assertProductionIsolated(cqlsh, "after worker import");

            start = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(start.output + workerError(workspace), 0, start.exitCode);
            workerRunning = true;
            String[] nativeEndpoint = property(start.output, "worker.native").split(":", 2);
            Assert.assertEquals(start.output, 2, nativeEndpoint.length);
            Path cqlshrc = Paths.get(property(start.output, "worker.cqlshrc"));
            Assert.assertEquals(workspace.resolve("state/cqlshrc"), cqlshrc);
            NativeCredentials credentials = readCredentials(cqlshrc);
            Assert.assertEquals("sstable_workspace", credentials.username);

            CommandResult unauthenticated = runCqlsh(Arrays.asList(
                    cqlsh.toString(), nativeEndpoint[0], nativeEndpoint[1], "-e",
                    "SHOW VERSION"));
            Assert.assertNotEquals(unauthenticated.output, 0, unauthenticated.exitCode);
            Path wrongCqlshrc = createCqlshrc("wrong-password");
            CommandResult wrongPassword = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint,
                    wrongCqlshrc, "SHOW VERSION"));
            Assert.assertNotEquals(wrongPassword.output, 0, wrongPassword.exitCode);
            Assert.assertTrue(wrongPassword.output,
                    wrongPassword.output.contains("incorrect"));

            CommandResult version = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint,
                    cqlshrc, "SHOW VERSION"));
            Assert.assertEquals(version.output, 0, version.exitCode);
            Assert.assertTrue(version.output, version.output.contains("Cassandra 3.11.19"));
            Assert.assertTrue(version.output, version.output.contains("Native protocol v4"));

            CommandResult sourceRow = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint, cqlshrc,
                    "SELECT user_name, password, state FROM blog.users "
                            + "WHERE user_name = 'frodo';"));
            Assert.assertEquals(sourceRow.output, 0, sourceRow.exitCode);
            Assert.assertTrue(sourceRow.output, sourceRow.output.contains("frodo"));
            Assert.assertTrue(sourceRow.output, sourceRow.output.contains("pass@"));

            CommandResult mutate = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint, cqlshrc,
                    "INSERT INTO blog.users (user_name, password, gender, state, birth_year) "
                            + "VALUES ('sam', 'inserted', 'male', 'CA', 1980); "
                            + "UPDATE blog.users SET password = 'after' "
                            + "WHERE user_name = 'frodo';"));
            Assert.assertEquals(mutate.output, 0, mutate.exitCode);

            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "DELETE FROM blog.users WHERE user_name = 'sam';");
            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "TRUNCATE blog.users;");
            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "CREATE TABLE blog.forbidden (id text PRIMARY KEY);");
            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "BEGIN BATCH INSERT INTO blog.users "
                            + "(user_name, password) VALUES ('batch', 'forbidden'); "
                            + "APPLY BATCH;");
            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "UPDATE blog.users SET password = 'conditional' "
                            + "WHERE user_name = 'frodo' IF password = 'after';");
            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "UPDATE system.local SET cluster_name = 'compromised' "
                            + "WHERE key = 'local';");
            assertPolicyRejected(cqlsh, nativeEndpoint, cqlshrc,
                    "SELECT * FROM system.batchlog;");

            CommandResult prepared = runPreparedPolicyCheck(cassandraHome,
                    nativeEndpoint[0], nativeEndpoint[1], credentials);
            Assert.assertEquals(prepared.output, 0, prepared.exitCode);
            assertGuardedState(cqlsh, nativeEndpoint, cqlshrc);

            CommandResult status = run(command(controllerJava(), toolJar,
                    "workspace", "status", workspace.toString()));
            Assert.assertEquals(status.output, 0, status.exitCode);
            Assert.assertTrue(status.output, status.output.contains("workspace.state=RUNNING"));
            Assert.assertTrue(status.output, status.output.contains("worker.status=RUNNING"));

            WorkerEndpoint firstEndpoint = WorkerEndpoint.read(
                    workspace.resolve("state/worker.properties"));
            assertWorkerEndpoint(firstEndpoint, nativeEndpoint);
            Path jcmd = javaHome.resolve("bin/jcmd");
            Assert.assertTrue("Selected Cassandra JDK has no jcmd: " + jcmd,
                    Files.isRegularFile(jcmd));
            CommandResult attach = run(Arrays.asList(jcmd.toString(),
                    Long.toString(firstEndpoint.pid()), "VM.version"));
            Assert.assertNotEquals("JVM attach unexpectedly reached the workspace worker:\n"
                    + attach.output, 0, attach.exitCode);
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
            Assert.assertFalse(Files.exists(cqlshrc));

            start = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(start.output + workerError(workspace), 0, start.exitCode);
            workerRunning = true;
            nativeEndpoint = property(start.output, "worker.native").split(":", 2);
            cqlshrc = Paths.get(property(start.output, "worker.cqlshrc"));
            NativeCredentials restartedCredentials = readCredentials(cqlshrc);
            Assert.assertNotEquals(credentials.password, restartedCredentials.password);
            WorkerEndpoint restartedEndpoint = WorkerEndpoint.read(
                    workspace.resolve("state/worker.properties"));
            assertWorkerEndpoint(restartedEndpoint, nativeEndpoint);
            CommandResult replayed = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint, cqlshrc,
                    "SELECT user_name, password FROM blog.users;"));
            Assert.assertEquals(replayed.output, 0, replayed.exitCode);
            Assert.assertTrue(replayed.output, replayed.output.contains("prepared"));
            Assert.assertTrue(replayed.output, replayed.output.contains("inserted"));
            Assert.assertTrue(replayed.output, replayed.output.contains("2 rows"));

            CommandResult stop = run(command(controllerJava(), toolJar,
                    "workspace", "stop", workspace.toString()));
            Assert.assertEquals(stop.output, 0, stop.exitCode);
            Assert.assertTrue(stop.output, stop.output.contains("workspace.state=STOPPED"));
            workerRunning = false;
            Assert.assertFalse(Files.exists(cqlshrc));
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
        capturedSource.verifyUnchanged();
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

    private static void assertPolicyRejected(Path cqlsh,
                                             String[] endpoint,
                                             Path cqlshrc,
                                             String cql) throws Exception {
        CommandResult rejected = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc, cql));
        Assert.assertNotEquals("Forbidden CQL unexpectedly succeeded:\n" + cql + "\n"
                + rejected.output, 0, rejected.exitCode);
        Assert.assertTrue(rejected.output, rejected.output.contains("SSTABLE_TOOLS_POLICY"));
    }

    private static void assertGuardedState(Path cqlsh,
                                           String[] endpoint,
                                           Path cqlshrc) throws Exception {
        CommandResult rows = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                "SELECT user_name, password FROM blog.users;"));
        Assert.assertEquals(rows.output, 0, rows.exitCode);
        Assert.assertTrue(rows.output, rows.output.contains("frodo"));
        Assert.assertTrue(rows.output, rows.output.contains("prepared"));
        Assert.assertTrue(rows.output, rows.output.contains("sam"));
        Assert.assertFalse(rows.output, rows.output.contains("batch"));
        Assert.assertTrue(rows.output, rows.output.contains("2 rows"));

        CommandResult schema = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                "SELECT table_name FROM system_schema.tables "
                        + "WHERE keyspace_name = 'blog';"));
        Assert.assertEquals(schema.output, 0, schema.exitCode);
        Assert.assertTrue(schema.output, schema.output.contains("users"));
        Assert.assertFalse(schema.output, schema.output.contains("forbidden"));
        Assert.assertTrue(schema.output, schema.output.contains("1 rows"));
    }

    private static CommandResult runPreparedPolicyCheck(Path cassandraHome,
                                                         String host,
                                                         String port,
                                                         NativeCredentials credentials)
            throws Exception {
        Path lib = cassandraHome.resolve("lib");
        Path driver = singleMatch(lib, "cassandra-driver-internal-only-*.zip");
        String filename = driver.getFileName().toString();
        String prefix = "cassandra-driver-internal-only-";
        String version = filename.substring(prefix.length(), filename.length() - 4);

        List<Path> archives = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(
                lib, "*.zip")) {
            for (Path entry : entries) {
                archives.add(entry);
            }
        }
        archives.sort(Comparator.comparing(Path::toString));
        StringBuilder pythonPath = new StringBuilder(driver + "/cassandra-driver-" + version);
        for (Path archive : archives) {
            pythonPath.append(File.pathSeparatorChar).append(archive);
        }

        String script = "from cassandra.cluster import Cluster\n"
                + "from cassandra import ConsistencyLevel\n"
                + "from cassandra.auth import PlainTextAuthProvider\n"
                + "from cassandra.query import SimpleStatement\n"
                + "import os, sys\n"
                + "auth = PlainTextAuthProvider("
                + "username=os.environ['SSTABLE_TOOLS_TEST_USERNAME'], "
                + "password=os.environ['SSTABLE_TOOLS_TEST_PASSWORD'])\n"
                + "cluster = Cluster([sys.argv[1]], port=int(sys.argv[2]), "
                + "auth_provider=auth)\n"
                + "session = cluster.connect()\n"
                + "local_read = SimpleStatement("
                + "\"SELECT user_name FROM blog.users WHERE user_name = 'frodo'\", "
                + "consistency_level=ConsistencyLevel.LOCAL_ONE)\n"
                + "assert next(iter(session.execute(local_read))).user_name == 'frodo'\n"
                + "quorum_read = SimpleStatement("
                + "\"SELECT user_name FROM blog.users WHERE user_name = 'frodo'\", "
                + "consistency_level=ConsistencyLevel.QUORUM)\n"
                + "try:\n"
                + "    session.execute(quorum_read)\n"
                + "    raise AssertionError('QUORUM read unexpectedly succeeded')\n"
                + "except Exception as error:\n"
                + "    if 'SSTABLE_TOOLS_POLICY' not in str(error):\n"
                + "        raise\n"
                + "allowed = session.prepare("
                + "\"UPDATE blog.users SET password = ? WHERE user_name = ?\")\n"
                + "rejected_write = allowed.bind(('forbidden-all', 'frodo'))\n"
                + "rejected_write.consistency_level = ConsistencyLevel.ALL\n"
                + "try:\n"
                + "    session.execute(rejected_write)\n"
                + "    raise AssertionError('ALL prepared update unexpectedly succeeded')\n"
                + "except Exception as error:\n"
                + "    if 'SSTABLE_TOOLS_POLICY' not in str(error):\n"
                + "        raise\n"
                + "local_write = allowed.bind(('prepared', 'frodo'))\n"
                + "local_write.consistency_level = ConsistencyLevel.LOCAL_ONE\n"
                + "session.execute(local_write)\n"
                + "paged_direct = SimpleStatement("
                + "\"SELECT user_name FROM blog.users\", fetch_size=1, "
                + "consistency_level=ConsistencyLevel.ONE)\n"
                + "assert sorted(row.user_name for row in session.execute(paged_direct)) "
                + "== ['frodo', 'sam']\n"
                + "paged_prepared = session.prepare("
                + "\"SELECT user_name FROM blog.users\")\n"
                + "paged_prepared.fetch_size = 1\n"
                + "paged_rows = session.execute(paged_prepared.bind(()))\n"
                + "assert sorted(row.user_name for row in paged_rows) "
                + "== ['frodo', 'sam']\n"
                + "try:\n"
                + "    session.prepare(\"DELETE FROM blog.users WHERE user_name = ?\")\n"
                + "    raise AssertionError('forbidden prepared DELETE succeeded')\n"
                + "except Exception as error:\n"
                + "    if 'SSTABLE_TOOLS_POLICY' not in str(error):\n"
                + "        raise\n"
                + "finally:\n"
                + "    cluster.shutdown()\n";
        Map<String, String> environment = new TreeMap<>();
        environment.put("PYTHONDONTWRITEBYTECODE", "1");
        environment.put("PYTHONPATH", pythonPath.toString());
        environment.put("SSTABLE_TOOLS_TEST_USERNAME", credentials.username);
        environment.put("SSTABLE_TOOLS_TEST_PASSWORD", credentials.password);
        return run(Arrays.asList(python2(), "-c", script, host, port), environment);
    }

    private Path createCqlshrc(String password) throws IOException {
        Path path = Files.createTempFile(temporary.getRoot().toPath(), "wrong-cqlshrc-", ".ini");
        Files.write(path, Arrays.asList("[authentication]",
                "username = sstable_workspace", "password = " + password),
                StandardCharsets.US_ASCII);
        return path;
    }

    private static NativeCredentials readCredentials(Path path) throws IOException {
        String username = null;
        String password = null;
        for (String line : Files.readAllLines(path, StandardCharsets.US_ASCII)) {
            String trimmed = line.trim();
            if (trimmed.startsWith("username = ")) {
                username = trimmed.substring("username = ".length());
            } else if (trimmed.startsWith("password = ")) {
                password = trimmed.substring("password = ".length());
            }
        }
        if (username == null || password == null) {
            throw new IOException("Invalid generated cqlshrc " + path);
        }
        return new NativeCredentials(username, password);
    }

    private static List<String> cqlshCommand(Path cqlsh,
                                             String[] endpoint,
                                             Path cqlshrc,
                                             String cql) {
        return Arrays.asList(cqlsh.toString(), "--cqlshrc", cqlshrc.toString(),
                endpoint[0], endpoint[1], "-e", cql);
    }

    private static Path singleMatch(Path directory, String glob) throws IOException {
        List<Path> matches = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(
                directory, glob)) {
            for (Path entry : entries) {
                matches.add(entry);
            }
        }
        if (matches.size() != 1) {
            throw new IOException("Expected one " + glob + " below " + directory
                    + " but found " + matches);
        }
        return matches.get(0);
    }

    private static String python2() throws Exception {
        for (String candidate : Arrays.asList("python", "python2.7", "pypy2.7", "pypy2")) {
            try {
                CommandResult version = run(Arrays.asList(candidate, "-c",
                        "import sys; sys.exit(0 if sys.version_info[:2] == (2, 7) else 1)"));
                if (version.exitCode == 0) {
                    return candidate;
                }
            } catch (IOException unavailable) {
                // Try the next common Python 2.7 executable name.
            }
        }
        throw new IOException("A Python 2.7 interpreter is required for prepared CQL tests");
    }

    private Path createSstableSource(Path fixtureDirectory) throws IOException {
        return createSstableSource(fixtureDirectory, "source", "ma-2-big-");
    }

    private Path createSstableSource(Path fixtureDirectory,
                                     String directoryName,
                                     String... prefixes) throws IOException {
        Path source = temporary.newFolder(directoryName).toPath();
        for (String prefix : prefixes) {
            try (java.nio.file.DirectoryStream<Path> fixtures = Files.newDirectoryStream(
                    fixtureDirectory, prefix + "*")) {
                for (Path fixture : fixtures) {
                    Files.copy(fixture, source.resolve(fixture.getFileName()),
                            StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
        return source;
    }

    private Path createSchemaBundle() throws IOException {
        return createSchemaBundle("bigint");
    }

    private Path createSchemaBundle(String birthYearType) throws IOException {
        return createSchemaBundle(birthYearType, null);
    }

    private Path createSchemaBundle(String birthYearType, String tableId)
            throws IOException {
        Path schema = Files.createTempFile(temporary.getRoot().toPath(),
                "schema-" + birthYearType + "-"
                        + (tableId == null ? "generated" : tableId) + "-", ".cql");
        Files.write(schema, Arrays.asList(
                "CREATE KEYSPACE blog WITH replication = {'class': "
                        + "'NetworkTopologyStrategy', 'dc1': 3};",
                "CREATE TABLE blog.users (",
                "  user_name varchar PRIMARY KEY,",
                "  password varchar,",
                "  gender varchar,",
                "  state varchar,",
                "  birth_year " + birthYearType,
                ")" + (tableId == null ? ";" : " WITH id = '" + tableId + "';")),
                StandardCharsets.UTF_8);
        return schema;
    }

    private void assertRejectedImport(Path toolJar,
                                      Path cassandraHome,
                                      Path javaHome,
                                      String workspaceName,
                                      Path source,
                                      SourceInventory captured,
                                      Path schema,
                                      String expectedError) throws Exception {
        Path workspace = temporary.newFolder(workspaceName).toPath();
        CommandResult create = run(command(controllerJava(), toolJar,
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString(), "--schema", schema.toString()));
        Assert.assertEquals(create.output, 0, create.exitCode);

        CommandResult imported = run(command(controllerJava(), toolJar,
                "--cassandra-home", cassandraHome.toString(),
                "--java-home", javaHome.toString(),
                "workspace", "import", workspace.toString()));

        Assert.assertNotEquals(imported.output, 0, imported.exitCode);
        Assert.assertEquals(WorkspaceState.FAILED_RECOVERABLE,
                WorkspaceRepository.open(workspace).load().state());
        Assert.assertEquals("Rejected import left user data components", 0,
                countDataComponents(workspace.resolve("data/blog")));
        Assert.assertTrue(importError(workspace),
                importError(workspace).contains(expectedError));
        Assert.assertTrue(new String(Files.readAllBytes(
                workspace.resolve("runtime/cassandra.yaml")), StandardCharsets.UTF_8)
                .contains("start_native_transport: false"));
        Assert.assertFalse(Files.exists(workspace.resolve("state/worker.properties")));
        captured.verifyUnchanged();
    }

    private static void renameDescriptor(Path source,
                                         String previousPrefix,
                                         String nextPrefix) throws IOException {
        Map<Path, Path> renames = new TreeMap<>();
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(source)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (name.startsWith(previousPrefix)) {
                    renames.put(entry, source.resolve(nextPrefix
                            + name.substring(previousPrefix.length())));
                }
            }
        }
        for (Map.Entry<Path, Path> rename : renames.entrySet()) {
            Files.move(rename.getKey(), rename.getValue());
        }
    }

    private Path createCompositeSchemaBundle() throws IOException {
        Path schema = temporary.newFile("schema-composites.cql").toPath();
        Files.write(schema, Arrays.asList(
                "CREATE KEYSPACE fixture WITH replication = {'class': "
                        + "'SimpleStrategy', 'replication_factor': 1};",
                "CREATE TABLE fixture.composites (",
                "  key1 varchar,",
                "  key2 varchar,",
                "  ckey1 varchar,",
                "  ckey2 varchar,",
                "  value bigint,",
                "  PRIMARY KEY ((key1, key2), ckey1, ckey2)",
                ");"), StandardCharsets.UTF_8);
        return schema;
    }

    private Path createTemperatureSchemaBundle() throws IOException {
        Path schema = temporary.newFile("schema-temperature.cql").toPath();
        Files.write(schema, Arrays.asList(
                "CREATE KEYSPACE fixture WITH replication = {'class': "
                        + "'SimpleStrategy', 'replication_factor': 1};",
                "CREATE TABLE fixture.temperature_by_day (",
                "  weatherstation_id text,",
                "  date text,",
                "  event_time timestamp,",
                "  temperature float,",
                "  PRIMARY KEY ((weatherstation_id, date), event_time)",
                ");"), StandardCharsets.UTF_8);
        return schema;
    }

    private static long countDataComponents(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(path -> Files.isRegularFile(path)
                    && path.getFileName().toString().endsWith("-Data.db")).count();
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

    private static String importError(Path workspace) {
        Path error = workspace.resolve("logs/import.err");
        try {
            return Files.isRegularFile(error)
                    ? "\nimport.err:\n" + new String(Files.readAllBytes(error),
                    StandardCharsets.UTF_8)
                    : "\nimport.err was not created";
        } catch (IOException e) {
            return "\nimport.err could not be read: " + e;
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

    private static final class NativeCredentials {
        private final String username;
        private final String password;

        private NativeCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
    }
}
