package com.axonops.sstable.worker.cassandra311;

import com.axonops.sstable.worker.api.WorkerEndpoint;
import com.axonops.sstable.workspace.ExportRecord;
import com.axonops.sstable.workspace.Hashing;
import com.axonops.sstable.workspace.ManifestFile;
import com.axonops.sstable.workspace.SourceComponent;
import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.SstableSet;
import com.axonops.sstable.workspace.WorkspaceFlushResult;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import com.axonops.sstable.workspace.WorkspaceState;
import com.axonops.sstable.workspace.WorkspaceVerificationResult;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
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
    public void mutatesSstablesGeneratedByStoppedStockCqlshNode() throws Exception {
        Path cassandraHome = requiredDirectory("sandbox.it.cassandra.home");
        Path javaHome = requiredDirectory("sandbox.it.java.home");
        Path toolJar = requiredFile("sandbox.it.jar");
        Path productionRoot = temporary.newFolder("cqlsh-source-node").toPath();
        Path source = temporary.newFolder("stopped-cqlsh-source").toPath();
        Path schema = createStoppedSourceSchemaBundle();
        Path cqlsh = cassandraHome.resolve("bin/cqlsh");

        Cassandra311ProductionFixture production = Cassandra311ProductionFixture.start(
                cassandraHome, javaHome, productionRoot);
        try {
            CommandResult seed = runCqlsh(Arrays.asList(cqlsh.toString(), "127.0.0.1",
                    Integer.toString(Cassandra311ProductionFixture.NATIVE_PORT), "-e",
                    "CREATE KEYSPACE stopped_source WITH replication = {'class': "
                            + "'SimpleStrategy', 'replication_factor': 1}; "
                            + "CREATE TABLE stopped_source.users ("
                            + "user_name text PRIMARY KEY, password text, state text); "
                            + "INSERT INTO stopped_source.users (user_name, password, state) "
                            + "VALUES ('source-user', 'initial', 'CA'); "
                            + "UPDATE stopped_source.users SET password = 'updated' "
                            + "WHERE user_name = 'source-user';"));
            Assert.assertEquals(seed.output, 0, seed.exitCode);

            CommandResult selected = runCqlsh(Arrays.asList(cqlsh.toString(), "127.0.0.1",
                    Integer.toString(Cassandra311ProductionFixture.NATIVE_PORT), "-e",
                    "SELECT user_name, password, state FROM stopped_source.users;"));
            Assert.assertEquals(selected.output, 0, selected.exitCode);
            Assert.assertTrue(selected.output, selected.output.contains("source-user"));
            Assert.assertTrue(selected.output, selected.output.contains("updated"));

            CommandResult flush = run(Arrays.asList(cassandraHome.resolve("bin/nodetool").toString(),
                    "-h", "127.0.0.1", "flush", "stopped_source", "users"));
            Assert.assertEquals(flush.output, 0, flush.exitCode);
        } finally {
            production.stop();
        }

        Path sourceTable = findGeneratedTable(productionRoot.resolve("data"),
                "stopped_source", "users");
        copyDirectory(sourceTable, source);
        Assert.assertTrue("Stock cqlsh source did not produce an SSTable",
                containsDataComponent(source));

        Path workspace = temporary.newFolder("stopped-cqlsh-workspace").toPath();
        CommandResult created = run(command(controllerJava(), toolJar,
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString(), "--schema", schema.toString()));
        Assert.assertEquals(created.output, 0, created.exitCode);
        CommandResult imported = run(command(controllerJava(), toolJar,
                "--cassandra-home", cassandraHome.toString(), "--java-home", javaHome.toString(),
                "workspace", "import", workspace.toString()));
        Assert.assertEquals(imported.output + importError(workspace), 0, imported.exitCode);

        boolean running = false;
        try {
            CommandResult started = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(), "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(started.output + workerError(workspace), 0, started.exitCode);
            running = true;
            String[] endpoint = property(started.output, "worker.native").split(":", 2);
            Path cqlshrc = Paths.get(property(started.output, "worker.cqlshrc"));

            CommandResult sourceRead = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                    "SELECT user_name, password, state FROM stopped_source.users "
                            + "WHERE user_name = 'source-user';"));
            Assert.assertEquals(sourceRead.output, 0, sourceRead.exitCode);
            Assert.assertTrue(sourceRead.output, sourceRead.output.contains("updated"));

            CommandResult mutation = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                    "INSERT INTO stopped_source.users (user_name, password, state) "
                            + "VALUES ('tool-user', 'created', 'NY'); "
                            + "UPDATE stopped_source.users SET password = 'changed' "
                            + "WHERE user_name = 'tool-user';"));
            Assert.assertEquals(mutation.output, 0, mutation.exitCode);
            CommandResult changed = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                    "SELECT password, state FROM stopped_source.users "
                            + "WHERE user_name = 'tool-user';"));
            Assert.assertEquals(changed.output, 0, changed.exitCode);
            Assert.assertTrue(changed.output, changed.output.contains("changed"));
            Assert.assertTrue(changed.output, changed.output.contains("NY"));

            CommandResult flushed = run(command(controllerJava(), toolJar,
                    "workspace", "flush", workspace.toString()));
            Assert.assertEquals(flushed.output + workerError(workspace), 0, flushed.exitCode);
            CommandResult exported = run(command(controllerJava(), toolJar,
                    "workspace", "export", workspace.toString(), "--mode", "delta",
                    "--output", temporary.newFolder("stopped-cqlsh-delta").toString()));
            Assert.assertEquals(exported.output + workerError(workspace), 0, exported.exitCode);
            Assert.assertTrue(exported.output, exported.output.contains("workspace.state=EXPORTED"));

            CommandResult stopped = run(command(controllerJava(), toolJar,
                    "workspace", "stop", workspace.toString()));
            Assert.assertEquals(stopped.output + workerError(workspace), 0, stopped.exitCode);
            running = false;
        } finally {
            if (running) {
                run(command(controllerJava(), toolJar, "workspace", "stop", workspace.toString()));
            }
        }
    }

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

        Path liveSource = Files.createDirectories(
                productionRoot.resolve("data/live-source"));
        copySstableFixtures(fixtureDirectory, liveSource, "ma-2-big-");
        Path rejectedLiveWorkspace = temporary.getRoot().toPath().resolve(
                "live-source-workspace");
        CommandResult rejectedLiveSource = run(command(controllerJava(), toolJar,
                "workspace", "create", rejectedLiveWorkspace.toString(),
                "--sstables", liveSource.toString(), "--schema", schema.toString()));
        Assert.assertNotEquals(rejectedLiveSource.output, 0, rejectedLiveSource.exitCode);
        Assert.assertTrue(rejectedLiveSource.output,
                rejectedLiveSource.output.contains("active Cassandra process"));
        Assert.assertTrue(rejectedLiveSource.output,
                rejectedLiveSource.output.contains("outside every live Cassandra data"));
        Assert.assertFalse("Live-source rejection left workspace artifacts",
                Files.exists(rejectedLiveWorkspace));

        CommandResult start = null;
        boolean workerRunning = false;
        try {
            long futureTimestampMicros = System.currentTimeMillis() * 1000L
                    + TimeUnit.DAYS.toMicros(365);
            Path futureSource = createFutureSstableSource(cassandraHome, javaHome,
                    futureTimestampMicros);
            SourceInventory capturedFutureSource = SourceInventory.capture(
                    Collections.singletonList(futureSource));
            assertFutureTimestampPolicies(toolJar, cassandraHome, javaHome,
                    futureSource, futureTimestampMicros);
            capturedFutureSource.verifyUnchanged();
            production.assertRunning("Future-timestamp workspaces affected production "
                    + "Cassandra");

            Path shapeSource = createShapeSstableSource(cassandraHome, javaHome);
            SourceInventory capturedShapeSource = SourceInventory.capture(
                    Collections.singletonList(shapeSource));
            assertSupportedDataShapes(toolJar, cassandraHome, javaHome, shapeSource);
            capturedShapeSource.verifyUnchanged();
            production.assertRunning("Data-shape workspaces affected production Cassandra");

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
            long sourceMaxTimestamp = Long.parseLong(property(imported.output,
                    "import.maxTimestampMicros"));
            Assert.assertTrue(sourceMaxTimestamp > 0);
            Assert.assertEquals(Long.toString(sourceMaxTimestamp),
                    WorkspaceRepository.open(workspace).load().schemaIdentity().get(
                            "source.max-timestamp-micros"));
            production.assertRunning("Worker import affected production Cassandra");
            assertProductionIsolated(cqlsh, "after worker import");

            start = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString(),
                    "--timestamp-policy", "after-source"));
            Assert.assertEquals(start.output + workerError(workspace), 0, start.exitCode);
            workerRunning = true;
            Assert.assertEquals(Long.toString(sourceMaxTimestamp),
                    property(start.output, "source.maxTimestampMicros"));
            Assert.assertEquals("after-source", property(start.output, "timestamp.policy"));
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

            CommandResult launchedCqlsh = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "cqlsh", workspace.toString(), "--execute",
                    "SELECT user_name, password FROM blog.users "
                            + "WHERE user_name = 'frodo';"));
            Assert.assertEquals(launchedCqlsh.output, 0, launchedCqlsh.exitCode);
            Assert.assertTrue(launchedCqlsh.output, launchedCqlsh.output.contains("frodo"));
            Assert.assertTrue(launchedCqlsh.output, launchedCqlsh.output.contains("pass@"));

            CommandResult sourceRow = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint, cqlshrc,
                    "SELECT user_name, password, state FROM blog.users "
                            + "WHERE user_name = 'frodo';"));
            Assert.assertEquals(sourceRow.output, 0, sourceRow.exitCode);
            Assert.assertTrue(sourceRow.output, sourceRow.output.contains("frodo"));
            Assert.assertTrue(sourceRow.output, sourceRow.output.contains("pass@"));

            assertNativeUnreachableOutsideLoopback(nativeEndpoint);
            assertForbiddenPolicyMatrix(cqlsh, nativeEndpoint, cqlshrc);
            CommandResult preparedRejections = runPreparedRejectionCheck(cassandraHome,
                    nativeEndpoint[0], nativeEndpoint[1], credentials);
            Assert.assertEquals(preparedRejections.output, 0,
                    preparedRejections.exitCode);

            CommandResult rejectionFlush = run(command(controllerJava(), toolJar,
                    "workspace", "flush", workspace.toString()));
            Assert.assertEquals(rejectionFlush.output + workerError(workspace),
                    0, rejectionFlush.exitCode);
            Assert.assertEquals("0", property(rejectionFlush.output,
                    "flush.deltaFileCount"));
            Assert.assertFalse("Policy-only flush retained native credentials",
                    Files.exists(cqlshrc));
            CommandResult rejectionStop = run(command(controllerJava(), toolJar,
                    "workspace", "stop", workspace.toString()));
            Assert.assertEquals(rejectionStop.output, 0, rejectionStop.exitCode);
            workerRunning = false;

            NativeCredentials firstSessionCredentials = credentials;
            start = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(start.output + workerError(workspace), 0, start.exitCode);
            workerRunning = true;
            Assert.assertEquals("after-source", property(start.output, "timestamp.policy"));
            nativeEndpoint = property(start.output, "worker.native").split(":", 2);
            cqlshrc = Paths.get(property(start.output, "worker.cqlshrc"));
            credentials = readCredentials(cqlshrc);
            Assert.assertNotEquals(firstSessionCredentials.password, credentials.password);

            CommandResult mutate = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint, cqlshrc,
                    "INSERT INTO blog.users (user_name, password, gender, state, birth_year) "
                            + "VALUES ('sam', 'inserted', 'male', 'CA', 1980); "
                            + "UPDATE blog.users SET password = 'after' "
                            + "WHERE user_name = 'frodo';"));
            Assert.assertEquals(mutate.output, 0, mutate.exitCode);
            Path timestampState = workspace.resolve("state/timestamp.properties");
            long firstHighWater = timestampHighWater(timestampState);
            Assert.assertEquals("Stock cqlsh protocol timestamp was overwritten",
                    sourceMaxTimestamp, firstHighWater);

            long explicitCqlTimestamp = sourceMaxTimestamp + 1000L;
            CommandResult explicitCql = runCqlsh(cqlshCommand(cqlsh, nativeEndpoint,
                    cqlshrc, "INSERT INTO blog.users (user_name, password) "
                            + "VALUES ('explicit_cql', 'exact') USING TIMESTAMP "
                            + explicitCqlTimestamp + "; SELECT writetime(password) "
                            + "FROM blog.users WHERE user_name = 'explicit_cql';"));
            Assert.assertEquals(explicitCql.output, 0, explicitCql.exitCode);
            Assert.assertTrue(explicitCql.output,
                    explicitCql.output.contains(Long.toString(explicitCqlTimestamp)));
            Assert.assertEquals("Explicit CQL timestamp advanced automatic high-water",
                    firstHighWater, timestampHighWater(timestampState));

            CommandResult prepared = runPreparedPolicyCheck(cassandraHome,
                    nativeEndpoint[0], nativeEndpoint[1], credentials,
                    sourceMaxTimestamp);
            Assert.assertEquals(prepared.output, 0, prepared.exitCode);
            long preCrashHighWater = timestampHighWater(timestampState);
            Assert.assertTrue(preCrashHighWater > firstHighWater);
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
            Assert.assertEquals("after-source", property(start.output, "timestamp.policy"));
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
            Assert.assertTrue(replayed.output, replayed.output.contains("explicit_cql"));
            Assert.assertTrue(replayed.output, replayed.output.contains("protocol_ts"));
            CommandResult postRestartMutation = runPreparedPolicyCheck(cassandraHome,
                    nativeEndpoint[0], nativeEndpoint[1], restartedCredentials,
                    sourceMaxTimestamp);
            Assert.assertEquals(postRestartMutation.output, 0,
                    postRestartMutation.exitCode);
            Assert.assertTrue("Restart reused the after-source timestamp high-water",
                    timestampHighWater(timestampState) > preCrashHighWater);
            List<String> expectedLogicalState = readLogicalState(cassandraHome,
                    nativeEndpoint, restartedCredentials);

            CommandResult flushed = run(command(controllerJava(), toolJar,
                    "workspace", "flush", workspace.toString()));
            Assert.assertEquals(flushed.output + workerError(workspace),
                    0, flushed.exitCode);
            Assert.assertEquals("FLUSHED", property(flushed.output, "workspace.state"));
            Assert.assertEquals("FLUSHED", property(flushed.output, "worker.status"));
            Assert.assertTrue(flushed.output,
                    Integer.parseInt(property(flushed.output, "flush.deltaFileCount")) > 0);
            Assert.assertFalse("Flush retained native credentials", Files.exists(cqlshrc));
            WorkspaceManifest flushedManifest = WorkspaceRepository.open(workspace).load();
            WorkspaceFlushResult flushResult = WorkspaceFlushResult.read(workspace);
            flushResult.requireIdentity(flushedManifest.workspaceId(), "3.11.19", "blog",
                    "users", flushedManifest.schemaIdentity().get("table.directory"));
            flushResult.verifyCompleteInventory(workspace);
            Assert.assertFalse(flushResult.deltaFiles(
                    flushedManifest.baselineInventory()).isEmpty());

            CommandResult closedNative = runCqlsh(Arrays.asList(cqlsh.toString(),
                    nativeEndpoint[0], nativeEndpoint[1], "-e", "SHOW VERSION"));
            Assert.assertNotEquals("Native CQL remained available after flush:\n"
                    + closedNative.output, 0, closedNative.exitCode);

            Path manifestPath = workspace.resolve("manifest.json");
            String committedManifest = new String(Files.readAllBytes(manifestPath),
                    StandardCharsets.UTF_8);
            String interruptedManifest = committedManifest.replace(
                    "\"state\": \"FLUSHED\"", "\"state\": \"RUNNING\"");
            Assert.assertNotEquals("Flush failure injection did not change manifest state",
                    committedManifest, interruptedManifest);
            Files.write(manifestPath, interruptedManifest.getBytes(StandardCharsets.UTF_8));
            Assert.assertEquals(WorkspaceState.RUNNING,
                    WorkspaceRepository.open(workspace).load().state());
            CommandResult flushedStatus = run(command(controllerJava(), toolJar,
                    "workspace", "status", workspace.toString()));
            Assert.assertEquals(flushedStatus.output, 0, flushedStatus.exitCode);
            Assert.assertEquals("FLUSHED",
                    property(flushedStatus.output, "workspace.state"));
            Assert.assertEquals("FLUSHED",
                    property(flushedStatus.output, "worker.status"));

            byte[] flushedManifestBytes = Files.readAllBytes(manifestPath);
            for (String failpoint : Arrays.asList(
                    "after-verification",
                    "after-first-copy",
                    "after-files-copied",
                    "after-export-manifest",
                    "after-rename",
                    "after-manifest-save")) {
                Path faultOutput = temporary.getRoot().toPath().resolve(
                        "fault-export-" + failpoint);
                CommandResult interruptedExport = run(exportFailpointCommand(
                        controllerJava(), toolJar, failpoint, workspace, faultOutput));
                Assert.assertEquals("Export failpoint did not halt at " + failpoint + ":\n"
                        + interruptedExport.output, 97, interruptedExport.exitCode);

                boolean publicationRenamed = "after-rename".equals(failpoint)
                        || "after-manifest-save".equals(failpoint);
                Assert.assertEquals("Unexpected publication visibility after " + failpoint,
                        publicationRenamed, Files.isDirectory(faultOutput));
                WorkspaceState interruptedState = "after-manifest-save".equals(failpoint)
                        ? WorkspaceState.EXPORTED : WorkspaceState.FLUSHED;
                Assert.assertEquals("Unexpected workspace state after " + failpoint,
                        interruptedState, WorkspaceRepository.open(workspace).load().state());

                CommandResult recoveredExport = run(command(controllerJava(), toolJar,
                        "workspace", "export", workspace.toString(), "--mode", "delta",
                        "--output", faultOutput.toString()));
                Assert.assertEquals("Export retry failed after " + failpoint + ":\n"
                        + recoveredExport.output + workerError(workspace),
                        0, recoveredExport.exitCode);
                Assert.assertEquals("EXPORTED",
                        property(recoveredExport.output, "workspace.state"));
                Assert.assertTrue("Recovered export is missing after " + failpoint,
                        Files.isDirectory(faultOutput));
                Assert.assertEquals("Export retry left staging state after " + failpoint,
                        0, countExportStaging(faultOutput));

                Files.write(manifestPath, flushedManifestBytes);
                Assert.assertEquals(WorkspaceState.FLUSHED,
                        WorkspaceRepository.open(workspace).load().state());
            }

            Path deltaExport = temporary.getRoot().toPath().resolve("delta-export");
            CommandResult exported = run(command(controllerJava(), toolJar,
                    "workspace", "export", workspace.toString(), "--mode", "delta",
                    "--output", deltaExport.toString()));
            Assert.assertEquals(exported.output + workerError(workspace),
                    0, exported.exitCode);
            Assert.assertEquals("EXPORTED", property(exported.output, "workspace.state"));
            Assert.assertEquals("FLUSHED", property(exported.output, "worker.status"));
            Assert.assertEquals("delta", property(exported.output, "export.mode"));
            Assert.assertEquals(deltaExport.toRealPath().toString(),
                    property(exported.output, "export.path"));
            Assert.assertTrue(exported.output,
                    Integer.parseInt(property(exported.output,
                            "verification.deltaSstables")) > 0);
            Assert.assertTrue(exported.output,
                    Long.parseLong(property(exported.output,
                            "verification.logicalRows")) >= 4);

            WorkspaceManifest exportedManifest = WorkspaceRepository.open(workspace).load();
            Assert.assertEquals(WorkspaceState.EXPORTED, exportedManifest.state());
            Assert.assertEquals(1, exportedManifest.exports().size());
            ExportRecord exportRecord = exportedManifest.exports().get(0);
            Assert.assertEquals(property(exported.output, "export.id"),
                    exportRecord.exportId().toString());
            for (ManifestFile file : exportRecord.files()) {
                Path published = deltaExport.resolve(file.relativePath());
                Assert.assertTrue("Missing published export file " + published,
                        Files.isRegularFile(published));
                Assert.assertEquals(file.size(), Files.size(published));
                Assert.assertEquals(file.sha256(), Hashing.sha256(published));
            }
            for (ManifestFile baseline : exportedManifest.baselineInventory()) {
                Assert.assertFalse("Delta export contains baseline component " + baseline,
                        Files.exists(deltaExport.resolve("sstables").resolve(
                                Paths.get(baseline.relativePath()).getFileName())));
            }
            String exportManifest = new String(Files.readAllBytes(
                    deltaExport.resolve("export-manifest.json")), StandardCharsets.UTF_8);
            Assert.assertTrue(exportManifest, exportManifest.contains("\"mode\": \"delta\""));
            Assert.assertTrue(exportManifest,
                    exportManifest.contains("\"requiredSources\""));
            WorkspaceVerificationResult verification = WorkspaceVerificationResult.read(
                    workspace);
            verification.requireIdentity(exportedManifest.workspaceId(), "3.11.19", "blog",
                    "users", flushResult.sha256());

            CommandResult replayedExport = run(command(controllerJava(), toolJar,
                    "workspace", "export", workspace.toString(), "--mode", "delta",
                    "--output", deltaExport.toString()));
            Assert.assertEquals(replayedExport.output, 0, replayedExport.exitCode);
            Assert.assertEquals(exportRecord.exportId().toString(),
                    property(replayedExport.output, "export.id"));

            CommandResult stop = run(command(controllerJava(), toolJar,
                    "workspace", "stop", workspace.toString()));
            Assert.assertEquals(stop.output, 0, stop.exitCode);
            Assert.assertTrue(stop.output, stop.output.contains("workspace.state=STOPPED"));
            workerRunning = false;
            Assert.assertFalse(Files.exists(cqlshrc));
            production.assertRunning("Graceful worker stop affected production Cassandra");
            assertProductionIsolated(cqlsh, "after graceful worker stop");

            Path replayWorkspace = temporary.newFolder("delta-replay-workspace").toPath();
            Path deltaSstables = deltaExport.resolve("sstables");
            SourceInventory capturedDelta = SourceInventory.capture(
                    Collections.singletonList(deltaSstables));
            CommandResult replayCreate = run(command(controllerJava(), toolJar,
                    "workspace", "create", replayWorkspace.toString(),
                    "--sstables", source.toString(), "--sstables", deltaSstables.toString(),
                    "--schema", schema.toString()));
            Assert.assertEquals(replayCreate.output, 0, replayCreate.exitCode);
            CommandResult replayImport = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "import", replayWorkspace.toString()));
            Assert.assertEquals(replayImport.output + importError(replayWorkspace),
                    0, replayImport.exitCode);
            Assert.assertEquals(Long.toString(verification.logicalRows()),
                    property(replayImport.output, "import.logicalRows"));

            boolean replayWorkerRunning = false;
            try {
                CommandResult replayStart = run(command(controllerJava(), toolJar,
                        "--cassandra-home", cassandraHome.toString(),
                        "--java-home", javaHome.toString(),
                        "workspace", "start", replayWorkspace.toString()));
                Assert.assertEquals(replayStart.output + workerError(replayWorkspace),
                        0, replayStart.exitCode);
                replayWorkerRunning = true;
                String[] replayEndpoint = property(replayStart.output,
                        "worker.native").split(":", 2);
                Path replayCqlshrc = Paths.get(property(replayStart.output,
                        "worker.cqlshrc"));
                List<String> actualLogicalState = readLogicalState(cassandraHome,
                        replayEndpoint, readCredentials(replayCqlshrc));
                Assert.assertEquals("Base plus exported delta changed logical values or cell "
                        + "timestamps", expectedLogicalState, actualLogicalState);

                CommandResult replayStop = run(command(controllerJava(), toolJar,
                        "workspace", "stop", replayWorkspace.toString()));
                Assert.assertEquals(replayStop.output, 0, replayStop.exitCode);
                replayWorkerRunning = false;
                Assert.assertEquals(WorkspaceState.STOPPED,
                        WorkspaceRepository.open(replayWorkspace).load().state());
            } finally {
                if (replayWorkerRunning) {
                    CommandResult replayStop = run(command(controllerJava(), toolJar,
                            "workspace", "stop", replayWorkspace.toString()));
                    Assert.assertEquals(replayStop.output, 0, replayStop.exitCode);
                }
            }
            capturedDelta.verifyUnchanged();
            production.assertRunning("Delta replay affected production Cassandra");
            assertProductionIsolated(cqlsh, "after base-plus-delta replay");

            CommandResult targetSchema = runCqlsh(Arrays.asList(cqlsh.toString(),
                    "127.0.0.1", Integer.toString(Cassandra311ProductionFixture.NATIVE_PORT),
                    "-e", "CREATE KEYSPACE blog WITH replication = {'class': "
                    + "'SimpleStrategy', 'replication_factor': 1}; "
                    + "CREATE TABLE blog.users (user_name varchar PRIMARY KEY, "
                    + "password varchar, gender varchar, state varchar, "
                    + "birth_year bigint);"));
            Assert.assertEquals(targetSchema.output, 0, targetSchema.exitCode);
            Path loaderTable = Files.createDirectories(temporary.getRoot().toPath()
                    .resolve("clean-node-load/blog/users"));
            copyInventory(capturedSource, loaderTable);
            copyInventory(capturedDelta, loaderTable);
            SourceInventory loaderInventory = SourceInventory.capture(
                    Collections.singletonList(loaderTable));
            Map<String, String> loaderEnvironment = new TreeMap<>();
            loaderEnvironment.put("JAVA_HOME", javaHome.toString());
            loaderEnvironment.put("CASSANDRA_HOME", cassandraHome.toString());
            CommandResult loaded = run(Arrays.asList(
                    cassandraHome.resolve("bin/sstableloader").toString(),
                    "--no-progress", "-d", "127.0.0.1", loaderTable.toString()),
                    loaderEnvironment);
            Assert.assertEquals(loaded.output, 0, loaded.exitCode);
            String[] productionEndpoint = new String[]{"127.0.0.1",
                    Integer.toString(Cassandra311ProductionFixture.NATIVE_PORT)};
            Assert.assertEquals("Clean target node changed logical values or cell metadata",
                    expectedLogicalState,
                    readLogicalState(cassandraHome, productionEndpoint, null));
            loaderInventory.verifyUnchanged();
            capturedSource.verifyUnchanged();
            capturedDelta.verifyUnchanged();
            production.assertRunning("SSTable loader affected target Cassandra");
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

    private static void assertForbiddenPolicyMatrix(Path cqlsh,
                                                    String[] endpoint,
                                                    Path cqlshrc) throws Exception {
        assertPolicyRejected(cqlsh, endpoint, cqlshrc,
                "DELETE FROM blog.users WHERE user_name = 'sam';");
        assertPolicyRejected(cqlsh, endpoint, cqlshrc, "TRUNCATE blog.users;");
        assertPolicyRejected(cqlsh, endpoint, cqlshrc,
                "CREATE TABLE blog.forbidden (id text PRIMARY KEY);");
        assertPolicyRejected(cqlsh, endpoint, cqlshrc,
                "BEGIN BATCH INSERT INTO blog.users "
                        + "(user_name, password) VALUES ('batch', 'forbidden'); "
                        + "APPLY BATCH;");
        assertPolicyRejected(cqlsh, endpoint, cqlshrc,
                "UPDATE blog.users SET password = 'conditional' "
                        + "WHERE user_name = 'frodo' IF password = 'pass@';");
        assertPolicyRejected(cqlsh, endpoint, cqlshrc,
                "UPDATE system.local SET cluster_name = 'compromised' "
                        + "WHERE key = 'local';");
        assertPolicyRejected(cqlsh, endpoint, cqlshrc,
                "SELECT * FROM system.batchlog;");
    }

    private static void assertNativeUnreachableOutsideLoopback(String[] endpoint)
            throws Exception {
        InetAddress nonLoopback = null;
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements() && nonLoopback == null) {
            NetworkInterface network = interfaces.nextElement();
            if (!network.isUp() || network.isLoopback()) {
                continue;
            }
            Enumeration<InetAddress> addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address && !address.isLoopbackAddress()
                        && !address.isLinkLocalAddress()) {
                    nonLoopback = address;
                    break;
                }
            }
        }
        Assert.assertNotNull("Test host has no non-loopback IPv4 address", nonLoopback);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(nonLoopback,
                    Integer.parseInt(endpoint[1])), 1000);
            Assert.fail("Workspace native endpoint accepted non-loopback connection on "
                    + nonLoopback.getHostAddress() + ":" + endpoint[1]);
        } catch (IOException expected) {
            // A loopback-only bind must refuse the host interface address.
        }
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
        Assert.assertTrue(rows.output, rows.output.contains("4 rows"));

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
                                                         NativeCredentials credentials,
                                                         long sourceMaxTimestamp)
            throws Exception {
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
                + "session.use_client_timestamp = False\n"
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
                + "session.execute(\"UPDATE blog.users SET state = 'OR' "
                + "WHERE user_name = 'sam'\")\n"
                + "written = next(iter(session.execute(\"SELECT writetime(password) AS ts "
                + "FROM blog.users WHERE user_name = 'frodo'\")))\n"
                + "assert written.ts > int(os.environ['SSTABLE_TOOLS_SOURCE_MAX_TS'])\n"
                + "protocol_ts = int(os.environ['SSTABLE_TOOLS_PROTOCOL_TS'])\n"
                + "cluster.timestamp_generator = lambda: protocol_ts\n"
                + "session.use_client_timestamp = True\n"
                + "session.execute(\"INSERT INTO blog.users (user_name, password) "
                + "VALUES ('protocol_ts', 'exact')\")\n"
                + "session.use_client_timestamp = False\n"
                + "protocol_row = next(iter(session.execute(\"SELECT writetime(password) AS ts "
                + "FROM blog.users WHERE user_name = 'protocol_ts'\")))\n"
                + "assert protocol_row.ts == protocol_ts\n"
                + "paged_direct = SimpleStatement("
                + "\"SELECT user_name FROM blog.users\", fetch_size=1, "
                + "consistency_level=ConsistencyLevel.ONE)\n"
                + "assert sorted(row.user_name for row in session.execute(paged_direct)) "
                + "== ['explicit_cql', 'frodo', 'protocol_ts', 'sam']\n"
                + "paged_prepared = session.prepare("
                + "\"SELECT user_name FROM blog.users\")\n"
                + "paged_prepared.fetch_size = 1\n"
                + "paged_rows = session.execute(paged_prepared.bind(()))\n"
                + "assert sorted(row.user_name for row in paged_rows) "
                + "== ['explicit_cql', 'frodo', 'protocol_ts', 'sam']\n"
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
        environment.put("PYTHONPATH", pythonDriverPath(cassandraHome));
        environment.put("SSTABLE_TOOLS_TEST_USERNAME", credentials.username);
        environment.put("SSTABLE_TOOLS_TEST_PASSWORD", credentials.password);
        environment.put("SSTABLE_TOOLS_SOURCE_MAX_TS", Long.toString(sourceMaxTimestamp));
        environment.put("SSTABLE_TOOLS_PROTOCOL_TS",
                Long.toString(sourceMaxTimestamp + 2000L));
        return run(Arrays.asList(python2(), "-c", script, host, port), environment);
    }

    private static CommandResult runPreparedRejectionCheck(Path cassandraHome,
                                                            String host,
                                                            String port,
                                                            NativeCredentials credentials)
            throws Exception {
        String script = "from cassandra.cluster import Cluster\n"
                + "from cassandra import ConsistencyLevel\n"
                + "from cassandra.auth import PlainTextAuthProvider\n"
                + "import os, sys\n"
                + "auth = PlainTextAuthProvider("
                + "username=os.environ['SSTABLE_TOOLS_TEST_USERNAME'], "
                + "password=os.environ['SSTABLE_TOOLS_TEST_PASSWORD'])\n"
                + "cluster = Cluster([sys.argv[1]], port=int(sys.argv[2]), "
                + "auth_provider=auth)\n"
                + "session = cluster.connect()\n"
                + "try:\n"
                + "    session.prepare("
                + "\"DELETE FROM blog.users WHERE user_name = ?\")\n"
                + "    raise AssertionError('prepared DELETE unexpectedly succeeded')\n"
                + "except Exception as error:\n"
                + "    if 'SSTABLE_TOOLS_POLICY' not in str(error):\n"
                + "        raise\n"
                + "allowed = session.prepare("
                + "\"UPDATE blog.users SET password = ? WHERE user_name = ?\")\n"
                + "rejected = allowed.bind(('forbidden-all', 'frodo'))\n"
                + "rejected.consistency_level = ConsistencyLevel.ALL\n"
                + "try:\n"
                + "    session.execute(rejected)\n"
                + "    raise AssertionError('ALL prepared update unexpectedly succeeded')\n"
                + "except Exception as error:\n"
                + "    if 'SSTABLE_TOOLS_POLICY' not in str(error):\n"
                + "        raise\n"
                + "finally:\n"
                + "    cluster.shutdown()\n";
        Map<String, String> environment = new TreeMap<>();
        environment.put("PYTHONDONTWRITEBYTECODE", "1");
        environment.put("PYTHONPATH", pythonDriverPath(cassandraHome));
        environment.put("SSTABLE_TOOLS_TEST_USERNAME", credentials.username);
        environment.put("SSTABLE_TOOLS_TEST_PASSWORD", credentials.password);
        return run(Arrays.asList(python2(), "-c", script, host, port), environment);
    }

    private static List<String> readLogicalState(Path cassandraHome,
                                                 String[] endpoint,
                                                 NativeCredentials credentials)
            throws Exception {
        String script = "from cassandra.cluster import Cluster\n"
                + "import os, sys\n"
                + (credentials == null
                ? "cluster = Cluster([sys.argv[1]], port=int(sys.argv[2]))\n"
                : "from cassandra.auth import PlainTextAuthProvider\n"
                + "auth = PlainTextAuthProvider("
                + "username=os.environ['SSTABLE_TOOLS_TEST_USERNAME'], "
                + "password=os.environ['SSTABLE_TOOLS_TEST_PASSWORD'])\n"
                + "cluster = Cluster([sys.argv[1]], port=int(sys.argv[2]), "
                + "auth_provider=auth)\n")
                + "session = cluster.connect()\n"
                + "rows = session.execute(\"SELECT user_name, password, state, "
                + "writetime(password) AS password_ts, writetime(state) AS state_ts, "
                + "ttl(password) AS password_ttl, ttl(state) AS state_ttl "
                + "FROM blog.users\")\n"
                + "for row in sorted(rows, key=lambda value: value.user_name):\n"
                + "    print('STATE|%s|%s|%s|%s|%s|%s|%s' % (row.user_name, "
                + "row.password, row.state, row.password_ts, row.state_ts, "
                + "row.password_ttl, row.state_ttl))\n"
                + "cluster.shutdown()\n";
        Map<String, String> environment = new TreeMap<>();
        environment.put("PYTHONDONTWRITEBYTECODE", "1");
        environment.put("PYTHONPATH", pythonDriverPath(cassandraHome));
        if (credentials != null) {
            environment.put("SSTABLE_TOOLS_TEST_USERNAME", credentials.username);
            environment.put("SSTABLE_TOOLS_TEST_PASSWORD", credentials.password);
        }
        CommandResult result = run(Arrays.asList(python2(), "-c", script,
                endpoint[0], endpoint[1]), environment);
        Assert.assertEquals(result.output, 0, result.exitCode);
        List<String> state = new ArrayList<>();
        for (String line : result.output.split("\\r?\\n")) {
            if (line.startsWith("STATE|")) {
                state.add(line);
            }
        }
        Assert.assertEquals(result.output, 4, state.size());
        return state;
    }

    private static void copyInventory(SourceInventory inventory, Path destination)
            throws Exception {
        inventory.verifyUnchanged();
        for (SstableSet set : inventory.sets()) {
            for (SourceComponent component : set.components()) {
                Path target = destination.resolve(component.path().getFileName().toString());
                Files.copy(component.path(), target, StandardCopyOption.COPY_ATTRIBUTES);
            }
        }
        inventory.verifyUnchanged();
    }

    private static String pythonDriverPath(Path cassandraHome) throws Exception {
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
        return pythonPath.toString();
    }

    private static long timestampHighWater(Path path) throws IOException {
        for (String line : Files.readAllLines(path, StandardCharsets.US_ASCII)) {
            if (line.startsWith("high-water-micros=")) {
                return Long.parseLong(line.substring("high-water-micros=".length()));
            }
        }
        throw new IOException("Missing timestamp high-water in " + path);
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

    private Path createFutureSstableSource(Path cassandraHome,
                                           Path javaHome,
                                           long timestampMicros) throws Exception {
        Path source = temporary.newFolder("future-source").toPath();
        runFixtureWriter(cassandraHome, javaHome, source, "future",
                Long.toString(timestampMicros));
        Assert.assertTrue("Future fixture writer produced no Data.db",
                countDataComponents(source) > 0);
        return source;
    }

    private Path createShapeSstableSource(Path cassandraHome,
                                          Path javaHome) throws Exception {
        Path source = temporary.newFolder("shape-source").toPath();
        runFixtureWriter(cassandraHome, javaHome, source, "shapes");
        Assert.assertTrue("Data-shape fixture writer produced no Data.db",
                countDataComponents(source) > 0);
        return source;
    }

    private void runFixtureWriter(Path cassandraHome,
                                  Path javaHome,
                                  Path source,
                                  String kind,
                                  String... arguments) throws Exception {
        Path logDirectory = temporary.newFolder(kind + "-writer-log").toPath();
        Path testClasses = Paths.get(Cassandra311FixtureWriter.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).toRealPath();
        List<Path> classpath = new ArrayList<>();
        classpath.add(testClasses);
        classpath.add(cassandraHome.resolve("conf"));
        try (java.nio.file.DirectoryStream<Path> libraries = Files.newDirectoryStream(
                cassandraHome.resolve("lib"), "*.jar")) {
            for (Path library : libraries) {
                classpath.add(library);
            }
        }
        classpath.sort(Comparator.comparing(Path::toString));
        StringBuilder joined = new StringBuilder();
        for (Path entry : classpath) {
            if (joined.length() > 0) {
                joined.append(File.pathSeparatorChar);
            }
            joined.append(entry);
        }
        List<String> command = new ArrayList<>(Arrays.asList(
                javaHome.resolve("bin/java").toString(),
                "-Xms128m", "-Xmx512m",
                "-javaagent:" + singleMatch(cassandraHome.resolve("lib"), "jamm-*.jar"),
                "-Dcassandra.storagedir=" + source,
                "-Dcassandra.logdir=" + logDirectory,
                "-cp", joined.toString(),
                Cassandra311FixtureWriter.class.getName(), kind, source.toString()));
        command.addAll(Arrays.asList(arguments));
        CommandResult generated = run(command);
        Assert.assertEquals(generated.output, 0, generated.exitCode);
    }

    private void assertFutureTimestampPolicies(Path toolJar,
                                               Path cassandraHome,
                                               Path javaHome,
                                               Path source,
                                               long futureTimestampMicros) throws Exception {
        Path schema = createFutureSchemaBundle();

        Path wallClockWorkspace = temporary.newFolder("future-wall-clock").toPath();
        createAndImportFutureWorkspace(toolJar, cassandraHome, javaHome,
                wallClockWorkspace, source, schema, futureTimestampMicros);
        boolean wallClockRunning = false;
        try {
            CommandResult started = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", wallClockWorkspace.toString()));
            Assert.assertEquals(started.output + workerError(wallClockWorkspace),
                    0, started.exitCode);
            wallClockRunning = true;
            Assert.assertEquals("wall-clock", property(started.output, "timestamp.policy"));
            Assert.assertEquals("true", property(started.output,
                    "warning.futureSourceTimestamp"));
            String[] endpoint = property(started.output, "worker.native").split(":", 2);
            Path cqlshrc = Paths.get(property(started.output, "worker.cqlshrc"));
            CommandResult lowerTimestamp = runCqlsh(cqlshCommand(
                    cassandraHome.resolve("bin/cqlsh"), endpoint, cqlshrc,
                    "UPDATE future_fixture.users SET password = 'wall-clock-loses' "
                            + "WHERE user_name = 'future'; "
                            + "SELECT password, writetime(password) FROM "
                            + "future_fixture.users WHERE user_name = 'future';"));
            Assert.assertEquals(lowerTimestamp.output, 0, lowerTimestamp.exitCode);
            Assert.assertTrue(lowerTimestamp.output,
                    lowerTimestamp.output.contains("future-base"));
            Assert.assertFalse(lowerTimestamp.output,
                    lowerTimestamp.output.contains("wall-clock-loses"));
            Assert.assertTrue(lowerTimestamp.output,
                    lowerTimestamp.output.contains(Long.toString(futureTimestampMicros)));
        } finally {
            if (wallClockRunning) {
                CommandResult stopped = run(command(controllerJava(), toolJar,
                        "workspace", "stop", wallClockWorkspace.toString()));
                Assert.assertEquals(stopped.output, 0, stopped.exitCode);
            }
        }

        Path afterSourceWorkspace = temporary.newFolder("future-after-source").toPath();
        createAndImportFutureWorkspace(toolJar, cassandraHome, javaHome,
                afterSourceWorkspace, source, schema, futureTimestampMicros);
        boolean afterSourceRunning = false;
        try {
            CommandResult started = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", afterSourceWorkspace.toString(),
                    "--timestamp-policy", "after-source"));
            Assert.assertEquals(started.output + workerError(afterSourceWorkspace),
                    0, started.exitCode);
            afterSourceRunning = true;
            Assert.assertEquals("after-source", property(started.output,
                    "timestamp.policy"));
            Assert.assertEquals("true", property(started.output,
                    "warning.futureSourceTimestamp"));
            String[] endpoint = property(started.output, "worker.native").split(":", 2);
            Path cqlshrc = Paths.get(property(started.output, "worker.cqlshrc"));
            CommandResult updated = runFutureTimestampFreeUpdate(cassandraHome,
                    endpoint[0], endpoint[1], readCredentials(cqlshrc));
            Assert.assertEquals(updated.output, 0, updated.exitCode);
            String[] result = property(updated.output, "future.result").split("\\|", 2);
            Assert.assertEquals(updated.output, 2, result.length);
            Assert.assertEquals("after-source-wins", result[0]);
            Assert.assertTrue(updated.output,
                    Long.parseLong(result[1]) > futureTimestampMicros);
        } finally {
            if (afterSourceRunning) {
                CommandResult stopped = run(command(controllerJava(), toolJar,
                        "workspace", "stop", afterSourceWorkspace.toString()));
                Assert.assertEquals(stopped.output, 0, stopped.exitCode);
            }
        }
    }

    private void createAndImportFutureWorkspace(Path toolJar,
                                                Path cassandraHome,
                                                Path javaHome,
                                                Path workspace,
                                                Path source,
                                                Path schema,
                                                long futureTimestampMicros) throws Exception {
        CommandResult created = run(command(controllerJava(), toolJar,
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString(), "--schema", schema.toString()));
        Assert.assertEquals(created.output, 0, created.exitCode);
        CommandResult imported = run(command(controllerJava(), toolJar,
                "--cassandra-home", cassandraHome.toString(),
                "--java-home", javaHome.toString(),
                "workspace", "import", workspace.toString()));
        Assert.assertEquals(imported.output + importError(workspace), 0, imported.exitCode);
        Assert.assertEquals(Long.toString(futureTimestampMicros),
                property(imported.output, "import.maxTimestampMicros"));
        Assert.assertEquals("true", property(imported.output,
                "warning.futureSourceTimestamp"));
    }

    private void assertSupportedDataShapes(Path toolJar,
                                           Path cassandraHome,
                                           Path javaHome,
                                           Path source) throws Exception {
        Path schema = createShapeSchemaBundle();
        Path workspace = temporary.newFolder("shape-workspace").toPath();
        createAndImportWorkspace(toolJar, cassandraHome, javaHome, workspace, source,
                schema);
        boolean workerRunning = false;
        Path deltaSstables;
        List<String> expected;
        try {
            CommandResult started = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", workspace.toString()));
            Assert.assertEquals(started.output + workerError(workspace), 0,
                    started.exitCode);
            workerRunning = true;
            String[] endpoint = property(started.output, "worker.native").split(":", 2);
            Path cqlshrc = Paths.get(property(started.output, "worker.cqlshrc"));
            Path cqlsh = cassandraHome.resolve("bin/cqlsh");
            String workspaceId = WorkspaceRepository.open(workspace).load()
                    .workspaceId().toString();
            CommandResult rejectedDestroy = run(command(controllerJava(), toolJar,
                    "workspace", "destroy", workspace.toString(),
                    "--confirm-workspace-id", workspaceId));
            Assert.assertNotEquals(rejectedDestroy.output, 0,
                    rejectedDestroy.exitCode);
            Assert.assertTrue(rejectedDestroy.output,
                    rejectedDestroy.output.contains("stop or recover the workspace first"));
            Assert.assertTrue("Live destroy removed the workspace",
                    Files.isDirectory(workspace));
            assertShapeSourceRow(cqlsh, endpoint, cqlshrc);

            CommandResult mutated = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                    "INSERT INTO shape_fixture.items (tenant, item_id, category, name, "
                            + "tags, scores, attrs, location, pair, expiring) VALUES "
                            + "('acct', 1, 'category-insert', 'insert-name', "
                            + "{'blue'}, [1, 2], {'a': 'one'}, "
                            + "{street: 'First Street', zip: 100}, "
                            + "('insert-pair', 10), 'insert-expiring') USING TTL 600; "
                            + "UPDATE shape_fixture.items USING TTL 600 SET "
                            + "name = 'updated-name', tags = tags + {'green'}, "
                            + "scores = [0] + scores, attrs['b'] = 'two', "
                            + "location = {street: 'Second Street', zip: 200}, "
                            + "pair = ('updated-pair', 20), "
                            + "expiring = 'updated-expiring' "
                            + "WHERE tenant = 'acct' AND item_id = 1; "
                            + "UPDATE shape_fixture.items SET category = 'category-static' "
                            + "WHERE tenant = 'acct'; "
                            + "INSERT INTO shape_fixture.items (tenant, item_id, name) "
                            + "VALUES ('acct', 2, 'sibling');"));
            Assert.assertEquals(mutated.output, 0, mutated.exitCode);
            assertShapeMutationRows(cqlsh, endpoint, cqlshrc);
            expected = readShapeState(cassandraHome, endpoint,
                    readCredentials(cqlshrc));

            CommandResult flushed = run(command(controllerJava(), toolJar,
                    "workspace", "flush", workspace.toString()));
            Assert.assertEquals(flushed.output + workerError(workspace), 0,
                    flushed.exitCode);
            Assert.assertTrue(flushed.output,
                    Integer.parseInt(property(flushed.output,
                            "flush.deltaFileCount")) > 0);

            Path deltaExport = temporary.getRoot().toPath().resolve("shape-delta-export");
            CommandResult exported = run(command(controllerJava(), toolJar,
                    "workspace", "export", workspace.toString(), "--mode", "delta",
                    "--output", deltaExport.toString()));
            Assert.assertEquals(exported.output + workerError(workspace), 0,
                    exported.exitCode);
            Assert.assertEquals("EXPORTED", property(exported.output, "workspace.state"));
            deltaSstables = deltaExport.resolve("sstables");
            Assert.assertTrue("Data-shape export has no delta SSTables",
                    countDataComponents(deltaSstables) > 0);

            CommandResult stopped = run(command(controllerJava(), toolJar,
                    "workspace", "stop", workspace.toString()));
            Assert.assertEquals(stopped.output, 0, stopped.exitCode);
            workerRunning = false;
            waitForProcessExit(WorkerEndpoint.read(
                    workspace.resolve("state/worker.properties")).pid());
            CommandResult destroyed = run(command(controllerJava(), toolJar,
                    "workspace", "destroy", workspace.toString(),
                    "--confirm-workspace-id", workspaceId));
            Assert.assertEquals(destroyed.output, 0, destroyed.exitCode);
            Assert.assertFalse("Stopped destroy retained the workspace",
                    Files.exists(workspace));
            Assert.assertTrue("Workspace destroy removed the external delta",
                    Files.isDirectory(deltaSstables));
        } finally {
            if (workerRunning) {
                CommandResult stopped = run(command(controllerJava(), toolJar,
                        "workspace", "stop", workspace.toString()));
                Assert.assertEquals(stopped.output, 0, stopped.exitCode);
            }
        }

        SourceInventory capturedDelta = SourceInventory.capture(
                Collections.singletonList(deltaSstables));
        Path replayWorkspace = temporary.newFolder("shape-replay-workspace").toPath();
        CommandResult replayCreated = run(command(controllerJava(), toolJar,
                "workspace", "create", replayWorkspace.toString(),
                "--sstables", source.toString(), "--sstables", deltaSstables.toString(),
                "--schema", schema.toString()));
        Assert.assertEquals(replayCreated.output, 0, replayCreated.exitCode);
        CommandResult replayImported = run(command(controllerJava(), toolJar,
                "--cassandra-home", cassandraHome.toString(),
                "--java-home", javaHome.toString(),
                "workspace", "import", replayWorkspace.toString()));
        Assert.assertEquals(replayImported.output + importError(replayWorkspace), 0,
                replayImported.exitCode);

        boolean replayRunning = false;
        try {
            CommandResult replayStarted = run(command(controllerJava(), toolJar,
                    "--cassandra-home", cassandraHome.toString(),
                    "--java-home", javaHome.toString(),
                    "workspace", "start", replayWorkspace.toString()));
            Assert.assertEquals(replayStarted.output + workerError(replayWorkspace), 0,
                    replayStarted.exitCode);
            replayRunning = true;
            String[] endpoint = property(replayStarted.output,
                    "worker.native").split(":", 2);
            Path cqlshrc = Paths.get(property(replayStarted.output, "worker.cqlshrc"));
            assertShapeSourceRow(cassandraHome.resolve("bin/cqlsh"), endpoint, cqlshrc);
            assertShapeMutationRows(cassandraHome.resolve("bin/cqlsh"), endpoint,
                    cqlshrc);
            Assert.assertEquals("Base plus data-shape delta changed logical values",
                    expected, readShapeState(cassandraHome, endpoint,
                            readCredentials(cqlshrc)));
        } finally {
            if (replayRunning) {
                CommandResult stopped = run(command(controllerJava(), toolJar,
                        "workspace", "stop", replayWorkspace.toString()));
                Assert.assertEquals(stopped.output, 0, stopped.exitCode);
            }
        }
        capturedDelta.verifyUnchanged();
    }

    private static void assertShapeSourceRow(Path cqlsh,
                                             String[] endpoint,
                                             Path cqlshrc) throws Exception {
        CommandResult selected = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                "SELECT category, name, tags, scores, attrs, location, pair, expiring "
                        + "FROM shape_fixture.items WHERE tenant = 'base';"));
        Assert.assertEquals(selected.output, 0, selected.exitCode);
        for (String value : Arrays.asList("source-category", "source-name", "seed",
                "source", "offline-writer", "Source Street", "source-pair",
                "source-expiring")) {
            Assert.assertTrue("Stock cqlsh did not render source value " + value + ":\n"
                    + selected.output, selected.output.contains(value));
        }
    }

    private static void assertShapeMutationRows(Path cqlsh,
                                                String[] endpoint,
                                                Path cqlshrc) throws Exception {
        CommandResult selected = runCqlsh(cqlshCommand(cqlsh, endpoint, cqlshrc,
                "SELECT item_id, category, name, tags, scores, attrs, location, pair, "
                        + "expiring, ttl(expiring) AS expires_in "
                        + "FROM shape_fixture.items WHERE tenant = 'acct';"));
        Assert.assertEquals(selected.output, 0, selected.exitCode);
        for (String value : Arrays.asList("category-static", "updated-name", "blue",
                "green", "Second Street", "updated-pair", "updated-expiring",
                "sibling", "2 rows")) {
            Assert.assertTrue("Stock cqlsh did not render mutated value " + value + ":\n"
                    + selected.output, selected.output.contains(value));
        }
    }

    private void createAndImportWorkspace(Path toolJar,
                                          Path cassandraHome,
                                          Path javaHome,
                                          Path workspace,
                                          Path source,
                                          Path schema) throws Exception {
        CommandResult created = run(command(controllerJava(), toolJar,
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString(), "--schema", schema.toString()));
        Assert.assertEquals(created.output, 0, created.exitCode);
        CommandResult imported = run(command(controllerJava(), toolJar,
                "--cassandra-home", cassandraHome.toString(),
                "--java-home", javaHome.toString(),
                "workspace", "import", workspace.toString()));
        Assert.assertEquals(imported.output + importError(workspace), 0,
                imported.exitCode);
    }

    private static List<String> readShapeState(Path cassandraHome,
                                               String[] endpoint,
                                               NativeCredentials credentials)
            throws Exception {
        String script = "from cassandra.cluster import Cluster\n"
                + "from cassandra.auth import PlainTextAuthProvider\n"
                + "import os, sys\n"
                + "auth = PlainTextAuthProvider("
                + "username=os.environ['SSTABLE_TOOLS_TEST_USERNAME'], "
                + "password=os.environ['SSTABLE_TOOLS_TEST_PASSWORD'])\n"
                + "cluster = Cluster([sys.argv[1]], port=int(sys.argv[2]), "
                + "auth_provider=auth)\n"
                + "session = cluster.connect()\n"
                + "selected = session.prepare("
                + "\"SELECT item_id, category, name, tags, scores, attrs, location, "
                + "pair, expiring FROM shape_fixture.items WHERE tenant = ?\")\n"
                + "rows = list(session.execute(selected.bind(('acct',))))\n"
                + "assert len(rows) == 2\n"
                + "rows = dict((row.item_id, row) for row in rows)\n"
                + "first = rows[1]\n"
                + "assert first.category == 'category-static'\n"
                + "assert first.name == 'updated-name'\n"
                + "assert sorted(first.tags) == ['blue', 'green']\n"
                + "assert first.scores == [0, 1, 2]\n"
                + "assert first.attrs == {'a': 'one', 'b': 'two'}\n"
                + "assert first.location.street == 'Second Street'\n"
                + "assert first.location.zip == 200\n"
                + "assert first.pair[0] == 'updated-pair' and first.pair[1] == 20\n"
                + "assert first.expiring == 'updated-expiring'\n"
                + "second = rows[2]\n"
                + "assert second.category == 'category-static'\n"
                + "assert second.name == 'sibling'\n"
                + "ttl = next(iter(session.execute("
                + "\"SELECT ttl(expiring) AS expires_in FROM shape_fixture.items "
                + "WHERE tenant = 'acct' AND item_id = 1\"))).expires_in\n"
                + "assert ttl is not None and ttl > 0 and ttl <= 600\n"
                + "print('SHAPE|1|%s|%s|%s|%s|%s|%s|%s|%s|%s' % ("
                + "first.category, first.name, ','.join(sorted(first.tags)), "
                + "','.join(str(value) for value in first.scores), "
                + "','.join('%s=%s' % item for item in sorted(first.attrs.items())), "
                + "first.location.street, first.location.zip, first.pair[0], "
                + "first.pair[1]))\n"
                + "print('SHAPE|2|%s|%s' % (second.category, second.name))\n"
                + "cluster.shutdown()\n";
        Map<String, String> environment = new TreeMap<>();
        environment.put("PYTHONDONTWRITEBYTECODE", "1");
        environment.put("PYTHONPATH", pythonDriverPath(cassandraHome));
        environment.put("SSTABLE_TOOLS_TEST_USERNAME", credentials.username);
        environment.put("SSTABLE_TOOLS_TEST_PASSWORD", credentials.password);
        CommandResult result = run(Arrays.asList(python2(), "-c", script,
                endpoint[0], endpoint[1]), environment);
        Assert.assertEquals(result.output, 0, result.exitCode);
        List<String> state = new ArrayList<>();
        for (String line : result.output.split("\\r?\\n")) {
            if (line.startsWith("SHAPE|")) {
                state.add(line);
            }
        }
        Assert.assertEquals(result.output, 2, state.size());
        return state;
    }

    private static CommandResult runFutureTimestampFreeUpdate(Path cassandraHome,
                                                               String host,
                                                               String port,
                                                               NativeCredentials credentials)
            throws Exception {
        String script = "from cassandra.cluster import Cluster\n"
                + "from cassandra.auth import PlainTextAuthProvider\n"
                + "import os, sys\n"
                + "auth = PlainTextAuthProvider("
                + "username=os.environ['SSTABLE_TOOLS_TEST_USERNAME'], "
                + "password=os.environ['SSTABLE_TOOLS_TEST_PASSWORD'])\n"
                + "cluster = Cluster([sys.argv[1]], port=int(sys.argv[2]), "
                + "auth_provider=auth)\n"
                + "session = cluster.connect()\n"
                + "session.use_client_timestamp = False\n"
                + "update = session.prepare("
                + "\"UPDATE future_fixture.users SET password = ? "
                + "WHERE user_name = ?\")\n"
                + "session.execute(update.bind(('after-source-wins', 'future')))\n"
                + "row = next(iter(session.execute("
                + "\"SELECT password, writetime(password) AS ts "
                + "FROM future_fixture.users WHERE user_name = 'future'\")))\n"
                + "print('future.result=%s|%s' % (row.password, row.ts))\n"
                + "cluster.shutdown()\n";
        Map<String, String> environment = new TreeMap<>();
        environment.put("PYTHONDONTWRITEBYTECODE", "1");
        environment.put("PYTHONPATH", pythonDriverPath(cassandraHome));
        environment.put("SSTABLE_TOOLS_TEST_USERNAME", credentials.username);
        environment.put("SSTABLE_TOOLS_TEST_PASSWORD", credentials.password);
        return run(Arrays.asList(python2(), "-c", script, host, port), environment);
    }

    private Path createSstableSource(Path fixtureDirectory) throws IOException {
        return createSstableSource(fixtureDirectory, "source", "ma-2-big-");
    }

    private Path createSstableSource(Path fixtureDirectory,
                                     String directoryName,
                                     String... prefixes) throws IOException {
        Path source = temporary.newFolder(directoryName).toPath();
        copySstableFixtures(fixtureDirectory, source, prefixes);
        return source;
    }

    private static void copySstableFixtures(Path fixtureDirectory,
                                            Path destination,
                                            String... prefixes) throws IOException {
        for (String prefix : prefixes) {
            try (java.nio.file.DirectoryStream<Path> fixtures = Files.newDirectoryStream(
                    fixtureDirectory, prefix + "*")) {
                for (Path fixture : fixtures) {
                    Files.copy(fixture, destination.resolve(fixture.getFileName()),
                            StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }

    private Path createSchemaBundle() throws IOException {
        return createSchemaBundle("bigint");
    }

    private Path createStoppedSourceSchemaBundle() throws IOException {
        Path schema = temporary.newFile("schema-stopped-source.cql").toPath();
        Files.write(schema, Arrays.asList(
                "CREATE KEYSPACE stopped_source WITH replication = {'class': "
                        + "'SimpleStrategy', 'replication_factor': 1};",
                "CREATE TABLE stopped_source.users (",
                "  user_name text PRIMARY KEY,",
                "  password text,",
                "  state text",
                ");"), StandardCharsets.UTF_8);
        return schema;
    }

    private static Path findGeneratedTable(Path dataDirectory,
                                           String keyspace,
                                           String table) throws IOException {
        Path keyspaceDirectory = dataDirectory.resolve(keyspace);
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(
                keyspaceDirectory, table + "-*")) {
            for (Path entry : entries) {
                if (Files.isDirectory(entry) && containsDataComponent(entry)) {
                    return entry;
                }
            }
        }
        throw new IOException("No flushed SSTable table directory under " + keyspaceDirectory);
    }

    private static boolean containsDataComponent(Path directory) throws IOException {
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(directory,
                "*-Data.db")) {
            return entries.iterator().hasNext();
        }
    }

    private static void copyDirectory(Path source, Path destination) throws IOException {
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(source)) {
            for (Path entry : entries) {
                if (Files.isRegularFile(entry)) {
                    Files.copy(entry, destination.resolve(entry.getFileName()),
                            StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }

    private Path createFutureSchemaBundle() throws IOException {
        Path schema = temporary.newFile("schema-future.cql").toPath();
        Files.write(schema, Arrays.asList(
                "CREATE KEYSPACE future_fixture WITH replication = {'class': "
                        + "'SimpleStrategy', 'replication_factor': 1};",
                "CREATE TABLE future_fixture.users (",
                "  user_name text PRIMARY KEY,",
                "  password text",
                ");"), StandardCharsets.UTF_8);
        return schema;
    }

    private Path createShapeSchemaBundle() throws IOException {
        Path schema = temporary.newFile("schema-shapes.cql").toPath();
        Files.write(schema, Arrays.asList(
                "CREATE KEYSPACE shape_fixture WITH replication = {'class': "
                        + "'SimpleStrategy', 'replication_factor': 1};",
                Cassandra311FixtureWriter.SHAPE_TYPE + ";",
                Cassandra311FixtureWriter.SHAPE_TABLE + ";"), StandardCharsets.UTF_8);
        return schema;
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

    private static List<String> exportFailpointCommand(String java,
                                                       Path jar,
                                                       String failpoint,
                                                       Path workspace,
                                                       Path output) {
        List<String> command = new ArrayList<>();
        command.add(java);
        command.add("-Dsstable.tools.test.enable-export-failpoints=true");
        command.add("-Dsstable.tools.test.export-failpoint=" + failpoint);
        command.add("-jar");
        command.add(jar.toString());
        command.addAll(Arrays.asList("workspace", "export", workspace.toString(),
                "--mode", "delta", "--output", output.toString()));
        return command;
    }

    private static long countExportStaging(Path output) throws IOException {
        Path parent = output.getParent();
        String glob = "." + output.getFileName() + ".*.tmp";
        long count = 0;
        try (java.nio.file.DirectoryStream<Path> entries =
                     Files.newDirectoryStream(parent, glob)) {
            for (Path ignored : entries) {
                count++;
            }
        }
        return count;
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
        Assert.assertFalse("Worker process did not exit: " + pid,
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
