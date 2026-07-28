package com.axonops.sstable.bootstrap;

import com.axonops.sstable.worker.api.WorkerEndpoint;
import com.axonops.sstable.worker.api.WorkerProtocol;
import com.axonops.sstable.workspace.WorkspaceLock;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import com.axonops.sstable.workspace.WorkspaceState;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BootstrapMainTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void helpDoesNotRequireCassandraClasses() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(
                new String[]{"--help"},
                new PrintStream(output, true, "UTF-8"),
                System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8").contains("--cassandra-home"));
        Assert.assertTrue(output.toString("UTF-8").contains("runtime preflight"));
        Assert.assertTrue(output.toString("UTF-8")
                .contains("Never run SSTable write operations"));
    }

    @Test
    public void invalidCommandReturnsUsageErrorWithoutLoadingCassandra() throws Exception {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(
                new String[]{"workspace", "start"},
                System.out,
                new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.USAGE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("runtime inspect"));
    }

    @Test
    public void createAndStatusAreIdempotentWithoutCassandraRuntime() throws Exception {
        Path source = createSstableSource("source", "mc-1-big");
        Path workspace = temporary.newFolder("workspace").toPath();
        ByteArrayOutputStream createOutput = new ByteArrayOutputStream();

        int createExitCode = BootstrapMain.run(new String[]{
                        "workspace", "create", workspace.toString(),
                        "--sstables", source.toString()
                }, new PrintStream(createOutput, true, "UTF-8"), System.err);

        Assert.assertEquals(0, createExitCode);
        Assert.assertTrue(createOutput.toString("UTF-8")
                .contains("workspace.state=VALIDATED"));
        Assert.assertTrue(Files.isRegularFile(workspace.resolve("manifest.json")));

        ByteArrayOutputStream statusOutput = new ByteArrayOutputStream();
        int statusExitCode = BootstrapMain.run(new String[]{
                        "workspace", "status", workspace.toString()
                }, new PrintStream(statusOutput, true, "UTF-8"), System.err);

        Assert.assertEquals(0, statusExitCode);
        Assert.assertTrue(statusOutput.toString("UTF-8")
                .contains("source.componentCount=3"));
        Assert.assertTrue(statusOutput.toString("UTF-8")
                .contains("source.integrity=verified"));

        ByteArrayOutputStream secondCreate = new ByteArrayOutputStream();
        int secondCreateExitCode = BootstrapMain.run(new String[]{
                        "workspace", "create", workspace.toString(),
                        "--sstables", source.toString()
                }, new PrintStream(secondCreate, true, "UTF-8"), System.err);
        Assert.assertEquals(0, secondCreateExitCode);
        Assert.assertTrue(secondCreate.toString("UTF-8")
                .contains("workspace.state=VALIDATED"));
    }

    @Test
    public void createPrintsProductionDataSafetyWarning() throws Exception {
        Path source = createSstableSource("warning-source", "mc-8-big");
        Path workspace = temporary.newFolder("warning-workspace").toPath();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "create", workspace.toString(),
                        "--sstables", source.toString()
                }, discard(), new PrintStream(error, true, "UTF-8"));

        String warning = error.toString("UTF-8");
        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(warning.contains("DANGEROUS SSTABLE WRITE WORKFLOW"));
        Assert.assertTrue(warning.contains("running production Cassandra process"));
        Assert.assertTrue(warning.contains("copied outside every live Cassandra data directory"));
        Assert.assertTrue(warning.contains("isolated workspace worker"));
    }

    @Test
    public void recoverReturnsFailedWorkspaceToItsLastStableState() throws Exception {
        Path source = createSstableSource("recover-source", "mc-2-big");
        Path workspace = temporary.newFolder("recover-workspace").toPath();
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString()
        }, discard(), System.err));

        WorkspaceRepository repository = WorkspaceRepository.open(workspace);
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest failed = repository.load().fail("simulated interruption");
            repository.save(lock, failed);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "recover", workspace.toString()
                }, new PrintStream(output, true, "UTF-8"), System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8").contains("workspace.state=VALIDATED"));
        Assert.assertFalse(output.toString("UTF-8").contains("workspace.failureMessage="));
    }

    @Test
    public void sourceMutationMakesStatusFailClosed() throws Exception {
        Path source = createSstableSource("mutable-source", "mc-3-big");
        Path workspace = temporary.newFolder("mutable-workspace").toPath();
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString()
        }, discard(), System.err));
        Files.write(source.resolve("mc-3-big-Data.db"),
                "changed".getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "status", workspace.toString()
                }, System.out, new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("Source mutation detected"));
    }

    @Test
    public void createRejectsOverlappingWorkspaceAndSource() throws Exception {
        Path source = createSstableSource("overlapping", "mc-4-big");
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "create", source.toString(),
                        "--sstables", source.toString()
                }, System.out, new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("must not overlap"));
        Assert.assertFalse(Files.exists(source.resolve("manifest.json")));
        Assert.assertFalse(Files.exists(source.resolve(".workspace.lock")));
    }

    @Test
    public void createRejectsSchemaStoredInsideWorkspace() throws Exception {
        Path source = createSstableSource("schema-overlap-source", "mc-6-big");
        Path workspace = temporary.newFolder("schema-overlap-workspace").toPath();
        Path schema = Files.write(workspace.resolve("input.cql"), Arrays.asList(
                "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', "
                        + "'replication_factor': 1};",
                "CREATE TABLE test.items (id int PRIMARY KEY);"),
                StandardCharsets.UTF_8);
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "create", workspace.toString(),
                        "--sstables", source.toString(), "--schema", schema.toString()
                }, System.out, new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("outside the workspace"));
        Assert.assertFalse(Files.exists(workspace.resolve("manifest.json")));
    }

    @Test
    public void repeatedCreateVerifiesPreviouslyCapturedSchema() throws Exception {
        Path source = createSstableSource("repeated-schema-source", "mc-7-big");
        Path workspace = temporary.newFolder("repeated-schema-workspace").toPath();
        Path schema = Files.write(temporary.newFile("schema.cql").toPath(), Arrays.asList(
                "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', "
                        + "'replication_factor': 1};",
                "CREATE TABLE test.items (id int PRIMARY KEY);"),
                StandardCharsets.UTF_8);
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString(), "--schema", schema.toString()
        }, discard(), System.err));
        Files.delete(schema);

        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "create", workspace.toString(),
                        "--sstables", source.toString()
                }, System.out, new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("Cannot capture schema bundle"));
    }

    @Test
    public void recoverRestoresVerifiedImportedWorkspaceWithoutAWorker() throws Exception {
        Path source = createSstableSource("worker-state-source", "mc-5-big");
        Path workspace = temporary.newFolder("worker-state-workspace").toPath();
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString()
        }, discard(), System.err));

        WorkspaceRepository repository = WorkspaceRepository.open(workspace);
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest imported = repository.load()
                    .transitionTo(WorkspaceState.IMPORTED);
            repository.save(lock, imported);
            repository.save(lock, imported.fail("worker stopped unexpectedly"));
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "recover", workspace.toString()
                }, new PrintStream(output, true, "UTF-8"), System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8").contains(
                "workspace.state=IMPORTED"));
        Assert.assertEquals(WorkspaceState.IMPORTED,
                repository.load().state());
    }

    @Test
    public void recoverRestoresStoppedWorkspaceOnlyAfterPidIsProvenGone() throws Exception {
        Path source = createSstableSource("stopped-recover-source", "mc-11-big");
        Path workspace = temporary.newFolder("stopped-recover-workspace").toPath();
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString()
        }, discard(), System.err));

        WorkspaceRepository repository = WorkspaceRepository.open(workspace);
        UUID workspaceId;
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest imported = repository.load()
                    .transitionTo(WorkspaceState.IMPORTED);
            repository.save(lock, imported);
            WorkspaceManifest running = imported.transitionTo(WorkspaceState.RUNNING);
            repository.save(lock, running);
            WorkspaceManifest stopped = running.transitionTo(WorkspaceState.STOPPED);
            repository.save(lock, stopped);
            repository.save(lock, stopped.fail("simulated stopped-state interruption"));
            workspaceId = stopped.workspaceId();
        }
        Instant now = Instant.parse("2026-07-17T12:00:00Z");
        new WorkerEndpoint(WorkerProtocol.CURRENT_VERSION, workspaceId,
                WorkerEndpoint.Status.STOPPED, 999999999L, "3.11.19", "127.0.0.1",
                19042, "127.0.0.1", 19043, now, now, "stopped")
                .writeAtomically(workspace.resolve("state/worker.properties"));
        Files.write(workspace.resolve("state/cqlshrc"),
                "stale credential".getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "recover", workspace.toString()
                }, new PrintStream(output, true, "UTF-8"), System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8").contains(
                "workspace.state=STOPPED"));
        Assert.assertEquals(WorkspaceState.STOPPED, repository.load().state());
        Assert.assertFalse(Files.exists(workspace.resolve("state/cqlshrc")));
    }

    @Test
    public void destroyRequiresMatchingUuidAndNeverFollowsWorkspaceSymlinks()
            throws Exception {
        Path source = createSstableSource("destroy-source", "mc-9-big");
        Path workspace = temporary.newFolder("destroy-workspace").toPath();
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString()
        }, discard(), System.err));
        Path canonicalWorkspace = workspace.toRealPath();
        UUID workspaceId = WorkspaceRepository.open(workspace).load().workspaceId();
        Path outside = Files.write(temporary.newFile("outside-evidence").toPath(),
                "outside".getBytes(StandardCharsets.UTF_8));
        try {
            Files.createSymbolicLink(workspace.resolve("runtime/outside-link"), outside);
        } catch (UnsupportedOperationException | java.io.IOException ignored) {
            // The confinement assertions still run on filesystems without symlinks.
        }

        ByteArrayOutputStream wrongError = new ByteArrayOutputStream();
        int wrongExit = BootstrapMain.run(new String[]{
                "workspace", "destroy", workspace.toString(),
                "--confirm-workspace-id", UUID.randomUUID().toString()
        }, discard(), new PrintStream(wrongError, true, "UTF-8"));
        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, wrongExit);
        Assert.assertTrue(wrongError.toString("UTF-8").contains(
                "confirmation UUID does not match"));
        Assert.assertTrue(Files.isDirectory(workspace));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                "workspace", "destroy", workspace.toString(),
                "--confirm-workspace-id", workspaceId.toString()
        }, new PrintStream(output, true, "UTF-8"), System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8").contains(
                "WARNING: permanently destroying workspace"));
        Assert.assertTrue(output.toString("UTF-8").contains(
                "workspace.destroyed=" + canonicalWorkspace));
        Assert.assertFalse(Files.exists(workspace));
        Assert.assertTrue(Files.isRegularFile(source.resolve("mc-9-big-Data.db")));
        Assert.assertEquals("outside", new String(Files.readAllBytes(outside),
                StandardCharsets.UTF_8));
    }

    @Test
    public void destroyRefusesLiveStateAndUnexpectedTopLevelEntries() throws Exception {
        Path source = createSstableSource("guarded-destroy-source", "mc-10-big");
        Path workspace = temporary.newFolder("guarded-destroy-workspace").toPath();
        Assert.assertEquals(0, BootstrapMain.run(new String[]{
                "workspace", "create", workspace.toString(),
                "--sstables", source.toString()
        }, discard(), System.err));
        WorkspaceRepository repository = WorkspaceRepository.open(workspace);
        UUID workspaceId = repository.load().workspaceId();
        Files.write(workspace.resolve("unowned.txt"),
                "do not delete".getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream unexpectedError = new ByteArrayOutputStream();
        int unexpectedExit = BootstrapMain.run(new String[]{
                "workspace", "destroy", workspace.toString(),
                "--confirm-workspace-id", workspaceId.toString()
        }, discard(), new PrintStream(unexpectedError, true, "UTF-8"));
        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, unexpectedExit);
        Assert.assertTrue(unexpectedError.toString("UTF-8").contains(
                "unexpected top-level entry"));
        Assert.assertTrue(Files.isRegularFile(workspace.resolve("unowned.txt")));
        Files.delete(workspace.resolve("unowned.txt"));

        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest imported = repository.load()
                    .transitionTo(WorkspaceState.IMPORTED);
            repository.save(lock, imported);
            repository.save(lock, imported.transitionTo(WorkspaceState.RUNNING));
        }
        ByteArrayOutputStream runningError = new ByteArrayOutputStream();
        int runningExit = BootstrapMain.run(new String[]{
                "workspace", "destroy", workspace.toString(),
                "--confirm-workspace-id", workspaceId.toString()
        }, discard(), new PrintStream(runningError, true, "UTF-8"));
        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, runningExit);
        Assert.assertTrue(runningError.toString("UTF-8").contains(
                "stop or recover the workspace first"));
        Assert.assertTrue(Files.isDirectory(workspace));
    }

    private Path createSstableSource(String directoryName, String descriptor) throws Exception {
        Path source = temporary.newFolder(directoryName).toPath();
        Files.write(source.resolve(descriptor + "-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve(descriptor + "-Data.db"),
                "data".getBytes(StandardCharsets.UTF_8));
        Files.write(source.resolve(descriptor + "-Statistics.db"),
                "statistics".getBytes(StandardCharsets.UTF_8));
        return source;
    }

    private static PrintStream discard() throws Exception {
        return new PrintStream(new ByteArrayOutputStream(), true, "UTF-8");
    }
}
