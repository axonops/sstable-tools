package com.csforge.sstable.bootstrap;

import com.csforge.sstable.workspace.WorkspaceLock;
import com.csforge.sstable.workspace.WorkspaceManifest;
import com.csforge.sstable.workspace.WorkspaceRepository;
import com.csforge.sstable.workspace.WorkspaceState;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
    public void recoverRefusesWorkerStateWithoutWorkerReconciliation() throws Exception {
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

        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int exitCode = BootstrapMain.run(new String[]{
                        "workspace", "recover", workspace.toString()
                }, System.out, new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.WORKSPACE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("worker reconciliation"));
        Assert.assertEquals(WorkspaceState.FAILED_RECOVERABLE,
                repository.load().state());
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
