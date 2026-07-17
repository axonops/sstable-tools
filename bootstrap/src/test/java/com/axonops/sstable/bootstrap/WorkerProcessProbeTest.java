package com.axonops.sstable.bootstrap;

import com.axonops.sstable.worker.api.WorkerEndpoint;
import com.axonops.sstable.worker.api.WorkerProtocol;
import com.axonops.sstable.workspace.WorkspaceException;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkerProcessProbeTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void acceptsMissingZombieAndReusedPidsButRejectsMatchingWorker() throws Exception {
        Path proc = temporary.newFolder("proc").toPath();
        Path workspace = temporary.newFolder("workspace").toPath().toRealPath();
        UUID workspaceId = UUID.randomUUID();
        WorkerEndpoint endpoint = endpoint(123, workspaceId);
        WorkerProcessProbe probe = new WorkerProcessProbe(proc);

        probe.requireMatchingWorkerStopped(endpoint, workspaceId, workspace);

        Path process = Files.createDirectory(proc.resolve("123"));
        Files.write(process.resolve("stat"), "123 (java) Z 1 2 3\n"
                .getBytes(StandardCharsets.US_ASCII));
        Files.write(process.resolve("cmdline"), new byte[0]);
        probe.requireMatchingWorkerStopped(endpoint, workspaceId, workspace);

        Files.write(process.resolve("stat"), "123 (other) S 1 2 3\n"
                .getBytes(StandardCharsets.US_ASCII));
        writeCommand(process.resolve("cmdline"), "other-command", "--sandbox");
        probe.requireMatchingWorkerStopped(endpoint, workspaceId, workspace);

        writeCommand(process.resolve("cmdline"), "java", ChildProcessLauncher.WORKER_MAIN,
                "--sandbox", "--workspace", workspace.toString(), "--workspace-id",
                workspaceId.toString());
        try {
            probe.requireMatchingWorkerStopped(endpoint, workspaceId, workspace);
            Assert.fail("Expected matching worker process to block recovery");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("still running"));
        }
    }

    @Test
    public void refusesRecoveryWithoutProcIdentitySupport() throws Exception {
        Path workspace = temporary.newFolder("no-proc-workspace").toPath().toRealPath();
        UUID workspaceId = UUID.randomUUID();
        WorkerProcessProbe probe = new WorkerProcessProbe(
                temporary.getRoot().toPath().resolve("missing-proc"));

        try {
            probe.requireMatchingWorkerStopped(endpoint(123, workspaceId), workspaceId,
                    workspace);
            Assert.fail("Expected missing proc to block recovery");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("requires Linux /proc"));
        }
    }

    private static WorkerEndpoint endpoint(long pid, UUID workspaceId) {
        Instant now = Instant.parse("2026-07-15T12:00:00Z");
        return new WorkerEndpoint(WorkerProtocol.CURRENT_VERSION, workspaceId,
                WorkerEndpoint.Status.RUNNING, pid, "3.11.19", "127.0.0.1", 19042,
                "127.0.0.1", 19043, now, now, "");
    }

    private static void writeCommand(Path path, String... arguments) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (String argument : arguments) {
            output.write(argument.getBytes(StandardCharsets.UTF_8));
            output.write(0);
        }
        Files.write(path, output.toByteArray());
    }
}
