package com.axonops.sstable.workspace;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceFlushResultTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void persistsAndVerifiesCompleteFlushInventory() throws Exception {
        Path workspace = temporary.newFolder("flush-workspace").toPath().toRealPath();
        Files.createDirectory(workspace.resolve("state"));
        Path table = Files.createDirectories(workspace.resolve("data/blog/users-id"));
        Files.write(table.resolve("mc-1-big-Data.db"), new byte[]{1});
        List<ManifestFile> baseline = WorkspaceFileInventory.capture(
                workspace, "data/blog/users-id");
        Files.write(table.resolve("mc-2-big-Data.db"), new byte[]{2});

        UUID workspaceId = UUID.randomUUID();
        WorkspaceFlushResult captured = WorkspaceFlushResult.capture(workspace, workspaceId,
                "3.11.19", "blog", "users", "data/blog/users-id");
        captured.writeAtomically(workspace);

        WorkspaceFlushResult restored = WorkspaceFlushResult.read(workspace);
        restored.requireIdentity(workspaceId, "3.11.19", "blog", "users",
                "data/blog/users-id");
        restored.verifyCompleteInventory(workspace);
        Assert.assertEquals(2, restored.files().size());
        Assert.assertEquals(1, restored.deltaFiles(baseline).size());

        Files.write(table.resolve("mc-2-big-Data.db"), new byte[]{3});
        try {
            restored.verifyCompleteInventory(workspace);
            Assert.fail("Expected changed flushed file to fail verification");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("inventory changed"));
        }
    }

    @Test
    public void rejectsUnknownFlushResultFields() throws Exception {
        Path workspace = temporary.newFolder("invalid-flush-workspace").toPath().toRealPath();
        Path state = Files.createDirectory(workspace.resolve("state"));
        String json = "{\"formatVersion\":1,\"unknown\":true}\n";
        Files.write(state.resolve("flush-result.json"),
                json.getBytes(StandardCharsets.UTF_8));
        try {
            Files.setPosixFilePermissions(state.resolve("flush-result.json"),
                    PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The implementation also supports ACL-based non-POSIX workspaces.
        }

        try {
            WorkspaceFlushResult.read(workspace);
            Assert.fail("Expected unknown flush result field to fail");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("fields are missing or unknown"));
        }
    }
}
