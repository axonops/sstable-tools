package com.csforge.sstable.workspace;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceFileInventoryTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void verifiesBaselineWhileAllowingNewDeltaFiles() throws Exception {
        Path workspace = temporary.newFolder("workspace").toPath();
        Path table = Files.createDirectories(workspace.resolve("data/test/items-id"));
        Path baseline = Files.write(table.resolve("mc-1-big-Data.db"),
                "base".getBytes(StandardCharsets.UTF_8));
        List<ManifestFile> inventory = WorkspaceFileInventory.capture(
                workspace, "data/test/items-id");

        Files.write(table.resolve("mc-2-big-Data.db"),
                "delta".getBytes(StandardCharsets.UTF_8));
        WorkspaceFileInventory.verifyUnchanged(workspace, inventory);

        Files.write(baseline, "changed".getBytes(StandardCharsets.UTF_8));
        try {
            WorkspaceFileInventory.verifyUnchanged(workspace, inventory);
            Assert.fail("Expected baseline mutation to fail");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("baseline file changed"));
        }
    }
}
