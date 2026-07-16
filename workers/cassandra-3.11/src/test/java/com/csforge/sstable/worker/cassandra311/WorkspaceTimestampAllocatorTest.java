package com.csforge.sstable.worker.cassandra311;

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

public class WorkspaceTimestampAllocatorTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void persistsMonotonicHighWaterAcrossAllocatorRestart() throws Exception {
        Path workspace = temporary.newFolder("timestamp-workspace").toPath().toRealPath();
        Path state = Files.createDirectory(workspace.resolve("state"));
        UUID workspaceId = UUID.randomUUID();
        Path path = state.resolve("timestamp.properties");
        writeState(path, workspaceId, 100L, 100L);

        WorkspaceTimestampAllocator first = WorkspaceTimestampAllocator.open(
                workspace, workspaceId, 100L);
        long firstTimestamp = first.next();
        Assert.assertTrue(firstTimestamp > 100L);
        Assert.assertEquals(firstTimestamp, first.highWaterMicros());
        Assert.assertTrue(Files.readAllLines(path, StandardCharsets.US_ASCII)
                .contains("high-water-micros=" + firstTimestamp));

        WorkspaceTimestampAllocator restarted = WorkspaceTimestampAllocator.open(
                workspace, workspaceId, 100L);
        long secondTimestamp = restarted.next();
        Assert.assertTrue(secondTimestamp > firstTimestamp);
    }

    @Test
    public void rejectsStateForAnotherWorkspace() throws Exception {
        Path workspace = temporary.newFolder("wrong-workspace").toPath().toRealPath();
        Path state = Files.createDirectory(workspace.resolve("state"));
        Path path = state.resolve("timestamp.properties");
        writeState(path, UUID.randomUUID(), 100L, 100L);

        try {
            WorkspaceTimestampAllocator.open(workspace, UUID.randomUUID(), 100L);
            Assert.fail("Expected mismatched timestamp state to fail");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cannot load"));
        }
    }

    private static void writeState(Path path,
                                   UUID workspaceId,
                                   long sourceMaximum,
                                   long highWater) throws Exception {
        List<String> lines = java.util.Arrays.asList(
                "version=1",
                "workspace.id=" + workspaceId,
                "policy=after-source",
                "source.max-timestamp-micros=" + sourceMaximum,
                "high-water-micros=" + highWater);
        Files.write(path, lines, StandardCharsets.US_ASCII);
        try {
            Files.setPosixFilePermissions(path,
                    PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The implementation also supports ACL-based non-POSIX workspaces.
        }
    }
}
