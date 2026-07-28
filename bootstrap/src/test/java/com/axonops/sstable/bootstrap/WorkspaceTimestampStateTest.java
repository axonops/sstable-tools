package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.WorkspaceLock;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceTimestampStateTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void initializesAfterSourceAndPreservesAdvancedHighWater() throws Exception {
        Path source = temporary.newFolder("source").toPath();
        Files.write(source.resolve("mc-1-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("mc-1-big-Data.db"), new byte[]{1});
        Files.write(source.resolve("mc-1-big-Statistics.db"), new byte[]{2});
        WorkspaceRepository repository = WorkspaceRepository.createAt(
                temporary.newFolder("workspace").toPath());
        WorkspaceManifest manifest = WorkspaceManifest.create(SourceInventory.capture(
                Collections.singletonList(source)));

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            WorkspaceTimestampState.prepare(repository, lock, manifest.workspaceId(),
                    TimestampPolicy.AFTER_SOURCE, 100L, true);
        }
        Path path = repository.root().resolve(WorkspaceTimestampState.WORKSPACE_PATH);
        String initialized = new String(Files.readAllBytes(path), StandardCharsets.US_ASCII);
        Assert.assertTrue(initialized.contains("high-water-micros=100"));
        Assert.assertEquals(Long.valueOf(100L),
                WorkspaceTimestampState.validatedHighWater(repository,
                        manifest.workspaceId(), TimestampPolicy.AFTER_SOURCE, 100L));

        Files.write(path, initialized.replace("high-water-micros=100",
                "high-water-micros=150").getBytes(StandardCharsets.US_ASCII));
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceTimestampState.prepare(repository, lock, manifest.workspaceId(),
                    TimestampPolicy.AFTER_SOURCE, 100L, false);
        }
        Assert.assertTrue(new String(Files.readAllBytes(path), StandardCharsets.US_ASCII)
                .contains("high-water-micros=150"));

        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceTimestampState.prepare(repository, lock, manifest.workspaceId(),
                    TimestampPolicy.WALL_CLOCK, 100L, true);
        }
        Assert.assertFalse(Files.exists(path));
    }

    @Test
    public void refusesToRecreateMissingPersistedAfterSourceState() throws Exception {
        WorkspaceRepository repository = WorkspaceRepository.createAt(
                temporary.newFolder("missing-state-workspace").toPath());
        java.util.UUID workspaceId = java.util.UUID.randomUUID();

        try (WorkspaceLock lock = repository.acquire()) {
            try {
                WorkspaceTimestampState.prepare(repository, lock, workspaceId,
                        TimestampPolicy.AFTER_SOURCE, 100L, false);
                Assert.fail("Expected missing durable timestamp state to fail");
            } catch (com.axonops.sstable.workspace.WorkspaceException expected) {
                Assert.assertTrue(expected.getMessage().contains("is missing"));
            }
        }
    }
}
