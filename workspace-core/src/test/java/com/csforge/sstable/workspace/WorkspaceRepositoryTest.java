package com.csforge.sstable.workspace;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceRepositoryTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void initializesPersistsAndTransitionsAtomically() throws Exception {
        Path root = temporary.newFolder("repository").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        Instant now = Instant.parse("2026-07-15T10:00:00Z");
        WorkspaceManifest manifest = WorkspaceManifest.create(UUID.randomUUID(), now,
                WorkspaceTestFixtures.inventory(root));

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            WorkspaceManifest validated = manifest.transitionTo(WorkspaceState.VALIDATED,
                    now.plusSeconds(1));
            repository.save(lock, validated);
        }

        Assert.assertEquals(WorkspaceState.VALIDATED, repository.load().state());
        Assert.assertTrue(Files.isDirectory(root.resolve("data")));
        Assert.assertTrue(Files.isDirectory(root.resolve("exports")));
    }

    @Test
    public void concurrentControllersCannotAcquireLock() throws Exception {
        WorkspaceRepository repository = WorkspaceRepository.createAt(
                temporary.newFolder("locked").toPath());

        try (WorkspaceLock ignored = repository.acquire()) {
            try {
                repository.acquire();
                Assert.fail("Expected overlapping lock failure");
            } catch (WorkspaceException e) {
                Assert.assertTrue(e.getMessage().contains("already locked"));
            }
        }
    }

    @Test
    public void staleLockFileDoesNotBlockController() throws Exception {
        Path root = temporary.newFolder("stale-lock").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        Files.write(root.resolve(".workspace.lock"),
                "jvm=dead-process\n".getBytes(StandardCharsets.UTF_8));

        try (WorkspaceLock ignored = repository.acquire()) {
            String identity = new String(Files.readAllBytes(root.resolve(".workspace.lock")),
                    StandardCharsets.UTF_8);
            Assert.assertTrue(identity.contains("acquiredAt="));
        }
    }

    @Test
    public void strayPartialManifestNeverReplacesValidState() throws Exception {
        Path root = temporary.newFolder("partial").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root));
        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
        }
        Files.write(root.resolve(".manifest.json.interrupted.tmp"),
                "{\"formatVersion\":".getBytes(StandardCharsets.UTF_8));

        Assert.assertEquals(manifest, repository.load());
    }

    @Test
    public void sourceMutationBlocksFurtherWork() throws Exception {
        Path root = temporary.newFolder("source-change").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root));
        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
        }
        Path source = manifest.sourceInventory().sets().get(0).components().get(0).path();
        Files.write(source, "changed".getBytes(StandardCharsets.UTF_8));

        try {
            repository.load().sourceInventory().verifyUnchanged();
            Assert.fail("Expected source mutation failure");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("Source mutation detected"));
        }
    }

    @Test
    public void workspacePathsRejectTraversalAndSymlinkEscape() throws Exception {
        Path root = temporary.newFolder("confined").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        assertPathFailure(repository, "../outside", "normalized and relative");
        assertPathFailure(repository, root.resolve("absolute").toString(),
                "normalized and relative");

        Path outside = temporary.newFolder("outside").toPath();
        Path link = root.resolve("escape");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | java.io.IOException e) {
            Assume.assumeNoException("Symbolic links are unavailable", e);
        }
        assertPathFailure(repository, "escape/file", "crosses a symlink");
    }

    @Test
    public void writesWorkspaceOwnedFilesAtomicallyUnderTheRequiredLock() throws Exception {
        Path root = temporary.newFolder("owned-file").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root));
        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            repository.writeOwnedFile(lock, "runtime/cassandra.yaml",
                    "cluster_name: test\n".getBytes(StandardCharsets.UTF_8));
        }

        Assert.assertEquals("cluster_name: test\n",
                new String(Files.readAllBytes(root.resolve("runtime/cassandra.yaml")),
                        StandardCharsets.UTF_8));
        Assert.assertTrue(Files.isDirectory(root.resolve("hints")));
        Assert.assertTrue(Files.isDirectory(root.resolve("saved_caches")));

        try {
            repository.writeOwnedFile(null, "runtime/unsafe", new byte[]{1});
            Assert.fail("Expected lock requirement");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("exclusive workspace lock"));
        }
    }

    private static void assertPathFailure(WorkspaceRepository repository,
                                          String path,
                                          String expected) throws Exception {
        try {
            repository.resolveInside(path);
            Assert.fail("Expected path failure containing: " + expected);
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expected));
        }
    }
}
