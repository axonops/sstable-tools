package com.axonops.sstable.workspace;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SourceInventoryTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void capturesCompleteTocDeclaredDescriptor() throws Exception {
        Path source = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("source").toPath());

        SourceInventory inventory = SourceInventory.capture(Collections.singletonList(source));

        Assert.assertEquals(1, inventory.sets().size());
        Assert.assertEquals(3, inventory.componentCount());
        Assert.assertEquals("ma-1-big", inventory.sets().get(0).descriptor());
        Assert.assertEquals("ma", inventory.sets().get(0).formatVersion());
        Assert.assertEquals("big", inventory.sets().get(0).format());
        inventory.verifyUnchanged();
    }

    @Test
    public void capturesOnlyExplicitlySelectedDataDescriptor() throws Exception {
        Path source = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("selected").toPath());
        Files.write(source.resolve("mb-2-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("mb-2-big-Data.db"), new byte[]{3});
        Files.write(source.resolve("mb-2-big-Statistics.db"), new byte[]{4});

        SourceInventory inventory = SourceInventory.capture(Collections.singletonList(
                source.resolve("ma-1-big-Data.db")));

        Assert.assertEquals(1, inventory.sets().size());
        Assert.assertEquals("ma-1-big", inventory.sets().get(0).descriptor());
        inventory.verifyUnchanged();
    }

    @Test
    public void rejectsStandaloneDataComponent() throws Exception {
        Path source = temporary.newFolder("standalone").toPath();
        Files.write(source.resolve("ma-1-big-Data.db"), new byte[]{1});

        assertCaptureFailure(source, "Data.db but no TOC.txt");
    }

    @Test
    public void rejectsMissingTocDeclaredComponent() throws Exception {
        Path source = temporary.newFolder("missing").toPath();
        Files.write(source.resolve("ma-1-big-Data.db"), new byte[]{1});
        Files.write(source.resolve("ma-1-big-TOC.txt"),
                Arrays.asList("TOC.txt", "Data.db", "Statistics.db"),
                StandardCharsets.UTF_8);

        assertCaptureFailure(source, "missing component");
    }

    @Test
    public void rejectsTemporaryComponents() throws Exception {
        Path source = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("temporary").toPath());
        Files.write(source.resolve("tmp-ma-1-big-Data.db"), new byte[]{1});

        assertCaptureFailure(source, "Temporary SSTable component");
    }

    @Test
    public void detectsSourceMutationByHash() throws Exception {
        Path source = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("mutated").toPath());
        SourceInventory inventory = SourceInventory.capture(Collections.singletonList(source));
        Path data = source.resolve("ma-1-big-Data.db");
        long originalSize = Files.size(data);
        Files.write(data, Collections.singletonList("DATA"), StandardCharsets.UTF_8);
        Assert.assertEquals(originalSize, Files.size(data));

        try {
            inventory.verifyUnchanged();
            Assert.fail("Expected source mutation failure");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("SHA-256 changed"));
        }
    }

    @Test
    public void detectsComponentReplacedBySymlinkEvenWhenContentMatches() throws Exception {
        Path source = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("symlink-mutation").toPath());
        SourceInventory inventory = SourceInventory.capture(Collections.singletonList(source));
        Path data = source.resolve("ma-1-big-Data.db");
        Path replacement = temporary.newFile("replacement.db").toPath();
        Files.copy(data, replacement, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.delete(data);
        try {
            Files.createSymbolicLink(data, replacement);
        } catch (UnsupportedOperationException | java.io.IOException e) {
            Assume.assumeNoException("Symbolic links are unavailable", e);
        }

        try {
            inventory.verifyUnchanged();
            Assert.fail("Expected symlink mutation failure");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Source mutation detected"));
        }
    }

    private static void assertCaptureFailure(Path source, String message) throws Exception {
        try {
            SourceInventory.capture(Collections.singletonList(source));
            Assert.fail("Expected capture failure containing: " + message);
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }
}
