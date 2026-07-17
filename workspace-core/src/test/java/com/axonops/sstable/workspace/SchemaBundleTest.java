package com.axonops.sstable.workspace;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SchemaBundleTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void capturesUtf8SchemaAndDetectsMutation() throws Exception {
        Path schema = temporary.newFile("schema.cql").toPath();
        Files.write(schema, "CREATE KEYSPACE test;\n".getBytes(StandardCharsets.UTF_8));

        SchemaBundle bundle = SchemaBundle.capture(schema);

        Assert.assertEquals(schema.toRealPath(), bundle.source());
        Assert.assertEquals(Files.size(schema), bundle.size());
        Assert.assertEquals(64, bundle.sha256().length());
        Assert.assertEquals(bundle.sha256(), bundle.identity().get("bundle.sha256"));

        Files.write(schema, "changed\n".getBytes(StandardCharsets.UTF_8));
        try {
            bundle.verifyUnchanged();
            Assert.fail("Expected changed schema bundle to be rejected");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("changed after workspace capture"));
        }
    }

    @Test
    public void rejectsSymlinkAndInvalidUtf8() throws Exception {
        Path target = temporary.newFile("target.cql").toPath();
        Files.write(target, "schema".getBytes(StandardCharsets.UTF_8));
        Path link = target.getParent().resolve("schema-link.cql");
        Files.createSymbolicLink(link, target.getFileName());
        assertCaptureFailure(link, "must not be a symlink");

        Path invalid = temporary.newFile("invalid.cql").toPath();
        Files.write(invalid, new byte[]{(byte) 0xc3, (byte) 0x28});
        assertCaptureFailure(invalid, "not valid UTF-8");
    }

    @Test
    public void inventoriesConfinedWorkspaceFiles() throws Exception {
        Path root = temporary.newFolder("workspace").toPath();
        Path table = root.resolve("data/ks/table-id");
        Files.createDirectories(table);
        Files.write(table.resolve("mc-2-big-Data.db"), new byte[]{2});
        Files.write(table.resolve("mc-1-big-Data.db"), new byte[]{1});

        List<ManifestFile> files = WorkspaceFileInventory.capture(root,
                "data/ks/table-id");

        Assert.assertEquals(2, files.size());
        Assert.assertEquals("data/ks/table-id/mc-1-big-Data.db",
                files.get(0).relativePath());
        Assert.assertEquals(64, files.get(0).sha256().length());
    }

    private static void assertCaptureFailure(Path path, String expected) throws Exception {
        try {
            SchemaBundle.capture(path);
            Assert.fail("Expected schema capture failure containing " + expected);
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expected));
        }
    }
}
