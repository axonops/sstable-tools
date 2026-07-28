package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.WorkspaceException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SstableIdentifierStyleTest {
    private static final String UUID_IDENTIFIER = "1234_0001_000010000000000001";

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void infersNumericStyleFromNumericSelection() throws Exception {
        Assert.assertEquals(SstableIdentifierStyle.NUMERIC,
                SstableIdentifierStyle.infer(source("oa-41-big")));
    }

    @Test
    public void infersUuidStyleFromUuidSelection() throws Exception {
        Assert.assertEquals(SstableIdentifierStyle.UUID,
                SstableIdentifierStyle.infer(source("oa-" + UUID_IDENTIFIER + "-big")));
    }

    @Test
    public void mixedSelectionUsesUuidStyle() throws Exception {
        Path directory = temporary.newFolder("mixed").toPath();
        writeDescriptor(directory, "oa-41-big");
        writeDescriptor(directory, "oa-" + UUID_IDENTIFIER + "-big");
        Assert.assertEquals(SstableIdentifierStyle.UUID,
                SstableIdentifierStyle.infer(SourceInventory.capture(
                        Collections.singletonList(directory))));
    }

    @Test
    public void defaultsAnEmptyCassandraFiveDirectoryToUuidStyle() throws Exception {
        SourceInventory empty = SourceInventory.captureDirectoryAllowEmpty(
                temporary.newFolder("empty").toPath());

        Assert.assertEquals(SstableIdentifierStyle.UUID,
                SstableIdentifierStyle.forImport(empty, "5.0"));
        Assert.assertEquals(SstableIdentifierStyle.NUMERIC,
                SstableIdentifierStyle.forImport(empty, "4.1"));
    }

    @Test
    public void rejectsUnknownSelectedIdentifierStyle() throws Exception {
        try {
            SstableIdentifierStyle.infer(source("oa-not_an_identifier-big"));
            Assert.fail("Expected identifier inference failure");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("Cannot infer SSTable identifier style"));
        }
    }

    private SourceInventory source(String descriptor) throws Exception {
        Path directory = temporary.newFolder().toPath();
        writeDescriptor(directory, descriptor);
        return SourceInventory.capture(Collections.singletonList(directory));
    }

    private static void writeDescriptor(Path directory, String descriptor) throws Exception {
        Files.write(directory.resolve(descriptor + "-Data.db"), new byte[]{1});
        Files.write(directory.resolve(descriptor + "-Statistics.db"), new byte[]{2});
        Files.write(directory.resolve(descriptor + "-TOC.txt"),
                "Data.db\nStatistics.db\nTOC.txt\n".getBytes(StandardCharsets.UTF_8));
    }
}
