package com.axonops.sstable.workspace;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SystemLocalImportDiagnosticsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void partialSelectionNamesEveryDescriptorAndDirectory() throws Exception {
        Path data = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("data-source").toPath());
        Path logData = WorkspaceTestFixtures.completeSstableDirectory(
                temporary.newFolder("log-data-source").toPath());
        renameDescriptor(logData, "ma-1-big", "mb-2-big");
        SourceInventory source = SourceInventory.capture(Arrays.asList(data, logData));

        String noRow = SystemLocalImportDiagnostics.noLocalRow(source);
        String partial = SystemLocalImportDiagnostics.partialClusterNameSelection(source);

        Assert.assertNotEquals(noRow, partial);
        Assert.assertTrue(partial, partial.contains("selection is partial"));
        Assert.assertTrue(partial, partial.contains("Select every SSTable for system.local"));
        Assert.assertTrue(partial, partial.contains("other Cassandra data directories"));
        Assert.assertTrue(partial, partial.contains("ma-1-big in " + data.toRealPath()));
        Assert.assertTrue(partial, partial.contains("mb-2-big in " + logData.toRealPath()));
    }

    private static void renameDescriptor(Path directory, String from, String to)
            throws Exception {
        Files.move(directory.resolve(from + "-TOC.txt"), directory.resolve(to + "-TOC.txt"));
        Files.move(directory.resolve(from + "-Data.db"), directory.resolve(to + "-Data.db"));
        Files.move(directory.resolve(from + "-Statistics.db"),
                directory.resolve(to + "-Statistics.db"));
    }
}
