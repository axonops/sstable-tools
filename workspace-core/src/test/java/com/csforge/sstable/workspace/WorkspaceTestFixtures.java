package com.csforge.sstable.workspace;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

final class WorkspaceTestFixtures {
    private WorkspaceTestFixtures() {
    }

    static Path completeSstableDirectory(Path directory) throws IOException {
        Files.createDirectories(directory);
        Files.write(directory.resolve("ma-1-big-Data.db"),
                Collections.singletonList("data"), StandardCharsets.UTF_8);
        Files.write(directory.resolve("ma-1-big-Statistics.db"),
                Collections.singletonList("statistics"), StandardCharsets.UTF_8);
        Files.write(directory.resolve("ma-1-big-TOC.txt"),
                Arrays.asList("TOC.txt", "Data.db", "Statistics.db"),
                StandardCharsets.UTF_8);
        return directory;
    }

    static SourceInventory inventory(Path root) throws Exception {
        return SourceInventory.capture(Collections.singletonList(
                completeSstableDirectory(root.resolve("source"))));
    }
}
