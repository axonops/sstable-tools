package com.axonops.sstable.worker.api;

import java.nio.file.Path;
import java.util.UUID;

/** Immutable controller inputs for a native-transport-disabled import worker. */
public final class ImportOptions {
    private final Path workspaceRoot;
    private final Path configurationFile;
    private final UUID workspaceId;

    public ImportOptions(Path workspaceRoot, Path configurationFile, UUID workspaceId) {
        if (workspaceRoot == null || !workspaceRoot.isAbsolute()
                || configurationFile == null || !configurationFile.isAbsolute()
                || !configurationFile.normalize().startsWith(workspaceRoot.normalize())
                || workspaceId == null) {
            throw new IllegalArgumentException("Invalid import worker options");
        }
        this.workspaceRoot = workspaceRoot.normalize();
        this.configurationFile = configurationFile.normalize();
        this.workspaceId = workspaceId;
    }

    public Path workspaceRoot() {
        return workspaceRoot;
    }

    public Path configurationFile() {
        return configurationFile;
    }

    public UUID workspaceId() {
        return workspaceId;
    }
}
