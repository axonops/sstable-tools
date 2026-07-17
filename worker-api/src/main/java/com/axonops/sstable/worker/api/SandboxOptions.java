package com.axonops.sstable.worker.api;

import java.nio.file.Path;
import java.util.UUID;

/** Cassandra-neutral immutable inputs for a release adapter sandbox. */
public final class SandboxOptions {
    private final Path workspaceRoot;
    private final Path configurationFile;
    private final UUID workspaceId;
    private final int nativePort;

    public SandboxOptions(Path workspaceRoot,
                          Path configurationFile,
                          UUID workspaceId,
                          int nativePort) {
        if (workspaceRoot == null || configurationFile == null || workspaceId == null
                || !workspaceRoot.isAbsolute() || !workspaceRoot.equals(workspaceRoot.normalize())
                || !configurationFile.isAbsolute()
                || !configurationFile.equals(configurationFile.normalize())
                || !configurationFile.startsWith(workspaceRoot)
                || nativePort < 1 || nativePort > 65535) {
            throw new IllegalArgumentException("Invalid Cassandra sandbox options");
        }
        this.workspaceRoot = workspaceRoot;
        this.configurationFile = configurationFile;
        this.workspaceId = workspaceId;
        this.nativePort = nativePort;
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

    public int nativePort() {
        return nativePort;
    }
}
