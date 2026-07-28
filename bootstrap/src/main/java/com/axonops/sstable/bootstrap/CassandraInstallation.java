package com.axonops.sstable.bootstrap;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Fully resolved runtime needed to start one release-specific worker child. */
public final class CassandraInstallation {
    private final Path home;
    private final Optional<Path> supportDirectory;
    private final Path serverJar;
    private final CassandraVersion version;
    private final JavaInstallation java;
    private final Path toolPath;
    private final List<Path> classpath;

    public CassandraInstallation(Path home,
                                 Path supportDirectory,
                                 Path serverJar,
                                 CassandraVersion version,
                                 JavaInstallation java,
                                 Path toolPath,
                                 List<Path> classpath) {
        this.home = home;
        this.supportDirectory = Optional.ofNullable(supportDirectory);
        this.serverJar = serverJar;
        this.version = version;
        this.java = java;
        this.toolPath = toolPath;
        this.classpath = Collections.unmodifiableList(new ArrayList<>(classpath));
    }

    public Path home() {
        return home;
    }

    public Optional<Path> supportDirectory() {
        return supportDirectory;
    }

    public Path serverJar() {
        return serverJar;
    }

    public CassandraVersion version() {
        return version;
    }

    public JavaInstallation java() {
        return java;
    }

    public Path toolPath() {
        return toolPath;
    }

    public List<Path> classpath() {
        return classpath;
    }
}
