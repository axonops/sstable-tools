package com.axonops.sstable.bootstrap;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Fully resolved runtime needed to start one release-specific worker child. */
public final class CassandraInstallation {
    private final Path home;
    private final Path conf;
    private final Path serverJar;
    private final CassandraVersion version;
    private final JavaInstallation java;
    private final Path toolPath;
    private final List<Path> classpath;

    public CassandraInstallation(Path home,
                                 Path conf,
                                 Path serverJar,
                                 CassandraVersion version,
                                 JavaInstallation java,
                                 Path toolPath,
                                 List<Path> classpath) {
        this.home = home;
        this.conf = conf;
        this.serverJar = serverJar;
        this.version = version;
        this.java = java;
        this.toolPath = toolPath;
        this.classpath = Collections.unmodifiableList(new ArrayList<>(classpath));
    }

    public Path home() {
        return home;
    }

    public Path conf() {
        return conf;
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
