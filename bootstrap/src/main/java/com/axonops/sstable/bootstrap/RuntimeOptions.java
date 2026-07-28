package com.axonops.sstable.bootstrap;

import java.nio.file.Path;

/** Explicit runtime paths supplied by the command line. */
public final class RuntimeOptions {
    private final Path cassandraHome;
    private final Path javaHome;

    public RuntimeOptions(Path cassandraHome, Path javaHome) {
        this.cassandraHome = cassandraHome;
        this.javaHome = javaHome;
    }

    public Path cassandraHome() {
        return cassandraHome;
    }

    public Path javaHome() {
        return javaHome;
    }
}
