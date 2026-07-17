package com.axonops.sstable.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Cassandra-free compatibility policy embedded in each release artifact. */
public final class AdapterMetadata {
    public static final String RESOURCE_NAME = "sstable-tools-adapter.properties";

    private final String releaseLine;
    private final CassandraVersion compiledVersion;
    private final CassandraVersion minimumVersion;
    private final CassandraVersion maximumVersion;
    private final String runtimeClass;
    private final int minimumJava;
    private final int maximumJava;
    private final boolean importSupported;

    private AdapterMetadata(String releaseLine,
                            CassandraVersion compiledVersion,
                            CassandraVersion minimumVersion,
                            CassandraVersion maximumVersion,
                            String runtimeClass,
                            int minimumJava,
                            int maximumJava,
                            boolean importSupported) {
        this.releaseLine = releaseLine;
        this.compiledVersion = compiledVersion;
        this.minimumVersion = minimumVersion;
        this.maximumVersion = maximumVersion;
        this.runtimeClass = runtimeClass;
        this.minimumJava = minimumJava;
        this.maximumJava = maximumJava;
        this.importSupported = importSupported;
    }

    public static AdapterMetadata loadRequired(ClassLoader classLoader) throws BootstrapException {
        Properties properties = new Properties();
        try (InputStream input = classLoader.getResourceAsStream(RESOURCE_NAME)) {
            if (input == null) {
                throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                        "The artifact does not contain " + RESOURCE_NAME);
            }
            properties.load(input);
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot read " + RESOURCE_NAME, e);
        }
        return fromProperties(properties);
    }

    public static AdapterMetadata fromProperties(Properties properties) throws BootstrapException {
        String releaseLine = required(properties, "adapter.release-line");
        CassandraVersion compiled = CassandraVersion.parse(
                required(properties, "adapter.cassandra-version"));
        CassandraVersion minimum = CassandraVersion.parse(
                required(properties, "adapter.minimum-cassandra-version"));
        CassandraVersion maximum = CassandraVersion.parse(
                required(properties, "adapter.maximum-cassandra-version"));
        String runtimeClass = required(properties, "adapter.runtime-class");
        int minimumJava = positiveInteger(properties, "adapter.minimum-java-version");
        int maximumJava = positiveInteger(properties, "adapter.maximum-java-version");
        boolean importSupported = optionalBoolean(properties, "adapter.import-supported", false);

        if (!compiled.releaseLine().equals(releaseLine)) {
            throw invalid("adapter.cassandra-version does not belong to release line " + releaseLine);
        }
        if (!minimum.releaseLine().equals(releaseLine)
                || !maximum.releaseLine().equals(releaseLine)) {
            throw invalid("supported Cassandra versions must belong to release line " + releaseLine);
        }
        if (minimum.compareTo(maximum) > 0) {
            throw invalid("minimum Cassandra version is newer than maximum Cassandra version");
        }
        if (minimumJava > maximumJava) {
            throw invalid("minimum Java version is newer than maximum Java version");
        }

        return new AdapterMetadata(releaseLine, compiled, minimum, maximum,
                runtimeClass, minimumJava, maximumJava, importSupported);
    }

    private static String required(Properties properties, String name) throws BootstrapException {
        String value = properties.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw invalid("missing required property " + name);
        }
        return value.trim();
    }

    private static int positiveInteger(Properties properties, String name) throws BootstrapException {
        String value = required(properties, name);
        try {
            int result = Integer.parseInt(value);
            if (result <= 0) {
                throw new NumberFormatException("not positive");
            }
            return result;
        } catch (NumberFormatException e) {
            throw invalid(name + " must be a positive integer");
        }
    }

    private static boolean optionalBoolean(Properties properties, String name, boolean fallback)
            throws BootstrapException {
        String value = properties.getProperty(name);
        if (value == null) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        throw invalid(name + " must be true or false");
    }

    private static BootstrapException invalid(String message) {
        return new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                "Invalid " + RESOURCE_NAME + ": " + message);
    }

    public void validate(CassandraVersion cassandraVersion, int javaMajor)
            throws BootstrapException {
        if (!releaseLine.equals(cassandraVersion.releaseLine())) {
            throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    "This artifact is for Cassandra " + releaseLine + " but the installation is "
                            + cassandraVersion);
        }
        if (cassandraVersion.compareTo(minimumVersion) < 0
                || cassandraVersion.compareTo(maximumVersion) > 0) {
            throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    "Cassandra " + cassandraVersion + " is outside this adapter's tested range "
                            + minimumVersion + " through " + maximumVersion);
        }
        if (javaMajor < minimumJava || javaMajor > maximumJava) {
            throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    "Java " + javaMajor + " is not supported by the Cassandra " + releaseLine
                            + " adapter; use Java " + javaRange());
        }
    }

    private String javaRange() {
        return minimumJava == maximumJava
                ? Integer.toString(minimumJava)
                : minimumJava + " through " + maximumJava;
    }

    public String releaseLine() {
        return releaseLine;
    }

    public CassandraVersion compiledVersion() {
        return compiledVersion;
    }

    public CassandraVersion minimumVersion() {
        return minimumVersion;
    }

    public CassandraVersion maximumVersion() {
        return maximumVersion;
    }

    public String runtimeClass() {
        return runtimeClass;
    }

    public int minimumJava() {
        return minimumJava;
    }

    public int maximumJava() {
        return maximumJava;
    }

    public boolean importSupported() {
        return importSupported;
    }
}
