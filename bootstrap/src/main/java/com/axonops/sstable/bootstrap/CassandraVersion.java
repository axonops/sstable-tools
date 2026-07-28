package com.axonops.sstable.bootstrap;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Comparable Cassandra release version using its numeric major/minor/patch core. */
public final class CassandraVersion implements Comparable<CassandraVersion> {
    private static final Pattern VERSION = Pattern.compile(
            "^(\\d+)\\.(\\d+)\\.(\\d+)(?:[-.+~].*)?$");

    private final String value;
    private final int major;
    private final int minor;
    private final int patch;

    private CassandraVersion(String value, int major, int minor, int patch) {
        this.value = value;
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    public static CassandraVersion parse(String value) throws BootstrapException {
        String candidate = value == null ? "" : value.trim();
        Matcher matcher = VERSION.matcher(candidate);
        if (!matcher.matches()) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot parse Cassandra release version '" + candidate + "'");
        }

        try {
            return new CassandraVersion(candidate,
                    Integer.parseInt(matcher.group(1)),
                    Integer.parseInt(matcher.group(2)),
                    Integer.parseInt(matcher.group(3)));
        } catch (NumberFormatException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cassandra release version is outside the supported numeric range: " + candidate, e);
        }
    }

    public int major() {
        return major;
    }

    public int minor() {
        return minor;
    }

    public int patch() {
        return patch;
    }

    public String releaseLine() {
        return major + "." + minor;
    }

    @Override
    public int compareTo(CassandraVersion other) {
        int result = Integer.compare(major, other.major);
        if (result == 0) {
            result = Integer.compare(minor, other.minor);
        }
        if (result == 0) {
            result = Integer.compare(patch, other.patch);
        }
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CassandraVersion)) {
            return false;
        }
        CassandraVersion that = (CassandraVersion) other;
        return major == that.major && minor == that.minor && patch == that.patch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch);
    }

    @Override
    public String toString() {
        return value;
    }
}
