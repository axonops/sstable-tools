package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.SstableSet;
import com.axonops.sstable.workspace.WorkspaceException;
import com.axonops.sstable.workspace.WorkspaceManifest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Resolves the Cassandra 5.0 storage format contract used by the target node. */
final class Cassandra50StorageCompatibility {
    static final String MANIFEST_KEY = "cassandra.storage-compatibility-mode";
    static final String CASSANDRA_4 = "CASSANDRA_4";
    static final String UPGRADING = "UPGRADING";
    static final String NONE = "NONE";

    private static final Pattern SETTING = Pattern.compile(
            "^\\s*storage_compatibility_mode\\s*:\\s*(['\"]?)"
                    + "([A-Za-z0-9_]+)\\1\\s*(?:#.*)?$");
    private static final Pattern SETTING_KEY = Pattern.compile(
            "^\\s*storage_compatibility_mode\\s*:.*$");

    private Cassandra50StorageCompatibility() {
    }

    static String resolve(CassandraInstallation installation,
                          SourceInventory inventory,
                          String outputFormat) throws WorkspaceException {
        return resolve(installation.home(), inventory, outputFormat, System.getenv());
    }

    static String resolve(Path cassandraHome,
                          SourceInventory inventory,
                          String outputFormat,
                          Map<String, String> environment) throws WorkspaceException {
        Path configuration = configurationPath(cassandraHome, environment);
        String mode = configuration == null
                ? inferFromInventory(inventory) : readMode(configuration);
        validateOutputFormat(mode, outputFormat, configuration);
        return mode;
    }

    static String required(WorkspaceManifest manifest) throws WorkspaceException {
        String value = manifest.outputIdentity().get(MANIFEST_KEY);
        if (value == null) {
            // Workspaces imported by releases before this setting was recorded always used NONE.
            return NONE;
        }
        return normalized(value, "workspace manifest");
    }

    static String bigVersion(String mode) throws WorkspaceException {
        return CASSANDRA_4.equals(normalized(mode, "storage compatibility mode"))
                ? "nb" : "oa";
    }

    private static Path configurationPath(Path cassandraHome,
                                          Map<String, String> environment)
            throws WorkspaceException {
        String configured = environment.get("CASSANDRA_CONF");
        if (configured != null && !configured.trim().isEmpty()) {
            final Path candidatePath;
            try {
                candidatePath = Paths.get(configured);
            } catch (RuntimeException e) {
                throw new WorkspaceException("CASSANDRA_CONF is not a valid path: "
                        + configured, e);
            }
            Path candidate = candidatePath;
            if (Files.isDirectory(candidate, LinkOption.NOFOLLOW_LINKS)) {
                candidate = candidate.resolve("cassandra.yaml");
            }
            if (!Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)
                    || !Files.isReadable(candidate)) {
                throw new WorkspaceException("CASSANDRA_CONF does not identify a readable "
                        + "cassandra.yaml: " + candidate);
            }
            return candidate;
        }

        Path homeConfiguration = cassandraHome.resolve("conf/cassandra.yaml");
        if (Files.exists(homeConfiguration, LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isRegularFile(homeConfiguration, LinkOption.NOFOLLOW_LINKS)
                    || !Files.isReadable(homeConfiguration)) {
                throw new WorkspaceException("Cassandra configuration is not a readable file: "
                        + homeConfiguration);
            }
            return homeConfiguration;
        }

        Path normalizedHome = cassandraHome.toAbsolutePath().normalize();
        if (normalizedHome.equals(Paths.get("/usr/share/cassandra"))) {
            Path packageConfiguration = Paths.get("/etc/cassandra/cassandra.yaml");
            if (!Files.isRegularFile(packageConfiguration, LinkOption.NOFOLLOW_LINKS)) {
                packageConfiguration = Paths.get(
                        "/etc/cassandra/default.conf/cassandra.yaml");
            }
            if (Files.isRegularFile(packageConfiguration, LinkOption.NOFOLLOW_LINKS)) {
                return packageConfiguration;
            }
        }
        return null;
    }

    private static String readMode(Path configuration) throws WorkspaceException {
        final List<String> lines;
        try {
            lines = Files.readAllLines(configuration, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read Cassandra storage compatibility mode from "
                    + configuration, e);
        }
        String selected = null;
        for (String line : lines) {
            Matcher matcher = SETTING.matcher(line);
            if (!matcher.matches()) {
                if (SETTING_KEY.matcher(line).matches()) {
                    throw new WorkspaceException("Malformed storage_compatibility_mode in "
                            + configuration + ": " + line.trim());
                }
                continue;
            }
            if (selected != null) {
                throw new WorkspaceException("Duplicate storage_compatibility_mode in "
                        + configuration);
            }
            selected = normalized(matcher.group(2), configuration.toString());
        }
        // Cassandra 5.0 defaults to Cassandra 4 storage compatibility when omitted.
        return selected == null ? CASSANDRA_4 : selected;
    }

    private static String inferFromInventory(SourceInventory inventory)
            throws WorkspaceException {
        for (SstableSet set : inventory.sets()) {
            if ("oa".equals(set.formatVersion()) || "da".equals(set.formatVersion())) {
                return NONE;
            }
        }
        // nb and older Big inputs are safest when written in Cassandra 4 compatible form.
        return CASSANDRA_4;
    }

    private static void validateOutputFormat(String mode,
                                             String outputFormat,
                                             Path configuration)
            throws WorkspaceException {
        if (CASSANDRA_4.equals(mode) && "bti".equals(outputFormat)) {
            String source = configuration == null
                    ? "the selected/existing SSTables"
                    : configuration.toString();
            throw new WorkspaceException("Cassandra 5.0 BTI output is unavailable in "
                    + "storage_compatibility_mode CASSANDRA_4 resolved from " + source
                    + "; use Big output or set the target Cassandra configuration to "
                    + "UPGRADING or NONE");
        }
    }

    private static String normalized(String value, String source) throws WorkspaceException {
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        if (!CASSANDRA_4.equals(normalized) && !UPGRADING.equals(normalized)
                && !NONE.equals(normalized)) {
            throw new WorkspaceException("Unsupported storage_compatibility_mode '" + value
                    + "' in " + source + "; expected CASSANDRA_4, UPGRADING, or NONE");
        }
        return normalized;
    }
}
