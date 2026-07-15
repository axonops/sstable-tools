package com.csforge.sstable.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable runtime identity fields suitable for a workspace manifest. */
public final class RuntimeIdentity {
    private final CassandraInstallation installation;
    private final String toolSha256;
    private final String configurationSha256;
    private final Map<Path, String> jarSha256;

    private RuntimeIdentity(CassandraInstallation installation,
                            String toolSha256,
                            String configurationSha256,
                            Map<Path, String> jarSha256) {
        this.installation = installation;
        this.toolSha256 = toolSha256;
        this.configurationSha256 = configurationSha256;
        this.jarSha256 = Collections.unmodifiableMap(new LinkedHashMap<>(jarSha256));
    }

    public static RuntimeIdentity capture(CassandraInstallation installation)
            throws BootstrapException {
        String toolHash = Files.isRegularFile(installation.toolPath())
                ? sha256(installation.toolPath())
                : "development-directory";
        String configurationHash = sha256(installation.conf().resolve("cassandra.yaml"));
        Map<Path, String> jarHashes = new LinkedHashMap<>();
        for (Path entry : installation.classpath()) {
            if (Files.isRegularFile(entry) && entry.getFileName().toString().endsWith(".jar")
                    && !entry.equals(installation.toolPath())) {
                jarHashes.put(entry, sha256(entry));
            }
        }
        return new RuntimeIdentity(installation, toolHash, configurationHash, jarHashes);
    }

    public List<String> asPropertyLines(AdapterMetadata adapter) {
        List<String> lines = new ArrayList<>();
        for (Map.Entry<String, String> entry : asMap(adapter).entrySet()) {
            lines.add(entry.getKey() + "=" + entry.getValue());
        }
        return Collections.unmodifiableList(lines);
    }

    public Map<String, String> asMap(AdapterMetadata adapter) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("adapter.release-line", adapter.releaseLine());
        values.put("adapter.tested-range",
                adapter.minimumVersion() + ".." + adapter.maximumVersion());
        values.put("tool.path", installation.toolPath().toString());
        values.put("tool.sha256", toolSha256);
        values.put("cassandra.home", installation.home().toString());
        values.put("cassandra.conf", installation.conf().toString());
        values.put("cassandra.conf.sha256", configurationSha256);
        values.put("cassandra.version", installation.version().toString());
        values.put("cassandra.server-jar", installation.serverJar().toString());
        values.put("java.home", installation.java().home().toString());
        values.put("java.executable", installation.java().executable().toString());
        values.put("java.version", installation.java().version());
        values.put("java.major", Integer.toString(installation.java().majorVersion()));
        values.put("classpath.count", Integer.toString(installation.classpath().size()));
        for (int index = 0; index < installation.classpath().size(); index++) {
            values.put("classpath." + index, installation.classpath().get(index).toString());
        }
        int index = 0;
        for (Map.Entry<Path, String> entry : jarSha256.entrySet()) {
            values.put("cassandra.jar." + index + ".path", entry.getKey().toString());
            values.put("cassandra.jar." + index + ".sha256", entry.getValue());
            index++;
        }
        return Collections.unmodifiableMap(values);
    }

    private static String sha256(Path path) throws BootstrapException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK does not provide SHA-256", e);
        }

        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, count);
            }
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot hash runtime file " + path, e);
        }

        StringBuilder result = new StringBuilder(64);
        for (byte value : digest.digest()) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
