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
        lines.add("adapter.release-line=" + adapter.releaseLine());
        lines.add("adapter.tested-range=" + adapter.minimumVersion() + ".." + adapter.maximumVersion());
        lines.add("tool.path=" + installation.toolPath());
        lines.add("tool.sha256=" + toolSha256);
        lines.add("cassandra.home=" + installation.home());
        lines.add("cassandra.conf=" + installation.conf());
        lines.add("cassandra.conf.sha256=" + configurationSha256);
        lines.add("cassandra.version=" + installation.version());
        lines.add("cassandra.server-jar=" + installation.serverJar());
        lines.add("java.home=" + installation.java().home());
        lines.add("java.executable=" + installation.java().executable());
        lines.add("java.version=" + installation.java().version());
        lines.add("java.major=" + installation.java().majorVersion());
        lines.add("classpath.count=" + installation.classpath().size());
        for (int index = 0; index < installation.classpath().size(); index++) {
            lines.add("classpath." + index + "=" + installation.classpath().get(index));
        }
        int index = 0;
        for (Map.Entry<Path, String> entry : jarSha256.entrySet()) {
            lines.add("cassandra.jar." + index + ".path=" + entry.getKey());
            lines.add("cassandra.jar." + index + ".sha256=" + entry.getValue());
            index++;
        }
        return Collections.unmodifiableList(lines);
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
