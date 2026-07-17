package com.axonops.sstable.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Finds one unambiguous installed Cassandra runtime without loading its classes. */
public final class RuntimeDiscovery {
    private static final String DAEMON_CLASS =
            "org/apache/cassandra/service/CassandraDaemon.class";
    private static final List<String> VERSION_PROPERTY_ENTRIES = Arrays.asList(
            "META-INF/maven/org.apache.cassandra/cassandra-all/pom.properties",
            "META-INF/maven/org.apache.cassandra/apache-cassandra/pom.properties");
    private static final Pattern VERSIONED_SERVER_JAR = Pattern.compile(
            "(?:cassandra-all|apache-cassandra)-([0-9]+\\.[0-9]+\\.[0-9]+(?:[-.+~][^/]*)?)\\.jar");

    private final Path toolPath;
    private final Map<String, String> environment;
    private final Properties systemProperties;
    private final List<Path> knownHomes;
    private final List<Path> knownConfigurations;

    public RuntimeDiscovery(Path toolPath,
                            Map<String, String> environment,
                            Properties systemProperties) {
        this(toolPath, environment, systemProperties,
                Collections.singletonList(Paths.get("/usr/share/cassandra")),
                Arrays.asList(Paths.get("/etc/cassandra"), Paths.get("/etc/cassandra/conf")));
    }

    RuntimeDiscovery(Path toolPath,
                     Map<String, String> environment,
                     Properties systemProperties,
                     List<Path> knownHomes,
                     List<Path> knownConfigurations) {
        this.toolPath = toolPath;
        this.environment = environment;
        this.systemProperties = systemProperties;
        this.knownHomes = new ArrayList<>(knownHomes);
        this.knownConfigurations = new ArrayList<>(knownConfigurations);
    }

    public CassandraInstallation discover(RuntimeOptions options, AdapterMetadata adapter)
            throws BootstrapException {
        Path home = resolveHome(options.cassandraHome());
        List<Path> jars = listRuntimeJars(home);
        ServerJar server = findServerJar(jars);
        Path conf = resolveConfiguration(options.cassandraConf(), home);
        JavaInstallation java = JavaInstallation.discover(
                options.javaHome(), environment, systemProperties);
        adapter.validate(server.version, java.majorVersion());

        Path realToolPath = canonicalExisting(toolPath, "tool artifact");
        List<Path> classpath = new ArrayList<>();
        classpath.add(realToolPath);
        classpath.add(conf);
        classpath.addAll(jars);

        return new CassandraInstallation(home, conf, server.path, server.version,
                java, realToolPath, classpath);
    }

    private Path resolveHome(Path explicitHome) throws BootstrapException {
        if (explicitHome != null) {
            return canonicalDirectory(explicitHome, "Cassandra home");
        }

        String environmentHome = environment.get("CASSANDRA_HOME");
        if (hasText(environmentHome)) {
            return canonicalDirectory(Paths.get(environmentHome), "CASSANDRA_HOME");
        }

        List<Path> candidates = new ArrayList<>();
        for (Path candidate : knownHomes) {
            if (Files.isDirectory(candidate) && containsServerJar(candidate)) {
                candidates.add(canonicalDirectory(candidate, "Cassandra home candidate"));
            }
        }
        if (candidates.isEmpty()) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot locate Cassandra; use --cassandra-home or CASSANDRA_HOME");
        }
        if (candidates.size() > 1) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Multiple Cassandra installations were found: " + candidates
                            + "; select one with --cassandra-home");
        }
        return candidates.get(0);
    }

    private Path resolveConfiguration(Path explicitConf, Path home) throws BootstrapException {
        if (explicitConf != null) {
            return validateConfiguration(explicitConf, "Cassandra configuration");
        }

        String environmentConf = environment.get("CASSANDRA_CONF");
        if (hasText(environmentConf)) {
            return validateConfiguration(Paths.get(environmentConf), "CASSANDRA_CONF");
        }

        Path tarballConf = home.resolve("conf");
        if (Files.isRegularFile(tarballConf.resolve("cassandra.yaml"))) {
            return validateConfiguration(tarballConf, "Cassandra tarball configuration");
        }

        List<Path> candidates = new ArrayList<>();
        for (Path candidate : knownConfigurations) {
            if (Files.isRegularFile(candidate.resolve("cassandra.yaml"))) {
                candidates.add(validateConfiguration(candidate,
                        "Cassandra package configuration"));
            }
        }
        if (candidates.isEmpty()) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot locate cassandra.yaml; use --cassandra-conf or CASSANDRA_CONF");
        }
        if (candidates.size() > 1) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Multiple Cassandra configuration directories were found: " + candidates
                            + "; select one with --cassandra-conf");
        }
        return candidates.get(0);
    }

    private static Path validateConfiguration(Path path, String description)
            throws BootstrapException {
        Path directory = canonicalDirectory(path, description);
        if (!Files.isRegularFile(directory.resolve("cassandra.yaml"))) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    description + " does not contain cassandra.yaml: " + directory);
        }
        return directory;
    }

    private static boolean containsServerJar(Path home) throws BootstrapException {
        return !serverJars(listRuntimeJars(home)).isEmpty();
    }

    static List<Path> listRuntimeJars(Path home) throws BootstrapException {
        Set<Path> jars = new LinkedHashSet<>();
        addJars(home, jars);
        Path lib = home.resolve("lib");
        if (Files.isDirectory(lib)) {
            addJars(lib, jars);
        }
        List<Path> sorted = new ArrayList<>(jars);
        sorted.sort(Comparator.comparing(Path::toString));
        return Collections.unmodifiableList(sorted);
    }

    private static void addJars(Path directory, Set<Path> jars) throws BootstrapException {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*.jar")) {
            for (Path entry : entries) {
                if (Files.isRegularFile(entry)) {
                    jars.add(entry.toRealPath());
                }
            }
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot inventory Cassandra JARs in " + directory, e);
        }
    }

    private static ServerJar findServerJar(List<Path> jars) throws BootstrapException {
        List<ServerJar> candidates = serverJars(jars);
        if (candidates.isEmpty()) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "No Cassandra server JAR containing " + DAEMON_CLASS + " was found");
        }
        if (candidates.size() > 1) {
            throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    "Multiple Cassandra server JARs were found: " + candidates
                            + "; remove duplicate or mixed-version JARs");
        }
        return candidates.get(0);
    }

    private static List<ServerJar> serverJars(List<Path> jars) throws BootstrapException {
        List<ServerJar> candidates = new ArrayList<>();
        for (Path path : jars) {
            try (JarFile jar = new JarFile(path.toFile())) {
                if (jar.getJarEntry(DAEMON_CLASS) != null) {
                    candidates.add(new ServerJar(path, readVersion(path, jar)));
                }
            } catch (IOException e) {
                throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                        "Cannot inspect Cassandra classpath JAR " + path, e);
            }
        }
        return candidates;
    }

    private static CassandraVersion readVersion(Path path, JarFile jar)
            throws IOException, BootstrapException {
        for (String entryName : VERSION_PROPERTY_ENTRIES) {
            JarEntry entry = jar.getJarEntry(entryName);
            if (entry != null) {
                Properties properties = new Properties();
                try (InputStream input = jar.getInputStream(entry)) {
                    properties.load(input);
                }
                String value = properties.getProperty("version");
                if (hasText(value)) {
                    return CassandraVersion.parse(value);
                }
            }
        }

        Manifest manifest = jar.getManifest();
        if (manifest != null) {
            Attributes attributes = manifest.getMainAttributes();
            for (String name : Arrays.asList("Implementation-Version", "Bundle-Version")) {
                String value = attributes.getValue(name);
                if (hasText(value)) {
                    return CassandraVersion.parse(value);
                }
            }
        }

        Matcher matcher = VERSIONED_SERVER_JAR.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return CassandraVersion.parse(matcher.group(1));
        }
        throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                "Cannot determine Cassandra version from server JAR " + path);
    }

    private static Path canonicalDirectory(Path path, String description)
            throws BootstrapException {
        Path real = canonicalExisting(path, description);
        if (!Files.isDirectory(real)) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    description + " is not a directory: " + path);
        }
        return real;
    }

    private static Path canonicalExisting(Path path, String description)
            throws BootstrapException {
        try {
            return path.toRealPath();
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    description + " does not exist or cannot be resolved: " + path, e);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static final class ServerJar {
        private final Path path;
        private final CassandraVersion version;

        private ServerJar(Path path, CassandraVersion version) {
            this.path = path;
            this.version = version;
        }

        @Override
        public String toString() {
            return path + " (" + version + ")";
        }
    }
}
