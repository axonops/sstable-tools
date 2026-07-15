package com.csforge.sstable.bootstrap;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RuntimeDiscoveryTest {
    private static final String DAEMON_CLASS =
            "org/apache/cassandra/service/CassandraDaemon.class";

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void discoversTarballLayoutAndPreservesPathsWithSpaces() throws Exception {
        Path root = temporary.newFolder("runtime with spaces").toPath();
        Path tool = Files.createDirectory(root.resolve("tool classes"));
        Path home = Files.createDirectory(root.resolve("cassandra home"));
        Path lib = Files.createDirectory(home.resolve("lib"));
        Path conf = Files.createDirectory(home.resolve("conf"));
        Files.write(conf.resolve("cassandra.yaml"), "cluster_name: test\n"
                .getBytes(StandardCharsets.UTF_8));
        Path server = createServerJar(lib.resolve("cassandra-all-3.11.19.jar"), "3.11.19");
        Path dependency = createEmptyJar(lib.resolve("a dependency.jar"));
        Path javaHome = createJavaHome(root.resolve("java home"), "1.8.0_402");

        RuntimeDiscovery discovery = discovery(tool, Collections.<String, String>emptyMap(),
                Collections.<Path>emptyList(), Collections.<Path>emptyList());
        CassandraInstallation installation = discovery.discover(
                new RuntimeOptions(home, null, javaHome), metadata("3.11", "3.11.19", 8, 8));

        Assert.assertEquals(home.toRealPath(), installation.home());
        Assert.assertEquals(conf.toRealPath(), installation.conf());
        Assert.assertEquals(server.toRealPath(), installation.serverJar());
        Assert.assertEquals(CassandraVersion.parse("3.11.19"), installation.version());
        Assert.assertEquals(8, installation.java().majorVersion());
        Assert.assertEquals(tool.toRealPath(), installation.classpath().get(0));
        Assert.assertEquals(conf.toRealPath(), installation.classpath().get(1));
        Assert.assertEquals(dependency.toRealPath(), installation.classpath().get(2));
        Assert.assertEquals(server.toRealPath(), installation.classpath().get(3));

        List<String> command = new ChildProcessLauncher().preflightCommand(installation);
        Assert.assertEquals(7, command.size());
        Assert.assertEquals("-cp", command.get(1));
        Assert.assertTrue(command.get(2).contains("runtime with spaces"));
        Assert.assertEquals(installation.version().toString(), command.get(6));
    }

    @Test
    public void discoversPackageLayoutFromOneUnambiguousCandidate() throws Exception {
        Path root = temporary.newFolder("package-layout").toPath();
        Path tool = Files.createDirectory(root.resolve("tool"));
        Path home = Files.createDirectory(root.resolve("usr-share-cassandra"));
        Files.createDirectory(home.resolve("lib"));
        Path server = createServerJar(home.resolve("apache-cassandra-4.1.3.jar"), "4.1.3");
        Path conf = Files.createDirectory(root.resolve("etc-cassandra"));
        Files.write(conf.resolve("cassandra.yaml"), "cluster_name: package\n"
                .getBytes(StandardCharsets.UTF_8));
        Path javaHome = createJavaHome(root.resolve("jdk-11"), "11.0.24");
        Map<String, String> environment = new HashMap<>();
        environment.put("JAVA_HOME", javaHome.toString());

        RuntimeDiscovery discovery = discovery(tool, environment,
                Collections.singletonList(home), Collections.singletonList(conf));
        CassandraInstallation installation = discovery.discover(
                new RuntimeOptions(null, null, null), metadata("4.1", "4.1.3", 11, 11));

        Assert.assertEquals(home.toRealPath(), installation.home());
        Assert.assertEquals(conf.toRealPath(), installation.conf());
        Assert.assertEquals(server.toRealPath(), installation.serverJar());
    }

    @Test
    public void explicitPathsTakePriorityOverEnvironment() throws Exception {
        Path root = temporary.newFolder("precedence").toPath();
        Path tool = Files.createDirectory(root.resolve("tool"));
        Path explicitHome = tarball(root.resolve("explicit"), "3.11.19");
        Path environmentHome = tarball(root.resolve("environment"), "3.11.18");
        Path explicitJava = createJavaHome(root.resolve("java-explicit"), "1.8.0_402");
        Path environmentJava = createJavaHome(root.resolve("java-environment"), "17.0.11");
        Map<String, String> environment = new HashMap<>();
        environment.put("CASSANDRA_HOME", environmentHome.toString());
        environment.put("JAVA_HOME", environmentJava.toString());

        CassandraInstallation installation = discovery(tool, environment,
                Collections.<Path>emptyList(), Collections.<Path>emptyList()).discover(
                new RuntimeOptions(explicitHome, null, explicitJava),
                metadata("3.11", "3.11.19", 8, 8));

        Assert.assertEquals(explicitHome.toRealPath(), installation.home());
        Assert.assertEquals(explicitJava.toRealPath(), installation.java().home());
    }

    @Test
    public void rejectsWrongReleaseLine() throws Exception {
        Fixture fixture = fixture("wrong-release", "3.11.19", "1.8.0_402");
        assertDiscoveryFailure(fixture, metadata("4.0", "4.0.17", 8, 11),
                "artifact is for Cassandra 4.0");
    }

    @Test
    public void rejectsUntestedPatch() throws Exception {
        Fixture fixture = fixture("unsupported-patch", "3.11.18", "1.8.0_402");
        assertDiscoveryFailure(fixture, metadata("3.11", "3.11.19", 8, 8),
                "outside this adapter's tested range");
    }

    @Test
    public void rejectsIncompatibleJava() throws Exception {
        Fixture fixture = fixture("wrong-java", "3.11.19", "17.0.11");
        assertDiscoveryFailure(fixture, metadata("3.11", "3.11.19", 8, 8),
                "Java 17 is not supported");
    }

    @Test
    public void rejectsMultipleServerJars() throws Exception {
        Fixture fixture = fixture("duplicate-server", "3.11.19", "1.8.0_402");
        createServerJar(fixture.home.resolve("lib").resolve("cassandra-all-4.0.17.jar"),
                "4.0.17");
        assertDiscoveryFailure(fixture, metadata("3.11", "3.11.19", 8, 8),
                "Multiple Cassandra server JARs");
    }

    @Test
    public void rejectsMissingServerJar() throws Exception {
        Path root = temporary.newFolder("missing-server").toPath();
        Path tool = Files.createDirectory(root.resolve("tool"));
        Path home = Files.createDirectory(root.resolve("home"));
        Files.createDirectory(home.resolve("lib"));
        Path conf = Files.createDirectory(home.resolve("conf"));
        Files.write(conf.resolve("cassandra.yaml"), new byte[0]);
        Path javaHome = createJavaHome(root.resolve("java"), "1.8.0_402");

        try {
            discovery(tool, Collections.<String, String>emptyMap(),
                    Collections.<Path>emptyList(), Collections.<Path>emptyList()).discover(
                    new RuntimeOptions(home, null, javaHome),
                    metadata("3.11", "3.11.19", 8, 8));
            Assert.fail("Expected missing server JAR failure");
        } catch (BootstrapException e) {
            Assert.assertTrue(e.getMessage().contains("No Cassandra server JAR"));
        }
    }

    @Test
    public void capturesStableRuntimeIdentityHashes() throws Exception {
        Fixture fixture = fixture("identity", "3.11.19", "1.8.0_402");
        CassandraInstallation installation = fixture.discovery.discover(
                new RuntimeOptions(fixture.home, null, fixture.javaHome),
                metadata("3.11", "3.11.19", 8, 8));

        List<String> lines = RuntimeIdentity.capture(installation)
                .asPropertyLines(metadata("3.11", "3.11.19", 8, 8));

        Assert.assertTrue(lines.contains("cassandra.version=3.11.19"));
        Assert.assertTrue(lines.stream().anyMatch(line -> line.startsWith("cassandra.jar.0.sha256=")));
        Assert.assertTrue(lines.stream().anyMatch(line -> line.startsWith("cassandra.conf.sha256=")));
    }

    private void assertDiscoveryFailure(Fixture fixture,
                                        AdapterMetadata metadata,
                                        String expectedMessage) throws Exception {
        try {
            fixture.discovery.discover(
                    new RuntimeOptions(fixture.home, null, fixture.javaHome), metadata);
            Assert.fail("Expected discovery failure containing: " + expectedMessage);
        } catch (BootstrapException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    private Fixture fixture(String name, String cassandraVersion, String javaVersion)
            throws Exception {
        Path root = temporary.newFolder(name).toPath();
        Path tool = Files.createDirectory(root.resolve("tool"));
        Path home = tarball(root.resolve("home"), cassandraVersion);
        Path javaHome = createJavaHome(root.resolve("java"), javaVersion);
        RuntimeDiscovery discovery = discovery(tool, Collections.<String, String>emptyMap(),
                Collections.<Path>emptyList(), Collections.<Path>emptyList());
        return new Fixture(discovery, home, javaHome);
    }

    private static RuntimeDiscovery discovery(Path tool,
                                              Map<String, String> environment,
                                              List<Path> knownHomes,
                                              List<Path> knownConfigurations) {
        Properties systemProperties = new Properties();
        return new RuntimeDiscovery(tool, environment, systemProperties,
                knownHomes, knownConfigurations);
    }

    private static AdapterMetadata metadata(String releaseLine,
                                            String version,
                                            int minimumJava,
                                            int maximumJava) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("adapter.release-line", releaseLine);
        properties.setProperty("adapter.cassandra-version", version);
        properties.setProperty("adapter.minimum-cassandra-version", version);
        properties.setProperty("adapter.maximum-cassandra-version", version);
        properties.setProperty("adapter.minimum-java-version", Integer.toString(minimumJava));
        properties.setProperty("adapter.maximum-java-version", Integer.toString(maximumJava));
        properties.setProperty("adapter.runtime-class", "example.Runtime");
        return AdapterMetadata.fromProperties(properties);
    }

    private static Path tarball(Path home, String version) throws IOException {
        Files.createDirectories(home.resolve("lib"));
        Files.createDirectories(home.resolve("conf"));
        Files.write(home.resolve("conf").resolve("cassandra.yaml"),
                "cluster_name: test\n".getBytes(StandardCharsets.UTF_8));
        createServerJar(home.resolve("lib").resolve("cassandra-all-" + version + ".jar"),
                version);
        return home;
    }

    private static Path createJavaHome(Path home, String version) throws IOException {
        Files.createDirectories(home.resolve("bin"));
        Path executable = Files.write(home.resolve("bin").resolve("java"),
                Arrays.asList("#!/bin/sh", "exit 0"), StandardCharsets.UTF_8);
        if (!executable.toFile().setExecutable(true)) {
            throw new IOException("Cannot make fake Java executable: " + executable);
        }
        Files.write(home.resolve("release"),
                Collections.singletonList("JAVA_VERSION=\"" + version + "\""),
                StandardCharsets.ISO_8859_1);
        return home;
    }

    private static Path createServerJar(Path path, String version) throws IOException {
        try (OutputStream output = Files.newOutputStream(path);
             JarOutputStream jar = new JarOutputStream(output)) {
            jar.putNextEntry(new JarEntry(DAEMON_CLASS));
            jar.closeEntry();
            jar.putNextEntry(new JarEntry(
                    "META-INF/maven/org.apache.cassandra/cassandra-all/pom.properties"));
            jar.write(("version=" + version + "\n").getBytes(StandardCharsets.ISO_8859_1));
            jar.closeEntry();
        }
        return path;
    }

    private static Path createEmptyJar(Path path) throws IOException {
        try (OutputStream output = Files.newOutputStream(path);
             JarOutputStream ignored = new JarOutputStream(output)) {
            return path;
        }
    }

    private static final class Fixture {
        private final RuntimeDiscovery discovery;
        private final Path home;
        private final Path javaHome;

        private Fixture(RuntimeDiscovery discovery, Path home, Path javaHome) {
            this.discovery = discovery;
            this.home = home;
            this.javaHome = javaHome;
        }
    }
}
