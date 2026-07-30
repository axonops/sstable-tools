package com.axonops.sstable.bootstrap;

import com.axonops.sstable.worker.api.WorkerEndpoint;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CqlshLauncherTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void pinsCredentialAndLoopbackEndpointWithoutPasswordArgument() throws Exception {
        Path root = temporary.newFolder("cqlsh-launcher").toPath();
        Path home = Files.createDirectories(root.resolve("cassandra/bin"));
        Path executable = Files.write(home.resolve("cqlsh"),
                "#!/bin/sh\n".getBytes(StandardCharsets.US_ASCII));
        executable.toFile().setExecutable(true, true);
        Path conf = Files.createDirectory(root.resolve("conf"));
        Path server = Files.write(root.resolve("cassandra-all-3.11.19.jar"), new byte[0]);
        Path tool = Files.write(root.resolve("tool.jar"), new byte[0]);
        Path cqlshrc = Files.write(root.resolve("cqlshrc"), Arrays.asList(
                "[authentication]", "username = sstable_workspace",
                "password = secret-not-on-command-line"), StandardCharsets.US_ASCII);
        try {
            Files.setPosixFilePermissions(cqlshrc,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The test still exercises the non-POSIX path checks.
        }
        JavaInstallation java = JavaInstallation.discover(
                Paths.get(System.getProperty("java.home")),
                System.getenv(), System.getProperties());
        CassandraInstallation installation = new CassandraInstallation(
                root.resolve("cassandra"), conf, server,
                CassandraVersion.parse("3.11.19"), java, tool,
                Arrays.asList(tool, conf, server));
        Instant now = Instant.parse("2026-07-17T08:00:00Z");
        WorkerEndpoint endpoint = new WorkerEndpoint(3,
                UUID.fromString("20a0d99c-f07a-4ef3-8999-e063aad5c183"),
                WorkerEndpoint.Status.RUNNING, 1234, "3.11.19", "127.0.0.1", 19042,
                "127.0.0.1", 19043, now, now, "");

        List<String> command = new CqlshLauncher().command(installation, endpoint,
                cqlshrc, "SELECT * FROM blog.users");

        Assert.assertEquals(executable.toString(), command.get(0));
        Assert.assertEquals(Arrays.asList("--cqlshrc", cqlshrc.toString(),
                "127.0.0.1", "19042", "-e", "SELECT * FROM blog.users"),
                command.subList(1, command.size()));
        Assert.assertFalse(command.toString().contains("secret-not-on-command-line"));
    }

    @Test
    public void usesUsrBinCqlshForDebianAndRpmInstallationLayout() throws Exception {
        Path root = temporary.newFolder("packaged-cqlsh-launcher").toPath();
        Path home = Files.createDirectories(root.resolve("usr/share/cassandra"));
        Path bin = Files.createDirectories(root.resolve("usr/bin"));
        Path executable = Files.write(bin.resolve("cqlsh"),
                "#!/bin/sh\n".getBytes(StandardCharsets.US_ASCII));
        executable.toFile().setExecutable(true, true);
        Path conf = Files.createDirectories(root.resolve("etc/cassandra"));
        Path server = Files.write(home.resolve("apache-cassandra-4.1.11.jar"), new byte[0]);
        Path tool = Files.write(root.resolve("tool.jar"), new byte[0]);
        Path cqlshrc = ownerOnlyCredential(root.resolve("cqlshrc"));
        CassandraInstallation installation = installation(home, conf, server, tool, "4.1.11");

        List<String> command = new CqlshLauncher().command(installation, runningEndpoint(),
                cqlshrc, null);

        Assert.assertEquals(executable.toString(), command.get(0));
    }

    private static Path ownerOnlyCredential(Path path) throws Exception {
        Path credential = Files.write(path, Arrays.asList(
                "[authentication]", "username = sstable_workspace",
                "password = secret-not-on-command-line"), StandardCharsets.US_ASCII);
        try {
            Files.setPosixFilePermissions(credential,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The test still exercises the non-POSIX path checks.
        }
        return credential;
    }

    private static CassandraInstallation installation(Path home,
                                                       Path conf,
                                                       Path server,
                                                       Path tool,
                                                       String version) throws Exception {
        JavaInstallation java = JavaInstallation.discover(
                Paths.get(System.getProperty("java.home")),
                System.getenv(), System.getProperties());
        return new CassandraInstallation(home, conf, server,
                CassandraVersion.parse(version), java, tool,
                Arrays.asList(tool, conf, server));
    }

    private static WorkerEndpoint runningEndpoint() {
        Instant now = Instant.parse("2026-07-17T08:00:00Z");
        return new WorkerEndpoint(3,
                UUID.fromString("20a0d99c-f07a-4ef3-8999-e063aad5c183"),
                WorkerEndpoint.Status.RUNNING, 1234, "4.1.11", "127.0.0.1", 19042,
                "127.0.0.1", 19043, now, now, "");
    }
}
