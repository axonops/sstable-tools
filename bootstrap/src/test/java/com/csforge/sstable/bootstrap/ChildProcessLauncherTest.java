package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.RuntimeAdapter;
import com.csforge.sstable.worker.api.WorkerMain;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ChildProcessLauncherTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void startsWorkerInSeparateJvmAndPropagatesSuccess() throws Exception {
        Path root = temporary.newFolder("child path with spaces").toPath();
        Path home = Files.createDirectory(root.resolve("cassandra home"));
        Path conf = Files.createDirectory(root.resolve("cassandra conf"));
        Files.write(conf.resolve("cassandra.yaml"), new byte[0]);
        Path serverJar = Files.write(root.resolve("cassandra-all-9.9.9.jar"), new byte[0]);
        JavaInstallation java = JavaInstallation.discover(
                Paths.get(System.getProperty("java.home")),
                System.getenv(), System.getProperties());
        Path testClasses = locationOf(ChildProcessLauncherTest.class);
        Path workerClasses = locationOf(WorkerMain.class);
        CassandraInstallation installation = new CassandraInstallation(
                home, conf, serverJar, CassandraVersion.parse("9.9.9"), java,
                testClasses, Arrays.asList(testClasses, workerClasses));

        int exitCode = new ChildProcessLauncher(false).runPreflight(installation);

        Assert.assertEquals(0, exitCode);
    }

    @Test
    public void sandboxCommandPinsPrivateConfigurationAndDisablesInternodeServices()
            throws Exception {
        Path root = temporary.newFolder("sandbox command").toPath();
        Path home = Files.createDirectory(root.resolve("cassandra home"));
        Path conf = Files.createDirectory(root.resolve("cassandra conf"));
        Files.write(conf.resolve("cassandra.yaml"), new byte[0]);
        Path serverJar = Files.write(root.resolve("cassandra-all-3.11.19.jar"), new byte[0]);
        Path jamm = Files.write(root.resolve("jamm-0.3.2.jar"), new byte[0]);
        Path tool = Files.write(root.resolve("sstable-tools.jar"), new byte[0]);
        Path workspace = Files.createDirectory(root.resolve("workspace"));
        JavaInstallation java = JavaInstallation.discover(
                Paths.get(System.getProperty("java.home")),
                System.getenv(), System.getProperties());
        CassandraInstallation installation = new CassandraInstallation(
                home, conf, serverJar, CassandraVersion.parse("3.11.19"), java,
                tool, Arrays.asList(tool, conf, serverJar, jamm));

        List<String> command = new ChildProcessLauncher(false).sandboxCommand(
                installation, workspace,
                UUID.fromString("20a0d99c-f07a-4ef3-8999-e063aad5c183"), 19042,
                "blog", "users");

        Assert.assertTrue(command.contains("-Dcassandra.start_gossip=false"));
        Assert.assertTrue(command.contains("-Dcassandra.join_ring=false"));
        Assert.assertTrue(command.contains("-Dcassandra.load_ring_state=false"));
        Assert.assertTrue(command.contains("-Dcassandra.start_rpc=false"));
        Assert.assertTrue(command.contains("-Dcassandra.start_native_transport=true"));
        Assert.assertTrue(command.contains("-XX:+DisableAttachMechanism"));
        Assert.assertTrue(command.contains("-Dcassandra.custom_query_handler_class="
                + "com.csforge.sstable.worker.cassandra311.WorkspaceQueryHandler"));
        Assert.assertTrue(command.contains("-Dsstable.tools.workspace.keyspace=blog"));
        Assert.assertTrue(command.contains("-Dsstable.tools.workspace.table=users"));
        Assert.assertTrue(command.contains("-javaagent:" + jamm));
        Assert.assertTrue(command.contains("--native-port"));
        Assert.assertTrue(command.contains("19042"));
        for (String argument : command) {
            Assert.assertFalse(argument.contains("jmxremote"));
        }
    }

    @Test
    public void importCommandDisablesNativeAndAllInternodeServices() throws Exception {
        Path root = temporary.newFolder("import command").toPath();
        Path home = Files.createDirectory(root.resolve("cassandra home"));
        Path conf = Files.createDirectory(root.resolve("cassandra conf"));
        Files.write(conf.resolve("cassandra.yaml"), new byte[0]);
        Path serverJar = Files.write(root.resolve("cassandra-all-3.11.19.jar"), new byte[0]);
        Path jamm = Files.write(root.resolve("jamm-0.3.2.jar"), new byte[0]);
        Path tool = Files.write(root.resolve("sstable-tools.jar"), new byte[0]);
        Path workspace = Files.createDirectory(root.resolve("workspace"));
        JavaInstallation java = JavaInstallation.discover(
                Paths.get(System.getProperty("java.home")),
                System.getenv(), System.getProperties());
        CassandraInstallation installation = new CassandraInstallation(
                home, conf, serverJar, CassandraVersion.parse("3.11.19"), java,
                tool, Arrays.asList(tool, conf, serverJar, jamm));

        List<String> command = new ChildProcessLauncher(false).importCommand(
                installation, workspace,
                UUID.fromString("20a0d99c-f07a-4ef3-8999-e063aad5c183"));

        Assert.assertTrue(command.contains("--import"));
        Assert.assertTrue(command.contains("-Dcassandra.start_native_transport=false"));
        Assert.assertTrue(command.contains("-Dcassandra.start_gossip=false"));
        Assert.assertTrue(command.contains("-Dcassandra.join_ring=false"));
        Assert.assertTrue(command.contains("-Dcassandra.load_ring_state=false"));
        Assert.assertTrue(command.contains("-Dcassandra.start_rpc=false"));
        Assert.assertTrue(command.contains("-XX:+DisableAttachMechanism"));
    }

    private static Path locationOf(Class<?> type) throws Exception {
        return Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toRealPath();
    }

    public static final class FakeRuntimeAdapter implements RuntimeAdapter {
        @Override
        public String installedVersion() {
            return "9.9.9";
        }

        @Override
        public void verifyLinkage() {
        }
    }
}
