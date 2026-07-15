package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.RuntimeAdapter;
import com.csforge.sstable.worker.api.WorkerMain;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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
