package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.WorkerMain;
import com.csforge.sstable.workspace.WorkspaceManifest;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ChildProcessLauncherSignalTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void sigtermStopsPreflightChild() throws Exception {
        Assume.assumeTrue("Signal test requires Linux /proc",
                Files.isDirectory(Paths.get("/proc")));
        Assume.assumeTrue("Signal test requires /bin/sh",
                Files.isExecutable(Paths.get("/bin/sh")));
        Path root = temporary.newFolder("signal-boundary").toPath();
        Path fakeJavaHome = createFakeJavaHome(root);
        Path childPidFile = root.resolve("child.pid");
        Path terminatedFile = root.resolve("child.terminated");
        Path controllerLog = root.resolve("controller.log");

        ProcessBuilder builder = new ProcessBuilder(
                javaExecutable().toString(), "-cp", fixtureClasspath(),
                SignalFixtureMain.class.getName(), root.toString(),
                fakeJavaHome.toString());
        builder.environment().put("SSTABLE_TOOLS_SIGNAL_ROOT", root.toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(controllerLog.toFile());
        Process controller = builder.start();
        long childPid = -1L;
        try {
            waitForFile(childPidFile, controller, controllerLog);
            childPid = Long.parseLong(new String(Files.readAllBytes(childPidFile),
                    StandardCharsets.US_ASCII).trim());
            Assert.assertTrue("Fake worker PID was not running: " + childPid,
                    Files.isDirectory(Paths.get("/proc", Long.toString(childPid))));

            controller.destroy();
            Assert.assertTrue("Controller ignored SIGTERM:\n" + read(controllerLog),
                    controller.waitFor(10, TimeUnit.SECONDS));
            waitForProcessStop(childPid, controllerLog);
            Assert.assertTrue("Child SIGTERM trap did not run:\n" + read(controllerLog),
                    Files.isRegularFile(terminatedFile));
        } finally {
            if (controller.isAlive()) {
                controller.destroyForcibly();
                controller.waitFor(5, TimeUnit.SECONDS);
            }
            if (childPid > 0
                    && Files.exists(Paths.get("/proc", Long.toString(childPid)))) {
                new ProcessBuilder("/bin/kill", "-KILL", Long.toString(childPid))
                        .start().waitFor();
            }
        }
    }

    private static Path createFakeJavaHome(Path root) throws Exception {
        Path home = Files.createDirectories(root.resolve("fake-java"));
        Path bin = Files.createDirectories(home.resolve("bin"));
        Path executable = Files.write(bin.resolve("java"), Arrays.asList(
                "#!/bin/sh",
                "echo $$ > \"$SSTABLE_TOOLS_SIGNAL_ROOT/child.pid\"",
                "trap 'echo terminated > \"$SSTABLE_TOOLS_SIGNAL_ROOT/"
                        + "child.terminated\"; exit 0' TERM INT",
                "while :; do :; done"), StandardCharsets.UTF_8);
        if (!executable.toFile().setExecutable(true)) {
            throw new IllegalStateException("Cannot make fake Java executable");
        }
        Files.write(home.resolve("release"),
                Collections.singletonList("JAVA_VERSION=\"1.8.0_402\""),
                StandardCharsets.ISO_8859_1);
        return home;
    }

    private static Path javaExecutable() {
        return Paths.get(System.getProperty("java.home"), "bin",
                isWindows() ? "java.exe" : "java");
    }

    private static String fixtureClasspath() throws Exception {
        return joinClasspath(locationOf(ChildProcessLauncherSignalTest.class),
                locationOf(ChildProcessLauncher.class), locationOf(WorkerMain.class),
                locationOf(WorkspaceManifest.class));
    }

    private static String joinClasspath(Path... entries) {
        StringBuilder value = new StringBuilder();
        for (Path entry : entries) {
            if (value.length() > 0) {
                value.append(File.pathSeparatorChar);
            }
            value.append(entry);
        }
        return value.toString();
    }

    private static Path locationOf(Class<?> type) throws Exception {
        return Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toRealPath();
    }

    private static void waitForFile(Path path, Process controller, Path log)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!Files.isRegularFile(path) && controller.isAlive()
                && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        Assert.assertTrue("Controller did not start fake worker:\n" + read(log),
                Files.isRegularFile(path));
    }

    private static void waitForProcessStop(long pid, Path log) throws Exception {
        Path process = Paths.get("/proc", Long.toString(pid));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (isRunning(process) && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        Assert.assertFalse("Child PID remained live after controller SIGTERM: " + pid + "\n"
                + read(log) + "\nchild stat=" + read(process.resolve("stat"))
                + "\nchild cmdline=" + read(process.resolve("cmdline")), isRunning(process));
    }

    private static boolean isRunning(Path process) throws Exception {
        if (!Files.exists(process)) {
            return false;
        }
        String stat = new String(Files.readAllBytes(process.resolve("stat")),
                StandardCharsets.US_ASCII);
        int commandEnd = stat.lastIndexOf(") ");
        return commandEnd < 0 || commandEnd + 2 >= stat.length()
                || stat.charAt(commandEnd + 2) != 'Z';
    }

    private static String read(Path path) throws Exception {
        return Files.isRegularFile(path)
                ? new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
                : "<no controller log>";
    }

    private static boolean isWindows() {
        return File.separatorChar == '\\';
    }

    public static final class SignalFixtureMain {
        private SignalFixtureMain() {
        }

        public static void main(String[] args) throws Exception {
            Path root = Paths.get(args[0]).toRealPath();
            Path javaHome = Paths.get(args[1]).toRealPath();
            Path home = Files.createDirectories(root.resolve("cassandra-home"));
            Path conf = Files.createDirectories(root.resolve("cassandra-conf"));
            Files.write(conf.resolve("cassandra.yaml"), new byte[0]);
            Path serverJar = Files.write(root.resolve("cassandra-all-9.9.9.jar"),
                    new byte[0]);
            Path tool = Files.write(root.resolve("sstable-tools.jar"), new byte[0]);
            JavaInstallation java = JavaInstallation.discover(javaHome,
                    System.getenv(), new Properties());
            CassandraInstallation installation = new CassandraInstallation(
                    home, conf, serverJar, CassandraVersion.parse("9.9.9"), java,
                    tool, Arrays.asList(tool, conf, serverJar));
            int exitCode = new ChildProcessLauncher(false).runPreflight(installation);
            if (exitCode != 0) {
                System.exit(exitCode);
            }
        }
    }
}
