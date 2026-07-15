package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.WorkerEndpoint;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Starts the Cassandra-linked worker in a separate JVM and propagates its status. */
public final class ChildProcessLauncher {
    static final String WORKER_MAIN = "com.csforge.sstable.worker.api.WorkerMain";
    private static final long SANDBOX_START_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(2);
    private final boolean inheritIo;

    public ChildProcessLauncher() {
        this(true);
    }

    ChildProcessLauncher(boolean inheritIo) {
        this.inheritIo = inheritIo;
    }

    public int runPreflight(CassandraInstallation installation) throws BootstrapException {
        ProcessBuilder builder = new ProcessBuilder(preflightCommand(installation));
        builder.environment().put("CASSANDRA_HOME", installation.home().toString());
        builder.environment().put("CASSANDRA_CONF", installation.conf().toString());
        builder.environment().put("JAVA_HOME", installation.java().home().toString());
        if (inheritIo) {
            builder.inheritIO();
        }

        final Process child;
        try {
            child = builder.start();
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Cannot start Cassandra worker child", e);
        }

        Thread shutdownHook = new Thread(() -> stopChild(child), "sstable-tools-worker-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            return child.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopChild(child);
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Interrupted while waiting for Cassandra worker child", e);
        } finally {
            if (!inheritIo) {
                closeQuietly(child.getInputStream());
                closeQuietly(child.getErrorStream());
            }
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM shutdown is already in progress and owns hook execution.
            }
        }
    }

    public WorkerEndpoint startSandbox(CassandraInstallation installation,
                                       Path workspace,
                                       UUID workspaceId,
                                       int nativePort) throws BootstrapException {
        List<String> command = sandboxCommand(installation, workspace, workspaceId, nativePort);
        ProcessBuilder builder = new ProcessBuilder(command);
        Path logs = prepareOwnedDirectory(workspace, "logs");
        Path runtime = prepareOwnedDirectory(workspace, "runtime");
        prepareOwnedDirectory(workspace, "runtime/tmp");
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(
                logs.resolve("worker.out").toFile()));
        builder.redirectError(ProcessBuilder.Redirect.appendTo(
                logs.resolve("worker.err").toFile()));
        builder.environment().put("CASSANDRA_HOME", installation.home().toString());
        builder.environment().put("CASSANDRA_CONF", runtime.toString());
        builder.environment().put("JAVA_HOME", installation.java().home().toString());
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        builder.environment().remove("_JAVA_OPTIONS");

        final Process child;
        try {
            child = builder.start();
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Cannot start Cassandra sandbox worker", e);
        }

        Path endpointPath = workspace.resolve(Cassandra311SandboxConfig.ENDPOINT_PATH);
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(SANDBOX_START_TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(endpointPath)) {
                try {
                    WorkerEndpoint endpoint = WorkerEndpoint.read(endpointPath);
                    if (workspaceId.equals(endpoint.workspaceId())
                            && endpoint.status() == WorkerEndpoint.Status.RUNNING) {
                        return endpoint;
                    }
                    if (workspaceId.equals(endpoint.workspaceId())
                            && endpoint.status() == WorkerEndpoint.Status.FAILED) {
                        stopChild(child);
                        throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                                "Cassandra sandbox failed: " + endpoint.message());
                    }
                } catch (IOException ignored) {
                    // Atomic publication may not have completed yet.
                }
            }
            if (!child.isAlive()) {
                throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                        "Cassandra sandbox worker exited before publishing readiness; inspect "
                                + logs.resolve("worker.err"));
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopChild(child);
                throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                        "Interrupted while waiting for Cassandra sandbox readiness", e);
            }
        }
        stopChild(child);
        throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                "Timed out waiting for Cassandra sandbox readiness; inspect "
                        + logs.resolve("worker.err"));
    }

    private static void closeQuietly(InputStream input) {
        try {
            input.close();
        } catch (IOException ignored) {
            // Test-only captured streams do not affect the child exit status.
        }
    }

    List<String> preflightCommand(CassandraInstallation installation) {
        List<String> command = new ArrayList<>();
        command.add(installation.java().executable().toString());
        command.add("-cp");
        command.add(joinClasspath(installation));
        command.add(WORKER_MAIN);
        command.add("--self-test");
        command.add("--expected-version");
        command.add(installation.version().toString());
        return Collections.unmodifiableList(command);
    }

    List<String> sandboxCommand(CassandraInstallation installation,
                                Path workspace,
                                UUID workspaceId,
                                int nativePort) throws BootstrapException {
        Path configuration = workspace.resolve(Cassandra311SandboxConfig.CONFIG_PATH);
        List<String> command = new ArrayList<>();
        command.add(installation.java().executable().toString());
        command.add("-Xms512m");
        command.add("-Xmx512m");
        command.add("-javaagent:" + findJamm(installation));
        command.add("-Dcassandra.config=" + configuration.toUri());
        command.add("-Dcassandra.storagedir=" + workspace);
        command.add("-Dcassandra.logdir=" + workspace.resolve("logs"));
        command.add("-Dcassandra-foreground=true");
        command.add("-Dcassandra.start_gossip=false");
        command.add("-Dcassandra.join_ring=false");
        command.add("-Dcassandra.load_ring_state=false");
        command.add("-Dcassandra.start_rpc=false");
        command.add("-Dcassandra.start_native_transport=true");
        command.add("-Dcassandra.size_recorder_interval=0");
        command.add("-Djava.io.tmpdir=" + workspace.resolve("runtime/tmp"));
        command.add("-cp");
        command.add(joinClasspath(installation));
        command.add(WORKER_MAIN);
        command.add("--sandbox");
        command.add("--expected-version");
        command.add(installation.version().toString());
        command.add("--workspace");
        command.add(workspace.toString());
        command.add("--workspace-id");
        command.add(workspaceId.toString());
        command.add("--native-port");
        command.add(Integer.toString(nativePort));
        return Collections.unmodifiableList(command);
    }

    private static String joinClasspath(CassandraInstallation installation) {
        StringBuilder classpath = new StringBuilder();
        for (java.nio.file.Path entry : installation.classpath()) {
            if (classpath.length() > 0) {
                classpath.append(File.pathSeparatorChar);
            }
            classpath.append(entry);
        }
        return classpath.toString();
    }

    private static Path findJamm(CassandraInstallation installation)
            throws BootstrapException {
        for (Path entry : installation.classpath()) {
            String name = entry.getFileName().toString();
            if (Files.isRegularFile(entry) && name.startsWith("jamm-")
                    && name.endsWith(".jar")) {
                return entry;
            }
        }
        throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                "Cassandra installation is missing the required jamm Java agent");
    }

    private static Path prepareOwnedDirectory(Path workspace, String relative)
            throws BootstrapException {
        Path candidate = workspace.resolve(relative).normalize();
        if (!candidate.startsWith(workspace) || candidate.equals(workspace)) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Sandbox directory escapes workspace: " + relative);
        }
        Path current = workspace;
        try {
            for (Path segment : workspace.relativize(candidate)) {
                current = current.resolve(segment);
                if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                    if (Files.isSymbolicLink(current)
                            || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                        throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                                "Sandbox directory is unsafe: " + current);
                    }
                } else {
                    Files.createDirectory(current);
                }
                restrictDirectory(current);
            }
            Path real = candidate.toRealPath();
            if (!real.startsWith(workspace)) {
                throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                        "Sandbox directory resolves outside workspace: " + candidate);
            }
            return real;
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Cannot prepare sandbox directory " + candidate, e);
        }
    }

    private static void restrictDirectory(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path,
                    PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException ignored) {
            // The workspace ACL remains the boundary on non-POSIX filesystems.
        }
    }

    private static void stopChild(Process child) {
        if (!child.isAlive()) {
            return;
        }
        child.destroy();
        try {
            if (!child.waitFor(5, TimeUnit.SECONDS)) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            child.destroyForcibly();
        }
    }
}
