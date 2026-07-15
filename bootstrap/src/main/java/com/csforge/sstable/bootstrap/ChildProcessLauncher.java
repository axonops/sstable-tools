package com.csforge.sstable.bootstrap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Starts the Cassandra-linked worker in a separate JVM and propagates its status. */
public final class ChildProcessLauncher {
    static final String WORKER_MAIN = "com.csforge.sstable.worker.api.WorkerMain";
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
