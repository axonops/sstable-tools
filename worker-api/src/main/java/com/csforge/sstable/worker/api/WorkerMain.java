package com.csforge.sstable.worker.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/** Child-process entry point that is allowed to link installed Cassandra classes. */
public final class WorkerMain {
    private static final String ADAPTER_RESOURCE = "sstable-tools-adapter.properties";

    private WorkerMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length != 3 || !"--self-test".equals(args[0])
                || !"--expected-version".equals(args[1])) {
            err.println("error: expected --self-test --expected-version <version>");
            return 2;
        }

        try {
            Properties metadata = loadMetadata();
            String adapterClassName = required(metadata, "adapter.runtime-class");
            Object instance = Class.forName(adapterClassName, true,
                            WorkerMain.class.getClassLoader())
                    .getDeclaredConstructor()
                    .newInstance();
            if (!(instance instanceof RuntimeAdapter)) {
                throw new IllegalStateException(adapterClassName
                        + " does not implement " + RuntimeAdapter.class.getName());
            }

            RuntimeAdapter adapter = (RuntimeAdapter) instance;
            String installedVersion = adapter.installedVersion();
            if (!args[2].equals(installedVersion)) {
                err.println("error: discovered Cassandra " + args[2]
                        + " but the worker loaded " + installedVersion
                        + "; check the installation classpath for duplicate versions");
                return 4;
            }
            adapter.verifyLinkage();
            out.println("WORKER_READY protocol=" + WorkerProtocol.CURRENT_VERSION
                    + " release=" + installedVersion);
            return 0;
        } catch (IOException | ReflectiveOperationException | LinkageError
                 | IllegalStateException e) {
            Throwable cause = e instanceof InvocationTargetException
                    && ((InvocationTargetException) e).getCause() != null
                    ? ((InvocationTargetException) e).getCause() : e;
            err.println("error: Cassandra worker linkage preflight failed: "
                    + cause.getClass().getSimpleName() + ": " + cause.getMessage());
            return 4;
        }
    }

    private static Properties loadMetadata() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = WorkerMain.class.getClassLoader()
                .getResourceAsStream(ADAPTER_RESOURCE)) {
            if (input == null) {
                throw new IOException("Missing " + ADAPTER_RESOURCE);
            }
            properties.load(input);
        }
        return properties;
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required adapter property " + name);
        }
        return value.trim();
    }
}
