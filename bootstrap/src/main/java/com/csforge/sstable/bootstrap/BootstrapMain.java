package com.csforge.sstable.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

/** Cassandra-free entry point shared by every release-specific artifact. */
public final class BootstrapMain {
    private static final String ADAPTER_RESOURCE = "sstable-tools-adapter.properties";

    private BootstrapMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            printHelp(out);
            return 0;
        }

        if ("--version".equals(args[0])) {
            Properties adapter = loadAdapterMetadata();
            out.printf("sstable-tools %s (Cassandra %s adapter, compiled against %s)%n",
                    implementationVersion(),
                    adapter.getProperty("adapter.release-line", "unbound"),
                    adapter.getProperty("adapter.cassandra-version", "unbound"));
            return 0;
        }

        err.println("Workspace commands are not implemented yet. See --help.");
        return 2;
    }

    private static String implementationVersion() {
        String version = BootstrapMain.class.getPackage().getImplementationVersion();
        return version == null ? "development" : version;
    }

    private static Properties loadAdapterMetadata() {
        Properties properties = new Properties();
        try (InputStream input = BootstrapMain.class.getClassLoader()
                .getResourceAsStream(ADAPTER_RESOURCE)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + ADAPTER_RESOURCE, e);
        }
        return properties;
    }

    private static void printHelp(PrintStream out) {
        out.println("Usage: java -jar sstable-tools-cassandra-<version>.jar [options] <command>");
        out.println();
        out.println("Options:");
        out.println("  --cassandra-home <path>  Cassandra installation to use");
        out.println("  --java-home <path>       Compatible Java installation to use");
        out.println("  --version                Print tool and adapter versions");
        out.println("  --help                   Print this help");
        out.println();
        out.println("Commands planned for the workspace runtime:");
        out.println("  workspace create|start|status|cqlsh|export|stop|destroy|recover");
    }
}
