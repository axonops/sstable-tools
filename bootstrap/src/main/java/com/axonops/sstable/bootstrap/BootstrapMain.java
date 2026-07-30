package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.WorkspaceException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/** Cassandra-free entry point shared by every release-specific artifact. */
public final class BootstrapMain {
    private BootstrapMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            BootstrapArguments arguments = BootstrapArguments.parse(args);
            if (arguments.action() == BootstrapArguments.Action.HELP) {
                printHelp(out);
                return 0;
            }
            if (arguments.action() == BootstrapArguments.Action.VERSION) {
                Properties adapter = loadAdapterProperties();
                out.printf("sstable-tools %s (Cassandra %s adapter, compiled against %s)%n",
                        implementationVersion(),
                        adapter.getProperty("adapter.release-line", "unbound"),
                        adapter.getProperty("adapter.cassandra-version", "unbound"));
                return 0;
            }
            if (requiresSstableWriteWarning(arguments.action())) {
                printSstableWriteWarning(err);
            }
            WorkspaceCommandRunner workspaceRunner = new WorkspaceCommandRunner(err);
            if (isCassandraFreeWorkspaceAction(arguments.action())) {
                workspaceRunner.run(arguments, out);
                return 0;
            }

            AdapterMetadata adapter = AdapterMetadata.loadRequired(
                    BootstrapMain.class.getClassLoader());
            RuntimeDiscovery discovery = new RuntimeDiscovery(
                    toolPath(), System.getenv(), System.getProperties());
            CassandraInstallation installation = discovery.discover(
                    arguments.runtimeOptions(), adapter);

            if (arguments.action() == BootstrapArguments.Action.WORKSPACE_START) {
                workspaceRunner.start(arguments, adapter, installation, out);
                return 0;
            }
            if (arguments.action() == BootstrapArguments.Action.DIRECT_CQLSH) {
                return workspaceRunner.directCqlsh(arguments, adapter, installation, out);
            }
            if (arguments.action() == BootstrapArguments.Action.WORKSPACE_CQLSH) {
                return workspaceRunner.cqlsh(arguments, installation);
            }
            if (arguments.action() == BootstrapArguments.Action.WORKSPACE_IMPORT) {
                workspaceRunner.importSstables(arguments, adapter, installation, out);
                return 0;
            }

            if (arguments.action() == BootstrapArguments.Action.RUNTIME_INSPECT) {
                RuntimeIdentity identity = RuntimeIdentity.capture(installation);
                for (String line : identity.asPropertyLines(adapter)) {
                    out.println(line);
                }
                return 0;
            }

            int childExitCode = new ChildProcessLauncher().runPreflight(installation);
            if (childExitCode != 0) {
                err.println("error: Cassandra worker preflight exited with code " + childExitCode);
            }
            return childExitCode;
        } catch (BootstrapException e) {
            err.println("error: " + e.getMessage());
            return e.exitCode();
        } catch (WorkspaceException e) {
            err.println("error: " + e.getMessage());
            return BootstrapException.WORKSPACE_EXIT_CODE;
        }
    }

    private static boolean isCassandraFreeWorkspaceAction(BootstrapArguments.Action action) {
        return action == BootstrapArguments.Action.WORKSPACE_CREATE
                || action == BootstrapArguments.Action.WORKSPACE_STATUS
                || action == BootstrapArguments.Action.WORKSPACE_FLUSH
                || action == BootstrapArguments.Action.WORKSPACE_EXPORT
                || action == BootstrapArguments.Action.WORKSPACE_STOP
                || action == BootstrapArguments.Action.WORKSPACE_RECOVER
                || action == BootstrapArguments.Action.WORKSPACE_DESTROY;
    }

    private static boolean requiresSstableWriteWarning(BootstrapArguments.Action action) {
        return action == BootstrapArguments.Action.WORKSPACE_CREATE
                || action == BootstrapArguments.Action.DIRECT_CQLSH
                || action == BootstrapArguments.Action.WORKSPACE_IMPORT
                || action == BootstrapArguments.Action.WORKSPACE_START
                || action == BootstrapArguments.Action.WORKSPACE_CQLSH
                || action == BootstrapArguments.Action.WORKSPACE_FLUSH
                || action == BootstrapArguments.Action.WORKSPACE_EXPORT;
    }

    private static void printSstableWriteWarning(PrintStream err) {
        err.println("WARNING: DANGEROUS SSTABLE WRITE WORKFLOW");
        err.println("Existing SSTable components are immutable and may be read while Cassandra");
        err.println("has them open. Publishing new SSTables into a running node's live table");
        err.println("directory is dangerous and requires explicit confirmation.");
        err.println("The isolated workspace worker started by this tool is expected and writes");
        err.println("only beneath a private workspace before verified output is published.");
    }

    private static String implementationVersion() {
        String version = BootstrapMain.class.getPackage().getImplementationVersion();
        return version == null ? "development" : version;
    }

    private static Properties loadAdapterProperties() {
        Properties properties = new Properties();
        try (InputStream input = BootstrapMain.class.getClassLoader()
                .getResourceAsStream(AdapterMetadata.RESOURCE_NAME)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + AdapterMetadata.RESOURCE_NAME, e);
        }
        return properties;
    }

    private static Path toolPath() throws BootstrapException {
        try {
            return Paths.get(BootstrapMain.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
        } catch (URISyntaxException | RuntimeException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot determine the tool artifact location", e);
        }
    }

    private static void printHelp(PrintStream out) {
        out.println("Usage: java -jar sstable-tools-cassandra-<version>.jar [options] <command>");
        out.println();
        out.println("Options:");
        out.println("  --cassandra-home <path>  Cassandra installation to use");
        out.println("  --java-home <path>       Compatible Java installation to use");
        out.println("  --sstables <path>        Selected SSTable Data.db or TOC.txt (repeatable)");
        out.println("  --output-dir <path>      Existing table directory for direct CQL output");
        out.println("  --schema <path>          UTF-8 CQL schema bundle for selected SSTables");
        out.println("  --timestamp-policy <p>  wall-clock or after-source (start/direct cqlsh)");
        out.println("  --mode <mode>           delta or snapshot (workspace export)");
        out.println("  --output <path>         Atomic publication destination (workspace export)");
        out.println("  --output-format <type>  big (default), or bti for Cassandra 5.0 output");
        out.println("  --tmp-dir <path>        Parent for private direct-cqlsh workspaces");
        out.println("  --execute <cql>         Execute CQL and exit (direct or workspace cqlsh)");
        out.println("  --allow-live-cassandra-output  Allow publication into a live table directory");
        out.println("  --confirm-workspace-id <uuid> Exact UUID required by workspace destroy");
        out.println("  --version                Print tool and adapter versions");
        out.println("  --help                   Print this help");
        out.println();
        out.println("Runtime commands:");
        out.println("  cqlsh                    Query SSTables or publish writes into --output-dir");
        out.println("  runtime inspect          Print resolved runtime paths, versions, and hashes");
        out.println("  runtime preflight        Run the release worker linkage self-test");
        out.println();
        out.println("Workspace commands:");
        out.println("  workspace create <path>  Inventory sources and create a validated workspace");
        out.println("  workspace import <path>  Validate and copy SSTables with native CQL disabled");
        out.println("  workspace start <path>   Start an imported workspace sandbox");
        out.println("  workspace cqlsh <path>   Launch the selected installation's stock cqlsh");
        out.println("  workspace status <path>  Verify sources and print persisted lifecycle state");
        out.println("  workspace flush <path>   Quiesce CQL and flush the workspace table");
        out.println("  workspace export <path>  Verify and publish a delta or snapshot export");
        out.println("  workspace stop <path>    Drain and stop a running workspace sandbox");
        out.println("  workspace recover <path> Recover a failed workspace to its last stable state");
        out.println("  workspace destroy <path> Permanently delete a stopped/inactive workspace");
        out.println();
        out.println("Safety warning:");
        out.println("  Existing SSTables are immutable and safe to read while Cassandra has them");
        out.println("  open. Publication into a running node's live table directory prompts for");
        out.println("  confirmation, or requires --allow-live-cassandra-output without a terminal.");
    }
}
