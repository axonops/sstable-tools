package com.axonops.sstable.worker.api;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

/** Child-process entry point that is allowed to link installed Cassandra classes. */
public final class WorkerMain {
    private static final String ADAPTER_RESOURCE = "sstable-tools-adapter.properties";
    private static final int MAX_CONTROL_LINE_BYTES = 1024;

    private WorkerMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0 || isSandboxCommand(args) || isImportCommand(args)) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (isSelfTestCommand(args)) {
            return runSelfTest(args[2], out, err);
        }
        if (isSandboxCommand(args)) {
            return runSandbox(args, out, err);
        }
        if (isImportCommand(args)) {
            return runImport(args, out, err);
        }
        err.println("error: expected --self-test, --import, or --sandbox worker arguments");
        return 2;
    }

    private static int runImport(String[] args, PrintStream out, PrintStream err) {
        try {
            ImportArguments parsed = ImportArguments.parse(args);
            ImportRuntimeAdapter adapter = requireImportAdapter(loadAdapter());
            String installedVersion = adapter.installedVersion();
            if (!parsed.expectedVersion.equals(installedVersion)) {
                throw new IllegalStateException("Discovered Cassandra "
                        + parsed.expectedVersion + " but the worker loaded " + installedVersion);
            }
            adapter.verifyLinkage();
            Path workspace = parsed.workspace.toRealPath();
            if (!Files.isDirectory(workspace, LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(parsed.workspace)) {
                throw new IllegalStateException("Workspace is not a canonical directory: "
                        + parsed.workspace);
            }
            Path configuration = regularFileInside(workspace, "runtime/cassandra.yaml");
            regularFileInside(workspace, "schema/schema.cql");
            Path resultPath = pathInside(workspace, ImportResult.WORKSPACE_PATH);
            ImportResult result = adapter.importSstables(new ImportOptions(
                    workspace, configuration, parsed.workspaceId));
            if (!parsed.workspaceId.equals(result.workspaceId())
                    || !installedVersion.equals(result.release())) {
                throw new IllegalStateException("Import adapter returned mismatched identity");
            }
            result.writeAtomically(resultPath);
            out.println("IMPORT_COMPLETE protocol=" + WorkerProtocol.CURRENT_VERSION
                    + " release=" + installedVersion + " table=" + result.keyspace()
                    + "." + result.table() + " sstables=" + result.liveSstables()
                    + " rows=" + result.logicalRows());
            return 0;
        } catch (Exception | LinkageError e) {
            Throwable cause = e instanceof InvocationTargetException
                    && ((InvocationTargetException) e).getCause() != null
                    ? ((InvocationTargetException) e).getCause() : e;
            err.println("error: Cassandra import worker failed: "
                    + cause.getClass().getSimpleName() + ": " + cause.getMessage());
            cause.printStackTrace(err);
            return 4;
        }
    }

    private static int runSelfTest(String expectedVersion, PrintStream out, PrintStream err) {
        try {
            RuntimeAdapter adapter = loadAdapter();
            String installedVersion = adapter.installedVersion();
            if (!expectedVersion.equals(installedVersion)) {
                err.println("error: discovered Cassandra " + expectedVersion
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

    private static int runSandbox(String[] args, PrintStream out, PrintStream err) {
        WorkerEndpoint endpoint = null;
        Path endpointPath = null;
        SandboxHandle handle = null;
        try {
            SandboxArguments parsed = SandboxArguments.parse(args);
            SandboxRuntimeAdapter adapter = requireSandboxAdapter(loadAdapter());
            String installedVersion = adapter.installedVersion();
            if (!parsed.expectedVersion.equals(installedVersion)) {
                throw new IllegalStateException("Discovered Cassandra "
                        + parsed.expectedVersion + " but the worker loaded " + installedVersion);
            }
            adapter.verifyLinkage();

            Path workspace = parsed.workspace.toRealPath();
            if (!Files.isDirectory(workspace, LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(parsed.workspace)) {
                throw new IllegalStateException("Workspace is not a canonical directory: "
                        + parsed.workspace);
            }
            Path configuration = regularFileInside(workspace, "runtime/cassandra.yaml");
            Path tokenPath = regularFileInside(workspace, "state/control.token");
            endpointPath = pathInside(workspace, "state/worker.properties");
            String token = new String(Files.readAllBytes(tokenPath), StandardCharsets.US_ASCII)
                    .trim();
            if (!token.matches("[0-9a-f]{64}")) {
                throw new IllegalStateException("Invalid worker control token");
            }

            SandboxOptions options = new SandboxOptions(workspace, configuration,
                    parsed.workspaceId, parsed.nativePort);
            try (ServerSocket control = new ServerSocket()) {
                control.setReuseAddress(false);
                control.bind(new InetSocketAddress(
                        InetAddress.getByName("127.0.0.1"), 0), 16);
                handle = adapter.startSandbox(options);
                if (!handle.isRunning() || !"127.0.0.1".equals(handle.nativeAddress())
                        || handle.nativePort() != parsed.nativePort) {
                    throw new IllegalStateException("Sandbox adapter did not start the expected "
                            + "loopback native endpoint");
                }

                Instant startedAt = Instant.now();
                endpoint = new WorkerEndpoint(WorkerProtocol.CURRENT_VERSION,
                        parsed.workspaceId, WorkerEndpoint.Status.RUNNING, processId(),
                        installedVersion, handle.nativeAddress(), handle.nativePort(),
                        "127.0.0.1", control.getLocalPort(), startedAt, startedAt, "");
                endpoint.writeAtomically(endpointPath);
                out.println("SANDBOX_READY protocol=" + WorkerProtocol.CURRENT_VERSION
                        + " release=" + installedVersion + " native=127.0.0.1:"
                        + handle.nativePort());
                out.flush();

                endpoint = waitForStop(control, token, handle, endpoint, endpointPath);
            }

            handle.stop();
            handle = null;
            endpoint.withStatus(WorkerEndpoint.Status.STOPPED, "")
                    .writeAtomically(endpointPath);
            return 0;
        } catch (Exception | LinkageError e) {
            Throwable cause = e instanceof InvocationTargetException
                    && ((InvocationTargetException) e).getCause() != null
                    ? ((InvocationTargetException) e).getCause() : e;
            err.println("error: Cassandra sandbox worker failed: "
                    + cause.getClass().getSimpleName() + ": " + cause.getMessage());
            if (endpoint != null && endpointPath != null) {
                try {
                    endpoint.withStatus(WorkerEndpoint.Status.FAILED,
                            cause.getClass().getSimpleName() + ": " + cause.getMessage())
                            .writeAtomically(endpointPath);
                } catch (IOException ignored) {
                    // The original failure remains authoritative.
                }
            }
            if (handle != null) {
                try {
                    handle.stop();
                } catch (Exception ignored) {
                    // The original failure remains authoritative.
                }
            }
            return 4;
        }
    }

    private static WorkerEndpoint waitForStop(ServerSocket control,
                                              String token,
                                              SandboxHandle handle,
                                              WorkerEndpoint endpoint,
                                              Path endpointPath) throws Exception {
        WorkerEndpoint current = endpoint;
        while (true) {
            try (Socket client = control.accept()) {
                client.setSoTimeout(5000);
                String request = readLine(client.getInputStream());
                BufferedWriter response = new BufferedWriter(new OutputStreamWriter(
                        client.getOutputStream(), StandardCharsets.US_ASCII));
                if ((token + " STATUS").equals(request)) {
                    if (current.status() == WorkerEndpoint.Status.RUNNING) {
                        if (!handle.isRunning()) {
                            throw new IllegalStateException("Cassandra native transport stopped "
                                    + "unexpectedly");
                        }
                        writeLine(response, "OK RUNNING");
                    } else if (current.status() == WorkerEndpoint.Status.FLUSHED) {
                        if (!handle.isFlushed()) {
                            throw new IllegalStateException("Cassandra flush state changed "
                                    + "unexpectedly");
                        }
                        writeLine(response, "OK FLUSHED");
                    } else {
                        writeLine(response, "ERROR worker-not-ready");
                    }
                } else if ((token + " FLUSH").equals(request)
                        && current.status() == WorkerEndpoint.Status.RUNNING) {
                    current = current.withStatus(WorkerEndpoint.Status.FLUSHING,
                            "quiescing native transport and flushing workspace table");
                    current.writeAtomically(endpointPath);
                    handle.flush();
                    current = current.withStatus(WorkerEndpoint.Status.FLUSHED,
                            "workspace table flushed and inventoried");
                    current.writeAtomically(endpointPath);
                    writeLine(response, "OK FLUSHED");
                } else if ((token + " VERIFY").equals(request)
                        && current.status() == WorkerEndpoint.Status.FLUSHED) {
                    handle.verify();
                    if (!handle.isVerified()) {
                        throw new IllegalStateException("Sandbox verification did not publish "
                                + "durable evidence");
                    }
                    writeLine(response, "OK VERIFIED");
                } else if ((token + " STOP").equals(request)) {
                    current = current.withStatus(WorkerEndpoint.Status.STOPPING,
                            "graceful stop requested");
                    current.writeAtomically(endpointPath);
                    writeLine(response, "OK STOPPING");
                    return current;
                } else {
                    writeLine(response, "ERROR unauthorized-or-unsupported-command");
                }
            }
        }
    }

    private static String readLine(InputStream input) throws IOException {
        byte[] bytes = new byte[MAX_CONTROL_LINE_BYTES];
        int count = 0;
        while (count < bytes.length) {
            int value = input.read();
            if (value < 0 || value == '\n') {
                return new String(bytes, 0, count, StandardCharsets.US_ASCII)
                        .replace("\r", "");
            }
            bytes[count++] = (byte) value;
        }
        throw new IOException("Worker control request exceeds " + MAX_CONTROL_LINE_BYTES
                + " bytes");
    }

    private static void writeLine(BufferedWriter output, String line) throws IOException {
        output.write(line);
        output.newLine();
        output.flush();
    }

    private static RuntimeAdapter loadAdapter()
            throws IOException, ReflectiveOperationException {
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
        return (RuntimeAdapter) instance;
    }

    private static SandboxRuntimeAdapter requireSandboxAdapter(RuntimeAdapter adapter) {
        if (!(adapter instanceof SandboxRuntimeAdapter)) {
            throw new IllegalStateException(adapter.getClass().getName()
                    + " does not provide sandbox support");
        }
        return (SandboxRuntimeAdapter) adapter;
    }

    private static ImportRuntimeAdapter requireImportAdapter(RuntimeAdapter adapter) {
        if (!(adapter instanceof ImportRuntimeAdapter)) {
            throw new IllegalStateException(adapter.getClass().getName()
                    + " does not provide import support");
        }
        return (ImportRuntimeAdapter) adapter;
    }

    private static Path regularFileInside(Path workspace, String relative) throws IOException {
        Path path = pathInside(workspace, relative);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(path)) {
            throw new IOException("Required sandbox file is missing or unsafe: " + path);
        }
        return path.toRealPath();
    }

    private static Path pathInside(Path workspace, String relative) throws IOException {
        Path candidate = workspace.resolve(relative).normalize();
        if (!candidate.startsWith(workspace)) {
            throw new IOException("Sandbox path escapes workspace: " + relative);
        }
        Path current = workspace;
        for (Path segment : workspace.relativize(candidate)) {
            current = current.resolve(segment);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)
                    && Files.isSymbolicLink(current)) {
                throw new IOException("Sandbox path crosses a symlink: " + current);
            }
        }
        return candidate;
    }

    private static long processId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int separator = name.indexOf('@');
        try {
            return Long.parseLong(separator < 0 ? name : name.substring(0, separator));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static boolean isSelfTestCommand(String[] args) {
        return args.length == 3 && "--self-test".equals(args[0])
                && "--expected-version".equals(args[1]);
    }

    private static boolean isSandboxCommand(String[] args) {
        return args.length > 0 && "--sandbox".equals(args[0]);
    }

    private static boolean isImportCommand(String[] args) {
        return args.length > 0 && "--import".equals(args[0]);
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

    private static final class SandboxArguments {
        private final String expectedVersion;
        private final Path workspace;
        private final UUID workspaceId;
        private final int nativePort;

        private SandboxArguments(String expectedVersion,
                                 Path workspace,
                                 UUID workspaceId,
                                 int nativePort) {
            this.expectedVersion = expectedVersion;
            this.workspace = workspace;
            this.workspaceId = workspaceId;
            this.nativePort = nativePort;
        }

        private static SandboxArguments parse(String[] args) {
            if (args.length != 9 || !"--sandbox".equals(args[0])
                    || !"--expected-version".equals(args[1])
                    || !"--workspace".equals(args[3])
                    || !"--workspace-id".equals(args[5])
                    || !"--native-port".equals(args[7])) {
                throw new IllegalArgumentException("Expected --sandbox --expected-version "
                        + "<version> --workspace <path> --workspace-id <uuid> "
                        + "--native-port <port>");
            }
            int port = Integer.parseInt(args[8]);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Native port is outside 1..65535");
            }
            return new SandboxArguments(args[2], Paths.get(args[4]),
                    UUID.fromString(args[6]), port);
        }
    }

    private static final class ImportArguments {
        private final String expectedVersion;
        private final Path workspace;
        private final UUID workspaceId;

        private ImportArguments(String expectedVersion, Path workspace, UUID workspaceId) {
            this.expectedVersion = expectedVersion;
            this.workspace = workspace;
            this.workspaceId = workspaceId;
        }

        private static ImportArguments parse(String[] args) {
            if (args.length != 7 || !"--import".equals(args[0])
                    || !"--expected-version".equals(args[1])
                    || !"--workspace".equals(args[3])
                    || !"--workspace-id".equals(args[5])) {
                throw new IllegalArgumentException("Expected --import --expected-version "
                        + "<version> --workspace <path> --workspace-id <uuid>");
            }
            return new ImportArguments(args[2], Paths.get(args[4]),
                    UUID.fromString(args[6]));
        }
    }
}
