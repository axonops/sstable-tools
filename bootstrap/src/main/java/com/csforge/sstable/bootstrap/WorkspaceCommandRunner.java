package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.WorkerEndpoint;
import com.csforge.sstable.workspace.SourceInventory;
import com.csforge.sstable.workspace.SstableSet;
import com.csforge.sstable.workspace.WorkspaceException;
import com.csforge.sstable.workspace.WorkspaceLock;
import com.csforge.sstable.workspace.WorkspaceManifest;
import com.csforge.sstable.workspace.WorkspaceRepository;
import com.csforge.sstable.workspace.WorkspaceState;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Executes Cassandra-free workspace lifecycle commands under an exclusive lock. */
final class WorkspaceCommandRunner {
    void run(BootstrapArguments arguments, PrintStream out) throws WorkspaceException {
        switch (arguments.action()) {
            case WORKSPACE_CREATE:
                create(arguments, out);
                return;
            case WORKSPACE_STATUS:
                status(arguments, out);
                return;
            case WORKSPACE_STOP:
                stop(arguments, out);
                return;
            case WORKSPACE_RECOVER:
                recover(arguments, out);
                return;
            default:
                throw new IllegalArgumentException("Not a workspace command: "
                        + arguments.action());
        }
    }

    void start(BootstrapArguments arguments,
               AdapterMetadata adapter,
               CassandraInstallation installation,
               PrintStream out) throws WorkspaceException, BootstrapException {
        if (!"3.11".equals(adapter.releaseLine())) {
            throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    "Isolated sandbox start is currently implemented only by the Cassandra "
                            + "3.11 prototype");
        }
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            if (manifest.state() != WorkspaceState.IMPORTED
                    && manifest.state() != WorkspaceState.STOPPED) {
                throw new WorkspaceException("workspace start requires state IMPORTED or "
                        + "STOPPED, not " + manifest.state());
            }

            RuntimeIdentity identity = RuntimeIdentity.capture(installation);
            Map<String, String> outputIdentity = new LinkedHashMap<>();
            outputIdentity.put("sandbox.config-contract", "cassandra-3.11-isolated-v1");
            outputIdentity.put("sandbox.network", "loopback-only");
            manifest = manifest.withRuntimeIdentity(identity.asMap(adapter), outputIdentity);
            repository.save(lock, manifest);

            int nativePort = allocateLoopbackPort();
            String token = controlToken();
            Cassandra311SandboxConfig.write(repository, lock, manifest.workspaceId(),
                    nativePort, token);
            repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.ENDPOINT_PATH);

            WorkerEndpoint endpoint = null;
            try {
                endpoint = new ChildProcessLauncher().startSandbox(installation,
                        repository.root(), manifest.workspaceId(), nativePort);
                new WorkerControlClient().status(repository, manifest.workspaceId());
                manifest.sourceInventory().verifyUnchanged();
                manifest = manifest.transitionTo(WorkspaceState.RUNNING);
                repository.save(lock, manifest);
                printStatus(repository, manifest, out);
                printEndpoint(endpoint, out);
            } catch (BootstrapException | WorkspaceException e) {
                if (endpoint != null) {
                    try {
                        new WorkerControlClient().stop(repository, manifest.workspaceId());
                    } catch (WorkspaceException stopFailure) {
                        e.addSuppressed(stopFailure);
                    }
                }
                try {
                    WorkspaceManifest current = repository.load();
                    if (current.state() != WorkspaceState.FAILED_RECOVERABLE) {
                        repository.save(lock, current.fail("Sandbox start failed: "
                                + e.getMessage()));
                    }
                } catch (WorkspaceException persistenceFailure) {
                    e.addSuppressed(persistenceFailure);
                }
                throw e;
            }
        }
    }

    private static void create(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        requireSeparateArguments(arguments.workspacePath(), arguments.sourceDirectories());
        WorkspaceRepository repository = WorkspaceRepository.createAt(
                arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            SourceInventory requested = SourceInventory.capture(
                    arguments.sourceDirectories());
            requested.verifyUnchanged();
            requireSeparateSourceAndWorkspace(repository, requested);

            WorkspaceManifest manifest;
            if (repository.manifestExists()) {
                manifest = repository.load();
                manifest.sourceInventory().verifyUnchanged();
                if (!manifest.sourceInventory().equals(requested)) {
                    throw new WorkspaceException("Workspace is already initialized with a "
                            + "different SSTable source inventory: " + repository.root());
                }
            } else {
                manifest = WorkspaceManifest.create(requested);
                repository.initialize(lock, manifest);
            }

            if (manifest.state() == WorkspaceState.NEW) {
                manifest = manifest.transitionTo(WorkspaceState.VALIDATED);
                repository.save(lock, manifest);
            }
            printStatus(repository, manifest, out);
        }
    }

    private static void status(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            WorkerEndpoint endpoint = null;
            if (manifest.state() == WorkspaceState.RUNNING) {
                try {
                    endpoint = new WorkerControlClient().status(repository,
                            manifest.workspaceId());
                } catch (WorkspaceException e) {
                    WorkspaceManifest failed = manifest.fail("Worker health check failed: "
                            + e.getMessage());
                    repository.save(lock, failed);
                    throw e;
                }
            }
            printStatus(repository, manifest, out);
            if (endpoint != null) {
                printEndpoint(endpoint, out);
            }
        }
    }

    private static void stop(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            if (manifest.state() == WorkspaceState.STOPPED) {
                printStatus(repository, manifest, out);
                return;
            }
            if (manifest.state() != WorkspaceState.RUNNING
                    && manifest.state() != WorkspaceState.FLUSHED
                    && manifest.state() != WorkspaceState.EXPORTED) {
                throw new WorkspaceException("workspace stop requires state RUNNING, FLUSHED, "
                        + "EXPORTED, or STOPPED, not " + manifest.state());
            }
            final WorkerEndpoint endpoint;
            try {
                endpoint = new WorkerControlClient().stop(repository,
                        manifest.workspaceId());
                manifest.sourceInventory().verifyUnchanged();
            } catch (WorkspaceException e) {
                WorkspaceManifest failed = manifest.fail("Sandbox stop failed: "
                        + e.getMessage());
                repository.save(lock, failed);
                throw e;
            }
            manifest = manifest.transitionTo(WorkspaceState.STOPPED);
            repository.save(lock, manifest);
            printStatus(repository, manifest, out);
            printEndpoint(endpoint, out);
        }
    }

    private static void recover(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            if (manifest.state() == WorkspaceState.FAILED_RECOVERABLE) {
                WorkspaceState recoveryTarget = manifest.lastStableState();
                if (recoveryTarget == WorkspaceState.RUNNING
                        || recoveryTarget == WorkspaceState.FLUSHED
                        || recoveryTarget == WorkspaceState.EXPORTED) {
                    WorkerEndpoint endpoint = WorkerControlClient.readEndpoint(repository,
                            manifest.workspaceId());
                    try {
                        WorkerEndpoint running = new WorkerControlClient().status(repository,
                                manifest.workspaceId());
                        manifest = manifest.recover();
                        repository.save(lock, manifest);
                        printStatus(repository, manifest, out);
                        printEndpoint(running, out);
                        return;
                    } catch (WorkspaceException unreachable) {
                        new WorkerProcessProbe().requireMatchingWorkerStopped(endpoint,
                                manifest.workspaceId(), repository.root());
                    }
                    manifest = manifest.recoverTo(WorkspaceState.STOPPED);
                    repository.save(lock, manifest);
                } else if (recoveryTarget == WorkspaceState.NEW
                        || recoveryTarget == WorkspaceState.VALIDATED) {
                    manifest = manifest.recover();
                    repository.save(lock, manifest);
                } else {
                    throw new WorkspaceException("Recovery from " + recoveryTarget
                            + " requires release-worker reconciliation, which is not yet "
                            + "implemented");
                }
            }
            printStatus(repository, manifest, out);
        }
    }

    private static void printStatus(WorkspaceRepository repository,
                                    WorkspaceManifest manifest,
                                    PrintStream out) {
        out.println("workspace.path=" + repository.root());
        out.println("workspace.id=" + manifest.workspaceId());
        out.println("workspace.formatVersion=" + manifest.formatVersion());
        out.println("workspace.state=" + manifest.state());
        if (manifest.lastStableState() != null) {
            out.println("workspace.lastStableState=" + manifest.lastStableState());
        }
        if (manifest.failureMessage() != null) {
            out.println("workspace.failureMessage=" + singleLine(manifest.failureMessage()));
        }
        out.println("workspace.createdAt=" + manifest.createdAt());
        out.println("workspace.updatedAt=" + manifest.updatedAt());
        out.println("workspace.recoveryAction=" + manifest.recoveryAction());
        out.println("workspace.recoveryDescription="
                + manifest.recoveryAction().description());
        out.println("source.setCount=" + manifest.sourceInventory().sets().size());
        out.println("source.componentCount=" + manifest.sourceInventory().componentCount());
        out.println("source.integrity=verified");
    }

    private static void printEndpoint(WorkerEndpoint endpoint, PrintStream out) {
        out.println("worker.status=" + endpoint.status());
        out.println("worker.pid=" + endpoint.pid());
        out.println("worker.release=" + endpoint.release());
        out.println("worker.native=" + endpoint.nativeAddress() + ":" + endpoint.nativePort());
        out.println("worker.control=" + endpoint.controlAddress() + ":"
                + endpoint.controlPort());
    }

    private static String singleLine(String value) {
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    private static void requireSeparateSourceAndWorkspace(WorkspaceRepository repository,
                                                           SourceInventory inventory)
            throws WorkspaceException {
        Path workspace = repository.root();
        for (SstableSet set : inventory.sets()) {
            requireNoOverlap(workspace, set.directory());
        }
    }

    private static void requireSeparateArguments(Path workspaceArgument,
                                                 List<Path> sourceArguments)
            throws WorkspaceException {
        Path workspace = canonicalCandidate(workspaceArgument);
        for (Path sourceArgument : sourceArguments) {
            final Path source;
            try {
                source = sourceArgument.toRealPath();
            } catch (IOException e) {
                throw new WorkspaceException("Cannot resolve source directory "
                        + sourceArgument, e);
            }
            requireNoOverlap(workspace, source);
        }
    }

    private static Path canonicalCandidate(Path path) throws WorkspaceException {
        Path absolute = path.toAbsolutePath().normalize();
        Path existing = absolute;
        while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            existing = existing.getParent();
        }
        if (existing == null) {
            throw new WorkspaceException("Cannot resolve workspace path " + path);
        }
        try {
            return existing.toRealPath().resolve(existing.relativize(absolute)).normalize();
        } catch (IOException e) {
            throw new WorkspaceException("Cannot resolve workspace path " + path, e);
        }
    }

    private static void requireNoOverlap(Path workspace, Path source)
            throws WorkspaceException {
        if (workspace.startsWith(source) || source.startsWith(workspace)) {
            throw new WorkspaceException("Workspace and SSTable source directories must not "
                    + "overlap: workspace=" + workspace + ", source=" + source);
        }
    }

    private static int allocateLoopbackPort() throws WorkspaceException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 1);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new WorkspaceException("Cannot allocate a loopback native transport port", e);
        }
    }

    private static String controlToken() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        StringBuilder token = new StringBuilder(64);
        for (byte value : bytes) {
            token.append(String.format("%02x", value & 0xff));
        }
        return token.toString();
    }
}
