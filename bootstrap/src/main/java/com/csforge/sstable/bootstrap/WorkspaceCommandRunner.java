package com.csforge.sstable.bootstrap;

import com.csforge.sstable.workspace.SourceInventory;
import com.csforge.sstable.workspace.SstableSet;
import com.csforge.sstable.workspace.WorkspaceException;
import com.csforge.sstable.workspace.WorkspaceLock;
import com.csforge.sstable.workspace.WorkspaceManifest;
import com.csforge.sstable.workspace.WorkspaceRepository;
import com.csforge.sstable.workspace.WorkspaceState;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;

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
            case WORKSPACE_RECOVER:
                recover(arguments, out);
                return;
            default:
                throw new IllegalArgumentException("Not a workspace command: "
                        + arguments.action());
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
            printStatus(repository, manifest, out);
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
                if (recoveryTarget != WorkspaceState.NEW
                        && recoveryTarget != WorkspaceState.VALIDATED) {
                    throw new WorkspaceException("Recovery from " + recoveryTarget
                            + " requires release-worker reconciliation, which is not yet "
                            + "implemented");
                }
                manifest = manifest.recover();
                repository.save(lock, manifest);
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
}
