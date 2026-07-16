package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.WorkerEndpoint;
import com.csforge.sstable.worker.api.ImportResult;
import com.csforge.sstable.workspace.ManifestFile;
import com.csforge.sstable.workspace.SchemaBundle;
import com.csforge.sstable.workspace.SourceInventory;
import com.csforge.sstable.workspace.SstableSet;
import com.csforge.sstable.workspace.WorkspaceException;
import com.csforge.sstable.workspace.WorkspaceLock;
import com.csforge.sstable.workspace.WorkspaceManifest;
import com.csforge.sstable.workspace.WorkspaceRepository;
import com.csforge.sstable.workspace.WorkspaceState;
import com.csforge.sstable.workspace.WorkspaceFileInventory;
import com.csforge.sstable.workspace.WorkspaceFlushResult;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Collections;
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
            case WORKSPACE_FLUSH:
                flush(arguments, out);
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
            verifySchemaIfPresent(repository, manifest);
            if (manifest.state() != WorkspaceState.IMPORTED
                    && manifest.state() != WorkspaceState.STOPPED) {
                throw new WorkspaceException("workspace start requires state IMPORTED or "
                        + "STOPPED, not " + manifest.state());
            }
            WorkspaceFileInventory.verifyUnchanged(repository.root(),
                    manifest.baselineInventory());
            long sourceMaximumMicros = requireSourceMaxTimestamp(manifest);
            boolean timestampPolicyRecorded = manifest.schemaIdentity().containsKey(
                    TimestampPolicy.MANIFEST_KEY);
            TimestampPolicy timestampPolicy = requireTimestampPolicy(
                    manifest, arguments.timestampPolicy(),
                    arguments.timestampPolicySpecified());
            if (!timestampPolicyRecorded) {
                manifest = manifest.withSchemaIdentity(Collections.singletonMap(
                        TimestampPolicy.MANIFEST_KEY, timestampPolicy.value()));
            }

            RuntimeIdentity identity = RuntimeIdentity.capture(installation);
            manifest = manifest.withRuntimeIdentity(identity.asMap(adapter), outputIdentity());
            WorkspaceTimestampState.prepare(repository, lock, manifest.workspaceId(),
                    timestampPolicy, sourceMaximumMicros, !timestampPolicyRecorded);
            repository.deleteOwnedFile(lock, WorkspaceFlushResult.WORKSPACE_PATH);
            repository.save(lock, manifest);

            int nativePort = allocateLoopbackPort();
            String token = randomSecret();
            String nativePassword = randomSecret();
            Cassandra311SandboxConfig.write(repository, lock, manifest.workspaceId(),
                    nativePort, token, nativePassword);
            repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.ENDPOINT_PATH);

            WorkerEndpoint endpoint = null;
            try {
                endpoint = new ChildProcessLauncher().startSandbox(installation,
                        repository.root(), manifest.workspaceId(), nativePort,
                        requiredImportedSchema(manifest, "keyspace"),
                        requiredImportedSchema(manifest, "table"), timestampPolicy,
                        sourceMaximumMicros);
                new WorkerControlClient().status(repository, manifest.workspaceId());
                manifest.sourceInventory().verifyUnchanged();
                WorkspaceFileInventory.verifyUnchanged(repository.root(),
                        manifest.baselineInventory());
                manifest = manifest.transitionTo(WorkspaceState.RUNNING);
                repository.save(lock, manifest);
                printStatus(repository, manifest, out);
                printEndpoint(endpoint, out);
                printNativeAuthentication(repository, out);
            } catch (BootstrapException | WorkspaceException e) {
                if (endpoint != null) {
                    try {
                        new WorkerControlClient().stop(repository, manifest.workspaceId());
                    } catch (WorkspaceException stopFailure) {
                        e.addSuppressed(stopFailure);
                    }
                }
                try {
                    repository.deleteOwnedFile(lock,
                            Cassandra311SandboxConfig.CQLSHRC_PATH);
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

    void importSstables(BootstrapArguments arguments,
                        AdapterMetadata adapter,
                        CassandraInstallation installation,
                        PrintStream out) throws WorkspaceException, BootstrapException {
        if (!"3.11".equals(adapter.releaseLine())) {
            throw new BootstrapException(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    "SSTable import is currently implemented only by the Cassandra 3.11 adapter");
        }
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            verifySchemaIfPresent(repository, manifest);
            if (manifest.state() == WorkspaceState.IMPORTED) {
                verifyBaselineIfPresent(repository, manifest);
                printStatus(repository, manifest, out);
                return;
            }
            if (manifest.state() != WorkspaceState.VALIDATED) {
                throw new WorkspaceException("workspace import requires state VALIDATED, not "
                        + manifest.state());
            }
            verifySchemaBundle(repository, manifest);

            RuntimeIdentity identity = RuntimeIdentity.capture(installation);
            manifest = manifest.withRuntimeIdentity(identity.asMap(adapter), outputIdentity());
            repository.save(lock, manifest);
            int unusedNativePort = allocateLoopbackPort();
            Cassandra311SandboxConfig.writeImport(repository, lock, manifest.workspaceId(),
                    unusedNativePort);
            repository.deleteOwnedFile(lock, ImportResult.WORKSPACE_PATH);

            try {
                ImportResult result = new ChildProcessLauncher().runImport(
                        installation, repository.root(), manifest.workspaceId());
                if (!manifest.workspaceId().equals(result.workspaceId())
                        || !installation.version().toString().equals(result.release())
                        || result.sourceSets() != manifest.sourceInventory().sets().size()) {
                    throw new WorkspaceException("Import worker returned inconsistent identity");
                }
                manifest.sourceInventory().verifyUnchanged();
                verifySchemaBundle(repository, manifest);
                List<ManifestFile> baseline = WorkspaceFileInventory.capture(
                        repository.root(), result.tableDirectory());
                Map<String, String> schema = new LinkedHashMap<>();
                schema.put("keyspace", result.keyspace());
                schema.put("table", result.table());
                schema.put("table.id", result.tableId().toString());
                schema.put("partitioner", result.partitioner());
                schema.put("table.directory", result.tableDirectory());
                schema.put("import.source-sets", Integer.toString(result.sourceSets()));
                schema.put("import.live-sstables", Integer.toString(result.liveSstables()));
                schema.put("import.logical-rows", Long.toString(result.logicalRows()));
                schema.put(SourceTimestampStatus.MANIFEST_KEY,
                        Long.toString(result.sourceMaxTimestampMicros()));
                manifest = manifest.withImportResult(schema, baseline)
                        .transitionTo(WorkspaceState.IMPORTED);
                repository.save(lock, manifest);
                printStatus(repository, manifest, out);
                out.println("import.table=" + result.keyspace() + "." + result.table());
                out.println("import.logicalRows=" + result.logicalRows());
                out.println("import.liveSstables=" + result.liveSstables());
                out.println("import.maxTimestampMicros="
                        + result.sourceMaxTimestampMicros());
            } catch (BootstrapException | WorkspaceException e) {
                WorkspaceManifest current = repository.load();
                if (current.state() != WorkspaceState.FAILED_RECOVERABLE) {
                    repository.save(lock, current.fail("SSTable import failed: "
                            + e.getMessage()));
                }
                manifest.sourceInventory().verifyUnchanged();
                verifySchemaBundle(repository, manifest);
                throw e;
            }
        }
    }

    private static void create(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        requireSeparateArguments(arguments.workspacePath(), arguments.sourceDirectories());
        SchemaBundle schema = arguments.schemaPath() == null
                ? null : SchemaBundle.capture(arguments.schemaPath());
        if (schema != null) {
            requireSchemaOutsideWorkspace(arguments.workspacePath(), schema.source());
        }
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
                if (schema != null) {
                    manifest = manifest.withSchemaIdentity(schema.identity());
                }
                repository.initialize(lock, manifest);
            }

            if (schema != null) {
                if (!manifest.schemaIdentity().isEmpty()) {
                    requireSchemaIdentity(manifest, schema);
                } else {
                    manifest = manifest.withSchemaIdentity(schema.identity());
                    repository.save(lock, manifest);
                }
                repository.writeOwnedFile(lock, SchemaBundle.WORKSPACE_PATH, schema.content());
                schema.verifyUnchanged();
            }
            verifySchemaIfPresent(repository, manifest);

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
            verifySchemaIfPresent(repository, manifest);
            verifyBaselineIfPresent(repository, manifest);
            WorkerEndpoint endpoint = null;
            if (manifest.state() == WorkspaceState.RUNNING) {
                try {
                    WorkerEndpoint recorded = WorkerControlClient.readEndpoint(repository,
                            manifest.workspaceId());
                    if (recorded.status() == WorkerEndpoint.Status.FLUSHED) {
                        endpoint = new WorkerControlClient().flushed(repository,
                                manifest.workspaceId());
                        manifest = transitionToFlushed(repository, lock, manifest, endpoint);
                    } else {
                        endpoint = new WorkerControlClient().status(repository,
                                manifest.workspaceId());
                    }
                } catch (WorkspaceException e) {
                    WorkspaceManifest failed = manifest.fail("Worker health check failed: "
                            + e.getMessage());
                    repository.save(lock, failed);
                    throw e;
                }
            } else if (manifest.state() == WorkspaceState.FLUSHED) {
                try {
                    endpoint = new WorkerControlClient().flushed(repository,
                            manifest.workspaceId());
                    requireFlushResult(repository, manifest, endpoint.release());
                } catch (WorkspaceException e) {
                    WorkspaceManifest failed = manifest.fail("Flushed worker health check "
                            + "failed: " + e.getMessage());
                    repository.save(lock, failed);
                    throw e;
                }
            }
            printStatus(repository, manifest, out);
            if (endpoint != null) {
                printEndpoint(endpoint, out);
                if (endpoint.status() == WorkerEndpoint.Status.RUNNING) {
                    printNativeAuthentication(repository, out);
                }
            }
        }
    }

    private static void flush(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            verifySchemaIfPresent(repository, manifest);
            verifyBaselineIfPresent(repository, manifest);
            if (manifest.state() == WorkspaceState.FLUSHED) {
                WorkerEndpoint endpoint = new WorkerControlClient().flushed(repository,
                        manifest.workspaceId());
                requireFlushResult(repository, manifest, endpoint.release());
                repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.CQLSHRC_PATH);
                printStatus(repository, manifest, out);
                printEndpoint(endpoint, out);
                return;
            }
            if (manifest.state() != WorkspaceState.RUNNING) {
                throw new WorkspaceException("workspace flush requires state RUNNING or "
                        + "FLUSHED, not " + manifest.state());
            }

            WorkerEndpoint endpoint;
            try {
                endpoint = new WorkerControlClient().flush(repository,
                        manifest.workspaceId());
                manifest = transitionToFlushed(repository, lock, manifest, endpoint);
            } catch (WorkspaceException e) {
                try {
                    WorkerEndpoint recorded = WorkerControlClient.readEndpoint(repository,
                            manifest.workspaceId());
                    if (recorded.status() == WorkerEndpoint.Status.FLUSHED) {
                        endpoint = new WorkerControlClient().flushed(repository,
                                manifest.workspaceId());
                        manifest = transitionToFlushed(repository, lock, manifest, endpoint);
                        printStatus(repository, manifest, out);
                        printEndpoint(endpoint, out);
                        return;
                    }
                } catch (WorkspaceException reconciliationFailure) {
                    e.addSuppressed(reconciliationFailure);
                }
                WorkspaceManifest failed = manifest.fail("Workspace flush failed: "
                        + e.getMessage());
                repository.save(lock, failed);
                throw e;
            }
            printStatus(repository, manifest, out);
            printEndpoint(endpoint, out);
        }
    }

    private static void stop(BootstrapArguments arguments, PrintStream out)
            throws WorkspaceException {
        WorkspaceRepository repository = WorkspaceRepository.open(arguments.workspacePath());
        try (WorkspaceLock lock = repository.acquire()) {
            WorkspaceManifest manifest = repository.load();
            manifest.sourceInventory().verifyUnchanged();
            verifySchemaIfPresent(repository, manifest);
            verifyBaselineIfPresent(repository, manifest);
            if (manifest.state() == WorkspaceState.STOPPED) {
                repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.CQLSHRC_PATH);
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
                repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.CQLSHRC_PATH);
                manifest.sourceInventory().verifyUnchanged();
                verifyBaselineIfPresent(repository, manifest);
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
            verifySchemaIfPresent(repository, manifest);
            verifyBaselineIfPresent(repository, manifest);
            if (manifest.state() == WorkspaceState.RUNNING) {
                WorkerEndpoint recorded = WorkerControlClient.readEndpoint(repository,
                        manifest.workspaceId());
                if (recorded.status() == WorkerEndpoint.Status.FLUSHED) {
                    WorkerEndpoint flushed = new WorkerControlClient().flushed(repository,
                            manifest.workspaceId());
                    manifest = transitionToFlushed(repository, lock, manifest, flushed);
                    printStatus(repository, manifest, out);
                    printEndpoint(flushed, out);
                    return;
                }
            }
            if (manifest.state() == WorkspaceState.FAILED_RECOVERABLE) {
                WorkspaceState recoveryTarget = manifest.lastStableState();
                if (recoveryTarget == WorkspaceState.RUNNING
                        || recoveryTarget == WorkspaceState.FLUSHED
                        || recoveryTarget == WorkspaceState.EXPORTED) {
                    WorkerEndpoint endpoint = WorkerControlClient.readEndpoint(repository,
                            manifest.workspaceId());
                    try {
                        if (endpoint.status() == WorkerEndpoint.Status.FLUSHED
                                && (recoveryTarget == WorkspaceState.RUNNING
                                || recoveryTarget == WorkspaceState.FLUSHED)) {
                            WorkerEndpoint flushed = new WorkerControlClient().flushed(
                                    repository, manifest.workspaceId());
                            requireFlushResult(repository, manifest, flushed.release());
                            if (recoveryTarget == WorkspaceState.FLUSHED) {
                                repository.deleteOwnedFile(lock,
                                        Cassandra311SandboxConfig.CQLSHRC_PATH);
                            }
                            manifest = manifest.recover();
                            repository.save(lock, manifest);
                            if (recoveryTarget == WorkspaceState.RUNNING) {
                                manifest = transitionToFlushed(repository, lock, manifest,
                                        flushed);
                            }
                            printStatus(repository, manifest, out);
                            printEndpoint(flushed, out);
                            return;
                        }
                        WorkerEndpoint running = new WorkerControlClient().status(repository,
                                manifest.workspaceId());
                        manifest = manifest.recover();
                        repository.save(lock, manifest);
                        printStatus(repository, manifest, out);
                        printEndpoint(running, out);
                        printNativeAuthentication(repository, out);
                        return;
                    } catch (WorkspaceException unreachable) {
                        new WorkerProcessProbe().requireMatchingWorkerStopped(endpoint,
                                manifest.workspaceId(), repository.root());
                    }
                    manifest = manifest.recoverTo(WorkspaceState.STOPPED);
                    repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.CQLSHRC_PATH);
                    repository.save(lock, manifest);
                } else if (recoveryTarget == WorkspaceState.NEW
                        || recoveryTarget == WorkspaceState.VALIDATED) {
                    manifest = manifest.recover();
                    repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.CQLSHRC_PATH);
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
                                    PrintStream out) throws WorkspaceException {
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
        Long sourceMaximum = sourceMaxTimestamp(manifest);
        if (sourceMaximum != null) {
            SourceTimestampStatus.print(sourceMaximum,
                    SourceTimestampStatus.currentTimeMicros(), out);
        }
        String timestampPolicy = manifest.schemaIdentity().get(TimestampPolicy.MANIFEST_KEY);
        if (timestampPolicy != null) {
            out.println("timestamp.policy=" + timestampPolicy);
            TimestampPolicy policy = parseTimestampPolicy(timestampPolicy);
            Long highWater = WorkspaceTimestampState.validatedHighWater(repository,
                    manifest.workspaceId(), policy, requireSourceMaxTimestamp(manifest));
            if (highWater != null) {
                out.println("timestamp.highWaterMicros=" + highWater);
            }
        }
        Path flushPath = repository.resolveInside(WorkspaceFlushResult.WORKSPACE_PATH);
        boolean flushResultRequired = manifest.state() == WorkspaceState.FLUSHED
                || manifest.state() == WorkspaceState.EXPORTED;
        if (flushResultRequired || Files.exists(flushPath, LinkOption.NOFOLLOW_LINKS)) {
            WorkspaceFlushResult result = requireFlushResult(repository, manifest,
                    requiredRuntimeValue(manifest, "cassandra.version"));
            out.println("flush.flushedAt=" + result.flushedAt());
            out.println("flush.fileCount=" + result.files().size());
            out.println("flush.deltaFileCount="
                    + result.deltaFiles(manifest.baselineInventory()).size());
        }
    }

    private static void printEndpoint(WorkerEndpoint endpoint, PrintStream out) {
        out.println("worker.status=" + endpoint.status());
        out.println("worker.pid=" + endpoint.pid());
        out.println("worker.release=" + endpoint.release());
        out.println("worker.native=" + endpoint.nativeAddress() + ":" + endpoint.nativePort());
        out.println("worker.control=" + endpoint.controlAddress() + ":"
                + endpoint.controlPort());
    }

    private static void printNativeAuthentication(WorkspaceRepository repository,
                                                  PrintStream out) {
        out.println("worker.username=" + Cassandra311SandboxConfig.NATIVE_USERNAME);
        out.println("worker.cqlshrc="
                + repository.root().resolve(Cassandra311SandboxConfig.CQLSHRC_PATH));
    }

    private static String singleLine(String value) {
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    private static void verifySchemaBundle(WorkspaceRepository repository,
                                           WorkspaceManifest manifest)
            throws WorkspaceException {
        String source = manifest.schemaIdentity().get("bundle.source");
        if (source == null) {
            throw new WorkspaceException("workspace import requires a schema bundle captured "
                    + "with workspace create --schema");
        }
        SchemaBundle sourceBundle = SchemaBundle.capture(Paths.get(source));
        requireSchemaIdentity(manifest, sourceBundle);
        SchemaBundle workspaceBundle = SchemaBundle.capture(
                repository.resolveInside(SchemaBundle.WORKSPACE_PATH));
        if (workspaceBundle.size() != sourceBundle.size()
                || !workspaceBundle.sha256().equals(sourceBundle.sha256())) {
            throw new WorkspaceException("Workspace schema copy does not match captured bundle");
        }
    }

    private static void requireSchemaIdentity(WorkspaceManifest manifest, SchemaBundle schema)
            throws WorkspaceException {
        if (!schema.source().toString().equals(manifest.schemaIdentity().get("bundle.source"))
                || !Long.toString(schema.size()).equals(
                manifest.schemaIdentity().get("bundle.size"))
                || !schema.sha256().equals(manifest.schemaIdentity().get("bundle.sha256"))) {
            throw new WorkspaceException("Workspace is already initialized with a different "
                    + "schema bundle");
        }
    }

    private static Map<String, String> outputIdentity() {
        Map<String, String> output = new LinkedHashMap<>();
        output.put("sandbox.config-contract", "cassandra-3.11-isolated-v1");
        output.put("sandbox.network", "loopback-only");
        output.put("native.authentication", "cassandra-3.11-cqlshrc-v1");
        output.put("native.query-guard", "cassandra-3.11-workspace-v4");
        output.put("flush.contract", "cassandra-3.11-quiesced-table-v1");
        output.put("import.contract", "cassandra-3.11-refresh-v2");
        return output;
    }

    private static String requiredImportedSchema(WorkspaceManifest manifest, String name)
            throws WorkspaceException {
        String value = manifest.schemaIdentity().get(name);
        if (value == null || value.isEmpty()) {
            throw new WorkspaceException("Imported workspace is missing schema " + name);
        }
        return value;
    }

    private static long requireSourceMaxTimestamp(WorkspaceManifest manifest)
            throws WorkspaceException {
        Long value = sourceMaxTimestamp(manifest);
        if (value == null) {
            throw new WorkspaceException("Imported workspace source timestamp metadata is "
                    + "missing");
        }
        return value;
    }

    private static Long sourceMaxTimestamp(WorkspaceManifest manifest)
            throws WorkspaceException {
        String value = manifest.schemaIdentity().get(SourceTimestampStatus.MANIFEST_KEY);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new WorkspaceException("Imported workspace source timestamp metadata is "
                    + "invalid", e);
        }
    }

    private static TimestampPolicy requireTimestampPolicy(WorkspaceManifest manifest,
                                                          TimestampPolicy requested,
                                                          boolean explicitlyRequested)
            throws WorkspaceException {
        String recorded = manifest.schemaIdentity().get(TimestampPolicy.MANIFEST_KEY);
        if (recorded == null) {
            return requested;
        }
        TimestampPolicy policy = parseTimestampPolicy(recorded);
        if (explicitlyRequested && policy != requested) {
            throw new WorkspaceException("Workspace timestamp policy is " + recorded
                    + "; restart with --timestamp-policy " + recorded);
        }
        return policy;
    }

    private static WorkspaceManifest transitionToFlushed(WorkspaceRepository repository,
                                                          WorkspaceLock lock,
                                                          WorkspaceManifest manifest,
                                                          WorkerEndpoint endpoint)
            throws WorkspaceException {
        if (manifest.state() != WorkspaceState.RUNNING) {
            throw new WorkspaceException("Cannot record a flush from workspace state "
                    + manifest.state());
        }
        requireFlushResult(repository, manifest, endpoint.release());
        manifest.sourceInventory().verifyUnchanged();
        verifySchemaIfPresent(repository, manifest);
        verifyBaselineIfPresent(repository, manifest);
        repository.deleteOwnedFile(lock, Cassandra311SandboxConfig.CQLSHRC_PATH);
        WorkspaceManifest flushed = manifest.transitionTo(WorkspaceState.FLUSHED);
        repository.save(lock, flushed);
        return flushed;
    }

    private static WorkspaceFlushResult requireFlushResult(WorkspaceRepository repository,
                                                            WorkspaceManifest manifest,
                                                            String release)
            throws WorkspaceException {
        WorkspaceFlushResult result = WorkspaceFlushResult.read(repository.root());
        result.requireIdentity(manifest.workspaceId(), release,
                requiredImportedSchema(manifest, "keyspace"),
                requiredImportedSchema(manifest, "table"),
                requiredImportedSchema(manifest, "table.directory"));
        result.verifyCompleteInventory(repository.root());
        result.deltaFiles(manifest.baselineInventory());
        return result;
    }

    private static String requiredRuntimeValue(WorkspaceManifest manifest, String key)
            throws WorkspaceException {
        String value = manifest.runtimeIdentity().get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new WorkspaceException("Workspace runtime identity is missing " + key);
        }
        return value;
    }

    private static TimestampPolicy parseTimestampPolicy(String recorded)
            throws WorkspaceException {
        final TimestampPolicy policy;
        try {
            policy = TimestampPolicy.valueOf(recorded.replace('-', '_').toUpperCase(
                    java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new WorkspaceException("Workspace timestamp policy is invalid", e);
        }
        return policy;
    }

    private static void verifyBaselineIfPresent(WorkspaceRepository repository,
                                                WorkspaceManifest manifest)
            throws WorkspaceException {
        if (!manifest.baselineInventory().isEmpty()) {
            WorkspaceFileInventory.verifyUnchanged(repository.root(),
                    manifest.baselineInventory());
        }
    }

    private static void verifySchemaIfPresent(WorkspaceRepository repository,
                                              WorkspaceManifest manifest)
            throws WorkspaceException {
        if (!manifest.schemaIdentity().isEmpty()) {
            verifySchemaBundle(repository, manifest);
        }
    }

    private static void requireSchemaOutsideWorkspace(Path workspaceArgument, Path schema)
            throws WorkspaceException {
        Path workspace = canonicalCandidate(workspaceArgument);
        if (schema.startsWith(workspace)) {
            throw new WorkspaceException("Schema bundle must be outside the workspace: "
                    + schema);
        }
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

    private static String randomSecret() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        StringBuilder token = new StringBuilder(64);
        for (byte value : bytes) {
            token.append(String.format("%02x", value & 0xff));
        }
        return token.toString();
    }
}
