package com.axonops.sstable.bootstrap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** Minimal JDK-only command-line parser for bootstrap options and runtime commands. */
final class BootstrapArguments {
    enum Action {
        HELP,
        VERSION,
        RUNTIME_INSPECT,
        RUNTIME_PREFLIGHT,
        DIRECT_CQLSH,
        WORKSPACE_CREATE,
        WORKSPACE_IMPORT,
        WORKSPACE_START,
        WORKSPACE_CQLSH,
        WORKSPACE_STATUS,
        WORKSPACE_FLUSH,
        WORKSPACE_EXPORT,
        WORKSPACE_STOP,
        WORKSPACE_RECOVER,
        WORKSPACE_DESTROY
    }

    private final Action action;
    private final RuntimeOptions runtimeOptions;
    private final Path workspacePath;
    private final List<Path> sourceDirectories;
    private final Path schemaPath;
    private final TimestampPolicy timestampPolicy;
    private final boolean timestampPolicySpecified;
    private final ExportMode exportMode;
    private final Path outputPath;
    private final Path temporaryDirectory;
    private final String executeCql;
    private final UUID confirmedWorkspaceId;

    private BootstrapArguments(Action action,
                               RuntimeOptions runtimeOptions,
                               Path workspacePath,
                               List<Path> sourceDirectories,
                               Path schemaPath,
                               TimestampPolicy timestampPolicy,
                               boolean timestampPolicySpecified,
                               ExportMode exportMode,
                               Path outputPath,
                               Path temporaryDirectory,
                               String executeCql,
                               UUID confirmedWorkspaceId) {
        this.action = action;
        this.runtimeOptions = runtimeOptions;
        this.workspacePath = workspacePath;
        this.sourceDirectories = Collections.unmodifiableList(
                new ArrayList<>(sourceDirectories));
        this.schemaPath = schemaPath;
        this.timestampPolicy = timestampPolicy;
        this.timestampPolicySpecified = timestampPolicySpecified;
        this.exportMode = exportMode;
        this.outputPath = outputPath;
        this.temporaryDirectory = temporaryDirectory;
        this.executeCql = executeCql;
        this.confirmedWorkspaceId = confirmedWorkspaceId;
    }

    static BootstrapArguments parse(String[] args) throws BootstrapException {
        if (args.length == 0) {
            return new BootstrapArguments(Action.HELP, new RuntimeOptions(null, null, null),
                    null, Collections.<Path>emptyList(), null, TimestampPolicy.WALL_CLOCK,
                    false, null, null, Paths.get("/tmp/sstable-tools"), null, null);
        }

        Path cassandraHome = null;
        Path cassandraConf = null;
        Path javaHome = null;
        List<Path> sourceDirectories = new ArrayList<>();
        Path schemaPath = null;
        TimestampPolicy timestampPolicy = TimestampPolicy.WALL_CLOCK;
        boolean timestampPolicySpecified = false;
        ExportMode exportMode = null;
        Path outputPath = null;
        Path temporaryDirectory = Paths.get("/tmp/sstable-tools");
        String executeCql = null;
        UUID confirmedWorkspaceId = null;
        List<String> command = new ArrayList<>();

        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if ("--help".equals(argument) || "-h".equals(argument)) {
                return new BootstrapArguments(Action.HELP,
                        new RuntimeOptions(cassandraHome, cassandraConf, javaHome),
                        null, sourceDirectories, schemaPath, timestampPolicy,
                        timestampPolicySpecified, exportMode, outputPath, temporaryDirectory,
                        executeCql,
                        confirmedWorkspaceId);
            }
            if ("--version".equals(argument)) {
                return new BootstrapArguments(Action.VERSION,
                        new RuntimeOptions(cassandraHome, cassandraConf, javaHome),
                        null, sourceDirectories, schemaPath, timestampPolicy,
                        timestampPolicySpecified, exportMode, outputPath, temporaryDirectory,
                        executeCql,
                        confirmedWorkspaceId);
            }
            if ("--cassandra-home".equals(argument)) {
                cassandraHome = pathValue(args, ++index, argument);
            } else if ("--cassandra-conf".equals(argument)) {
                cassandraConf = pathValue(args, ++index, argument);
            } else if ("--java-home".equals(argument)) {
                javaHome = pathValue(args, ++index, argument);
            } else if ("--sstables".equals(argument)) {
                sourceDirectories.addAll(sourcePaths(args, ++index));
            } else if ("--schema".equals(argument)) {
                schemaPath = pathValue(args, ++index, argument);
            } else if ("--timestamp-policy".equals(argument)) {
                if (timestampPolicySpecified || ++index >= args.length) {
                    throw usage("--timestamp-policy requires one value");
                }
                timestampPolicy = TimestampPolicy.parse(args[index]);
                timestampPolicySpecified = true;
            } else if ("--mode".equals(argument)) {
                if (exportMode != null || ++index >= args.length) {
                    throw usage("--mode requires one value");
                }
                exportMode = ExportMode.parse(args[index]);
            } else if ("--output".equals(argument)) {
                if (outputPath != null) {
                    throw usage("--output may be specified only once");
                }
                outputPath = pathValue(args, ++index, argument);
            } else if ("--tmp-dir".equals(argument)) {
                temporaryDirectory = pathValue(args, ++index, argument);
            } else if ("--execute".equals(argument)) {
                if (executeCql != null || ++index >= args.length
                        || args[index].trim().isEmpty()) {
                    throw usage("--execute requires one non-empty CQL statement");
                }
                executeCql = args[index];
            } else if ("--confirm-workspace-id".equals(argument)) {
                if (confirmedWorkspaceId != null || ++index >= args.length
                        || args[index].trim().isEmpty()) {
                    throw usage("--confirm-workspace-id requires one UUID");
                }
                try {
                    confirmedWorkspaceId = UUID.fromString(args[index]);
                } catch (IllegalArgumentException e) {
                    throw usage("--confirm-workspace-id requires a valid UUID");
                }
            } else if (argument.startsWith("--")) {
                throw usage("Unknown option: " + argument);
            } else {
                command.add(argument);
            }
        }

        Action action;
        Path workspacePath = null;
        if (command.size() == 2 && "runtime".equals(command.get(0))
                && "inspect".equals(command.get(1))) {
            action = Action.RUNTIME_INSPECT;
        } else if (command.size() == 2 && "runtime".equals(command.get(0))
                && "preflight".equals(command.get(1))) {
            action = Action.RUNTIME_PREFLIGHT;
        } else if (command.size() == 1 && "cqlsh".equals(command.get(0))) {
            action = Action.DIRECT_CQLSH;
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "create".equals(command.get(1))) {
            action = Action.WORKSPACE_CREATE;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "import".equals(command.get(1))) {
            action = Action.WORKSPACE_IMPORT;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "start".equals(command.get(1))) {
            action = Action.WORKSPACE_START;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "cqlsh".equals(command.get(1))) {
            action = Action.WORKSPACE_CQLSH;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "status".equals(command.get(1))) {
            action = Action.WORKSPACE_STATUS;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "flush".equals(command.get(1))) {
            action = Action.WORKSPACE_FLUSH;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "export".equals(command.get(1))) {
            action = Action.WORKSPACE_EXPORT;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "stop".equals(command.get(1))) {
            action = Action.WORKSPACE_STOP;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "recover".equals(command.get(1))) {
            action = Action.WORKSPACE_RECOVER;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "destroy".equals(command.get(1))) {
            action = Action.WORKSPACE_DESTROY;
            workspacePath = commandPath(command.get(2));
        } else {
            throw usage("Expected 'cqlsh', 'runtime inspect', 'runtime preflight', or a "
                    + "supported 'workspace <command> <path>' command");
        }

        boolean workspaceAction = action == Action.WORKSPACE_CREATE
                || action == Action.WORKSPACE_IMPORT || action == Action.WORKSPACE_START
                || action == Action.WORKSPACE_CQLSH
                || action == Action.WORKSPACE_STATUS
                || action == Action.WORKSPACE_FLUSH
                || action == Action.WORKSPACE_EXPORT
                || action == Action.WORKSPACE_STOP || action == Action.WORKSPACE_RECOVER
                || action == Action.WORKSPACE_DESTROY;
        if (workspaceAction && action != Action.WORKSPACE_START
                && action != Action.WORKSPACE_IMPORT
                && action != Action.WORKSPACE_CQLSH
                && (cassandraHome != null || cassandraConf != null
                || javaHome != null)) {
            throw usage("Cassandra runtime options are not accepted by this workspace command");
        }
        if ((action == Action.WORKSPACE_CREATE || action == Action.DIRECT_CQLSH)
                && sourceDirectories.isEmpty()) {
            throw usage(action == Action.DIRECT_CQLSH
                    ? "cqlsh requires at least one --sstables source"
                    : "workspace create requires at least one --sstables source");
        }
        if (action != Action.WORKSPACE_CREATE && action != Action.DIRECT_CQLSH
                && !sourceDirectories.isEmpty()) {
            throw usage("--sstables is only valid with workspace create");
        }
        if (action != Action.WORKSPACE_CREATE && action != Action.DIRECT_CQLSH
                && schemaPath != null) {
            throw usage("--schema is only valid with workspace create");
        }
        if (action == Action.DIRECT_CQLSH && schemaPath == null) {
            throw usage("cqlsh requires --schema <path>");
        }
        if (action != Action.WORKSPACE_START && action != Action.DIRECT_CQLSH
                && timestampPolicySpecified) {
            throw usage("--timestamp-policy is only valid with workspace start");
        }
        if (action == Action.WORKSPACE_EXPORT && (exportMode == null || outputPath == null)) {
            throw usage("workspace export requires --mode delta|snapshot and --output <path>");
        }
        if (action != Action.WORKSPACE_EXPORT && (exportMode != null || outputPath != null)) {
            throw usage("--mode and --output are only valid with workspace export");
        }
        if (action != Action.WORKSPACE_CQLSH && action != Action.DIRECT_CQLSH
                && executeCql != null) {
            throw usage("--execute is only valid with workspace cqlsh");
        }
        if (action != Action.DIRECT_CQLSH
                && !Paths.get("/tmp/sstable-tools").equals(temporaryDirectory)) {
            throw usage("--tmp-dir is only valid with cqlsh");
        }
        if (action == Action.WORKSPACE_DESTROY && confirmedWorkspaceId == null) {
            throw usage("workspace destroy requires --confirm-workspace-id <uuid>");
        }
        if (action != Action.WORKSPACE_DESTROY && confirmedWorkspaceId != null) {
            throw usage("--confirm-workspace-id is only valid with workspace destroy");
        }
        return new BootstrapArguments(action,
                new RuntimeOptions(cassandraHome, cassandraConf, javaHome),
                workspacePath, sourceDirectories, schemaPath, timestampPolicy,
                timestampPolicySpecified, exportMode, outputPath, temporaryDirectory, executeCql,
                confirmedWorkspaceId);
    }

    private static Path pathValue(String[] args, int index, String option)
            throws BootstrapException {
        if (index >= args.length || args[index].trim().isEmpty()) {
            throw usage(option + " requires a path");
        }
        try {
            return Paths.get(args[index]);
        } catch (RuntimeException e) {
            throw usage(option + " has an invalid path: " + args[index]);
        }
    }

    private static List<Path> sourcePaths(String[] args, int index) throws BootstrapException {
        if (index >= args.length || args[index].trim().isEmpty()) {
            throw usage("--sstables requires at least one path");
        }
        String[] values = args[index].split(",", -1);
        List<Path> paths = new ArrayList<>();
        for (String value : values) {
            if (value.trim().isEmpty()) {
                throw usage("--sstables contains an empty path");
            }
            try {
                paths.add(Paths.get(value));
            } catch (RuntimeException e) {
                throw usage("--sstables has an invalid path: " + value);
            }
        }
        return paths;
    }

    private static Path commandPath(String value) throws BootstrapException {
        if (value.trim().isEmpty()) {
            throw usage("Workspace path must not be empty");
        }
        try {
            return Paths.get(value);
        } catch (RuntimeException e) {
            throw usage("Invalid workspace path: " + value);
        }
    }

    private static BootstrapException usage(String message) {
        return new BootstrapException(BootstrapException.USAGE_EXIT_CODE, message);
    }

    Action action() {
        return action;
    }

    RuntimeOptions runtimeOptions() {
        return runtimeOptions;
    }

    Path workspacePath() {
        return workspacePath;
    }

    List<Path> sourceDirectories() {
        return sourceDirectories;
    }

    Path schemaPath() {
        return schemaPath;
    }

    TimestampPolicy timestampPolicy() {
        return timestampPolicy;
    }

    boolean timestampPolicySpecified() {
        return timestampPolicySpecified;
    }

    ExportMode exportMode() {
        return exportMode;
    }

    Path outputPath() {
        return outputPath;
    }

    Path temporaryDirectory() {
        return temporaryDirectory;
    }

    String executeCql() {
        return executeCql;
    }

    UUID confirmedWorkspaceId() {
        return confirmedWorkspaceId;
    }

    BootstrapArguments forWorkspace(Action workspaceAction, Path path,
                                    TimestampPolicy policy, boolean policySpecified) {
        return new BootstrapArguments(workspaceAction, runtimeOptions, path,
                sourceDirectories, schemaPath, policy, policySpecified, exportMode,
                outputPath, temporaryDirectory, executeCql, confirmedWorkspaceId);
    }

    BootstrapArguments withConfirmation(UUID workspaceId) {
        return new BootstrapArguments(action, runtimeOptions, workspacePath,
                sourceDirectories, schemaPath, timestampPolicy, timestampPolicySpecified,
                exportMode, outputPath, temporaryDirectory, executeCql, workspaceId);
    }
}
