package com.csforge.sstable.bootstrap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Minimal JDK-only command-line parser for bootstrap options and runtime commands. */
final class BootstrapArguments {
    enum Action {
        HELP,
        VERSION,
        RUNTIME_INSPECT,
        RUNTIME_PREFLIGHT,
        WORKSPACE_CREATE,
        WORKSPACE_START,
        WORKSPACE_STATUS,
        WORKSPACE_STOP,
        WORKSPACE_RECOVER
    }

    private final Action action;
    private final RuntimeOptions runtimeOptions;
    private final Path workspacePath;
    private final List<Path> sourceDirectories;

    private BootstrapArguments(Action action,
                               RuntimeOptions runtimeOptions,
                               Path workspacePath,
                               List<Path> sourceDirectories) {
        this.action = action;
        this.runtimeOptions = runtimeOptions;
        this.workspacePath = workspacePath;
        this.sourceDirectories = Collections.unmodifiableList(
                new ArrayList<>(sourceDirectories));
    }

    static BootstrapArguments parse(String[] args) throws BootstrapException {
        if (args.length == 0) {
            return new BootstrapArguments(Action.HELP, new RuntimeOptions(null, null, null),
                    null, Collections.<Path>emptyList());
        }

        Path cassandraHome = null;
        Path cassandraConf = null;
        Path javaHome = null;
        List<Path> sourceDirectories = new ArrayList<>();
        List<String> command = new ArrayList<>();

        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if ("--help".equals(argument) || "-h".equals(argument)) {
                return new BootstrapArguments(Action.HELP,
                        new RuntimeOptions(cassandraHome, cassandraConf, javaHome),
                        null, sourceDirectories);
            }
            if ("--version".equals(argument)) {
                return new BootstrapArguments(Action.VERSION,
                        new RuntimeOptions(cassandraHome, cassandraConf, javaHome),
                        null, sourceDirectories);
            }
            if ("--cassandra-home".equals(argument)) {
                cassandraHome = pathValue(args, ++index, argument);
            } else if ("--cassandra-conf".equals(argument)) {
                cassandraConf = pathValue(args, ++index, argument);
            } else if ("--java-home".equals(argument)) {
                javaHome = pathValue(args, ++index, argument);
            } else if ("--sstables".equals(argument)) {
                sourceDirectories.add(pathValue(args, ++index, argument));
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
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "create".equals(command.get(1))) {
            action = Action.WORKSPACE_CREATE;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "start".equals(command.get(1))) {
            action = Action.WORKSPACE_START;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "status".equals(command.get(1))) {
            action = Action.WORKSPACE_STATUS;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "stop".equals(command.get(1))) {
            action = Action.WORKSPACE_STOP;
            workspacePath = commandPath(command.get(2));
        } else if (command.size() == 3 && "workspace".equals(command.get(0))
                && "recover".equals(command.get(1))) {
            action = Action.WORKSPACE_RECOVER;
            workspacePath = commandPath(command.get(2));
        } else {
            throw usage("Expected 'runtime inspect', 'runtime preflight', or a supported "
                    + "'workspace <command> <path>' command");
        }

        boolean workspaceAction = action == Action.WORKSPACE_CREATE
                || action == Action.WORKSPACE_START || action == Action.WORKSPACE_STATUS
                || action == Action.WORKSPACE_STOP || action == Action.WORKSPACE_RECOVER;
        if (workspaceAction && action != Action.WORKSPACE_START
                && (cassandraHome != null || cassandraConf != null
                || javaHome != null)) {
            throw usage("Cassandra runtime options are not accepted by this workspace command");
        }
        if (action == Action.WORKSPACE_CREATE && sourceDirectories.isEmpty()) {
            throw usage("workspace create requires at least one --sstables directory");
        }
        if (action != Action.WORKSPACE_CREATE && !sourceDirectories.isEmpty()) {
            throw usage("--sstables is only valid with workspace create");
        }
        return new BootstrapArguments(action,
                new RuntimeOptions(cassandraHome, cassandraConf, javaHome),
                workspacePath, sourceDirectories);
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
}
