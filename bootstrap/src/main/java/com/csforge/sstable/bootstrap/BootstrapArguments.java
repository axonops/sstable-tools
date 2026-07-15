package com.csforge.sstable.bootstrap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Minimal JDK-only command-line parser for bootstrap options and runtime commands. */
final class BootstrapArguments {
    enum Action {
        HELP,
        VERSION,
        RUNTIME_INSPECT,
        RUNTIME_PREFLIGHT
    }

    private final Action action;
    private final RuntimeOptions runtimeOptions;

    private BootstrapArguments(Action action, RuntimeOptions runtimeOptions) {
        this.action = action;
        this.runtimeOptions = runtimeOptions;
    }

    static BootstrapArguments parse(String[] args) throws BootstrapException {
        if (args.length == 0) {
            return new BootstrapArguments(Action.HELP, new RuntimeOptions(null, null, null));
        }

        Path cassandraHome = null;
        Path cassandraConf = null;
        Path javaHome = null;
        List<String> command = new ArrayList<>();

        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if ("--help".equals(argument) || "-h".equals(argument)) {
                return new BootstrapArguments(Action.HELP,
                        new RuntimeOptions(cassandraHome, cassandraConf, javaHome));
            }
            if ("--version".equals(argument)) {
                return new BootstrapArguments(Action.VERSION,
                        new RuntimeOptions(cassandraHome, cassandraConf, javaHome));
            }
            if ("--cassandra-home".equals(argument)) {
                cassandraHome = pathValue(args, ++index, argument);
            } else if ("--cassandra-conf".equals(argument)) {
                cassandraConf = pathValue(args, ++index, argument);
            } else if ("--java-home".equals(argument)) {
                javaHome = pathValue(args, ++index, argument);
            } else if (argument.startsWith("--")) {
                throw usage("Unknown option: " + argument);
            } else {
                command.add(argument);
            }
        }

        Action action;
        if (command.size() == 2 && "runtime".equals(command.get(0))
                && "inspect".equals(command.get(1))) {
            action = Action.RUNTIME_INSPECT;
        } else if (command.size() == 2 && "runtime".equals(command.get(0))
                && "preflight".equals(command.get(1))) {
            action = Action.RUNTIME_PREFLIGHT;
        } else {
            throw usage("Expected command 'runtime inspect' or 'runtime preflight'");
        }
        return new BootstrapArguments(action,
                new RuntimeOptions(cassandraHome, cassandraConf, javaHome));
    }

    private static Path pathValue(String[] args, int index, String option)
            throws BootstrapException {
        if (index >= args.length || args[index].trim().isEmpty()) {
            throw usage(option + " requires a path");
        }
        return Paths.get(args[index]);
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
}
