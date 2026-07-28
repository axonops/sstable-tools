package com.axonops.sstable.bootstrap;

import com.axonops.sstable.worker.api.WorkerEndpoint;
import com.axonops.sstable.workspace.WorkspaceException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Linux process-identity check used before recovering an unreachable worker. */
final class WorkerProcessProbe {
    private static final int MAX_COMMAND_LINE_BYTES = 64 * 1024;
    private final Path procRoot;

    WorkerProcessProbe() {
        this(Paths.get("/proc"));
    }

    WorkerProcessProbe(Path procRoot) {
        this.procRoot = procRoot;
    }

    void requireMatchingWorkerStopped(WorkerEndpoint endpoint,
                                      UUID workspaceId,
                                      Path workspace) throws WorkspaceException {
        if (endpoint.pid() <= 0) {
            throw new WorkspaceException("Cannot reconcile worker without a valid PID");
        }
        if (!Files.isDirectory(procRoot)) {
            throw new WorkspaceException("Automatic worker reconciliation requires Linux /proc");
        }
        Path process = procRoot.resolve(Long.toString(endpoint.pid()));
        if (!Files.exists(process)) {
            return;
        }

        try {
            if (isZombie(process.resolve("stat"))) {
                return;
            }
            List<String> command = readCommandLine(process.resolve("cmdline"));
            if (command.isEmpty()) {
                throw new WorkspaceException("Cannot prove whether worker PID "
                        + endpoint.pid() + " is still running");
            }
            if (isMatchingWorker(command, workspaceId, workspace)) {
                throw new WorkspaceException("Recorded sandbox worker PID " + endpoint.pid()
                        + " is still running but its control endpoint is unreachable");
            }
            // The PID was reused by another command, so the recorded worker is gone.
        } catch (NoSuchFileException e) {
            if (Files.exists(process)) {
                throw new WorkspaceException("Cannot inspect recorded worker PID "
                        + endpoint.pid(), e);
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect recorded worker PID "
                    + endpoint.pid(), e);
        }
    }

    private static boolean isZombie(Path stat) throws IOException, WorkspaceException {
        String value = new String(Files.readAllBytes(stat), StandardCharsets.US_ASCII);
        int commandEnd = value.lastIndexOf(") ");
        if (commandEnd < 0 || commandEnd + 2 >= value.length()) {
            throw new WorkspaceException("Cannot parse worker process state from " + stat);
        }
        return value.charAt(commandEnd + 2) == 'Z';
    }

    private static List<String> readCommandLine(Path path) throws IOException,
            WorkspaceException {
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length > MAX_COMMAND_LINE_BYTES) {
            throw new WorkspaceException("Worker process command line exceeds safety limit");
        }
        List<String> arguments = new ArrayList<>();
        int start = 0;
        for (int index = 0; index <= bytes.length; index++) {
            if (index == bytes.length || bytes[index] == 0) {
                if (index > start) {
                    arguments.add(new String(bytes, start, index - start,
                            StandardCharsets.UTF_8));
                }
                start = index + 1;
            }
        }
        return arguments;
    }

    private static boolean isMatchingWorker(List<String> command,
                                            UUID workspaceId,
                                            Path workspace) {
        return command.contains(ChildProcessLauncher.WORKER_MAIN)
                && command.contains("--sandbox")
                && optionEquals(command, "--workspace-id", workspaceId.toString())
                && optionEquals(command, "--workspace", workspace.toString());
    }

    private static boolean optionEquals(List<String> command, String option, String value) {
        for (int index = 0; index + 1 < command.size(); index++) {
            if (option.equals(command.get(index)) && value.equals(command.get(index + 1))) {
                return true;
            }
        }
        return false;
    }
}
