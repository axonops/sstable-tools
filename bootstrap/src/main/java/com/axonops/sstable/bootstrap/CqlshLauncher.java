package com.axonops.sstable.bootstrap;

import com.axonops.sstable.worker.api.WorkerEndpoint;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Launches the selected installation's stock cqlsh against one workspace. */
final class CqlshLauncher {
    private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS =
            new HashSet<>(Arrays.asList(
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_WRITE,
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OTHERS_READ,
                    PosixFilePermission.OTHERS_WRITE,
                    PosixFilePermission.OTHERS_EXECUTE));

    int run(CassandraInstallation installation,
            WorkerEndpoint endpoint,
            Path cqlshrc,
            String executeCql) throws BootstrapException {
        List<String> command = command(installation, endpoint, cqlshrc, executeCql);
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.inheritIO();
        builder.environment().put("CASSANDRA_HOME", installation.home().toString());
        if (installation.supportDirectory().isPresent()) {
            builder.environment().put("CASSANDRA_CONF",
                    installation.supportDirectory().get().toString());
        } else {
            builder.environment().remove("CASSANDRA_CONF");
        }
        builder.environment().put("PYTHONDONTWRITEBYTECODE", "1");
        builder.environment().remove("PYTHONHOME");
        builder.environment().remove("PYTHONPATH");
        builder.environment().remove("CQLSH_NO_BUNDLED");
        Process process = null;
        try {
            process = builder.start();
            return process.waitFor();
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Cannot launch installed cqlsh", e);
        } catch (InterruptedException e) {
            if (process != null && process.isAlive()) {
                process.destroy();
            }
            Thread.currentThread().interrupt();
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Interrupted while waiting for installed cqlsh", e);
        }
    }

    List<String> command(CassandraInstallation installation,
                         WorkerEndpoint endpoint,
                         Path cqlshrc,
                         String executeCql) throws BootstrapException {
        Path executable = findExecutable(installation.home());
        requireOwnerOnlyCredential(cqlshrc);
        if (endpoint.status() != WorkerEndpoint.Status.RUNNING
                || !"127.0.0.1".equals(endpoint.nativeAddress())) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Workspace cqlsh requires a running loopback native endpoint");
        }
        List<String> command = new ArrayList<>();
        command.add(executable.toString());
        command.add("--cqlshrc");
        command.add(cqlshrc.toString());
        command.add(endpoint.nativeAddress());
        command.add(Integer.toString(endpoint.nativePort()));
        if (executeCql != null) {
            command.add("-e");
            command.add(executeCql);
        }
        return Collections.unmodifiableList(command);
    }

    private static Path findExecutable(Path home) throws BootstrapException {
        List<Path> candidates = new ArrayList<>();
        candidates.add(home.resolve("bin/cqlsh"));
        Path packaged = packagedExecutable(home);
        if (packaged != null) {
            candidates.add(packaged);
        }
        for (Path candidate : candidates) {
            if (!Files.isSymbolicLink(candidate)
                    && Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)
                    && Files.isExecutable(candidate)) {
                return candidate;
            }
        }
        throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                "Selected Cassandra installation has no safe executable cqlsh; checked "
                        + candidates);
    }

    private static Path packagedExecutable(Path home) {
        Path share = home.getParent();
        Path usr = share == null ? null : share.getParent();
        if (usr == null
                || !"cassandra".equals(fileName(home))
                || !"share".equals(fileName(share))
                || !"usr".equals(fileName(usr))) {
            return null;
        }
        return usr.resolve("bin/cqlsh");
    }

    private static String fileName(Path path) {
        Path name = path.getFileName();
        return name == null ? "" : name.toString();
    }

    private static void requireOwnerOnlyCredential(Path cqlshrc) throws BootstrapException {
        if (Files.isSymbolicLink(cqlshrc)
                || !Files.isRegularFile(cqlshrc, LinkOption.NOFOLLOW_LINKS)) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Workspace cqlsh credential is missing or unsafe: " + cqlshrc);
        }
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(cqlshrc);
            if (!Collections.disjoint(permissions, FORBIDDEN_PERMISSIONS)) {
                throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                        "Workspace cqlsh credential must be owner-only: " + cqlshrc);
            }
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.CHILD_EXIT_CODE,
                    "Cannot inspect workspace cqlsh credential " + cqlshrc, e);
        } catch (UnsupportedOperationException ignored) {
            // The workspace ACL remains the boundary on non-POSIX filesystems.
        }
    }
}
