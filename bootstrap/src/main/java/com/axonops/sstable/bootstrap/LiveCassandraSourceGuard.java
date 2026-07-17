package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.SstableSet;
import com.axonops.sstable.workspace.WorkspaceException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Rejects sources observably owned by a running Cassandra daemon on Linux. */
final class LiveCassandraSourceGuard {
    private static final String CASSANDRA_DAEMON =
            "org.apache.cassandra.service.CassandraDaemon";
    private static final String STORAGE_DIRECTORY_PROPERTY = "-Dcassandra.storagedir=";

    private LiveCassandraSourceGuard() {
    }

    static void reject(List<Path> sourceDirectories) throws WorkspaceException {
        reject(canonicalDirectories(sourceDirectories), Paths.get("/proc"));
    }

    static void reject(SourceInventory inventory) throws WorkspaceException {
        Set<Path> directories = new LinkedHashSet<>();
        for (SstableSet set : inventory.sets()) {
            directories.add(set.directory());
        }
        reject(new ArrayList<>(directories), Paths.get("/proc"));
    }

    static void reject(List<Path> canonicalSources, Path procRoot)
            throws WorkspaceException {
        if (!Files.isDirectory(procRoot, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (DirectoryStream<Path> processes = Files.newDirectoryStream(procRoot)) {
            for (Path process : processes) {
                if (process.getFileName().toString().matches("[0-9]+")) {
                    inspectProcess(canonicalSources, process);
                }
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inspect active processes for live Cassandra "
                    + "SSTable ownership under " + procRoot, e);
        }
    }

    private static List<Path> canonicalDirectories(List<Path> directories)
            throws WorkspaceException {
        List<Path> result = new ArrayList<>();
        for (Path directory : directories) {
            try {
                Path canonical = directory.toRealPath();
                if (!Files.isDirectory(canonical, LinkOption.NOFOLLOW_LINKS)) {
                    throw new WorkspaceException("SSTable source is not a canonical directory: "
                            + directory);
                }
                result.add(canonical);
            } catch (IOException e) {
                throw new WorkspaceException("Cannot resolve SSTable source " + directory, e);
            }
        }
        return result;
    }

    private static void inspectProcess(List<Path> sources, Path process)
            throws WorkspaceException {
        List<String> arguments = readArguments(process.resolve("cmdline"));
        if (!arguments.contains(CASSANDRA_DAEMON)) {
            return;
        }
        String pid = process.getFileName().toString();
        for (String argument : arguments) {
            if (argument.startsWith(STORAGE_DIRECTORY_PROPERTY)) {
                String value = argument.substring(STORAGE_DIRECTORY_PROPERTY.length());
                Path storage = processPath(process, value);
                if (storage == null) {
                    continue;
                }
                for (Path source : sources) {
                    if (source.startsWith(storage)) {
                        reject(source, pid, "storage directory " + storage);
                    }
                }
            }
        }
        inspectOpenFiles(sources, process, pid);
        inspectMappedFiles(sources, process, pid);
    }

    private static List<String> readArguments(Path commandLine) {
        List<String> result = new ArrayList<>();
        try {
            byte[] bytes = Files.readAllBytes(commandLine);
            int start = 0;
            for (int index = 0; index <= bytes.length; index++) {
                if (index == bytes.length || bytes[index] == 0) {
                    if (index > start) {
                        result.add(new String(bytes, start, index - start,
                                StandardCharsets.UTF_8));
                    }
                    start = index + 1;
                }
            }
        } catch (IOException | SecurityException ignored) {
            // Processes outside the current user's visibility cannot be classified here.
        }
        return result;
    }

    private static Path processPath(Path process, String value) {
        final Path configured;
        try {
            configured = Paths.get(value);
        } catch (InvalidPathException invalid) {
            return null;
        }
        Path path = configured;
        if (!path.isAbsolute()) {
            try {
                Path cwd = Files.readSymbolicLink(process.resolve("cwd"));
                path = cwd.resolve(path);
            } catch (IOException | SecurityException ignored) {
                path = path.toAbsolutePath();
            }
        }
        return canonicalIfPossible(path);
    }

    private static void inspectOpenFiles(List<Path> sources, Path process, String pid)
            throws WorkspaceException {
        Path descriptors = process.resolve("fd");
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(descriptors)) {
            for (Path entry : entries) {
                try {
                    Path target = canonicalIfPossible(cleanDeletedSuffix(
                            Files.readSymbolicLink(entry)));
                    rejectMatchingSource(sources, target, pid, "open file " + target);
                } catch (IOException | SecurityException ignored) {
                    // File descriptors can disappear while the process is being inspected.
                }
            }
        } catch (IOException | SecurityException ignored) {
            // The storage-directory property and memory maps remain independent evidence.
        }
    }

    private static void inspectMappedFiles(List<Path> sources, Path process, String pid)
            throws WorkspaceException {
        try (BufferedReader maps = Files.newBufferedReader(process.resolve("maps"),
                StandardCharsets.UTF_8)) {
            String line;
            while ((line = maps.readLine()) != null) {
                int pathStart = line.indexOf('/');
                if (pathStart < 0) {
                    continue;
                }
                try {
                    Path mapped = canonicalIfPossible(cleanDeletedSuffix(Paths.get(
                            decodeProcPath(line.substring(pathStart)))));
                    rejectMatchingSource(sources, mapped, pid, "mapped file " + mapped);
                } catch (InvalidPathException ignored) {
                    // Ignore an unrepresentable transient mapping path.
                }
            }
        } catch (IOException | SecurityException ignored) {
            // A process can exit or restrict maps while it is being inspected.
        }
    }

    private static String decodeProcPath(String value) {
        return value.replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\012", "\n")
                .replace("\\134", "\\");
    }

    private static Path cleanDeletedSuffix(Path path) {
        String value = path.toString();
        String suffix = " (deleted)";
        return value.endsWith(suffix)
                ? Paths.get(value.substring(0, value.length() - suffix.length())) : path;
    }

    private static Path canonicalIfPossible(Path path) {
        try {
            return path.toRealPath();
        } catch (IOException | SecurityException ignored) {
            return path.toAbsolutePath().normalize();
        }
    }

    private static void rejectMatchingSource(List<Path> sources,
                                             Path observed,
                                             String pid,
                                             String evidence) throws WorkspaceException {
        for (Path source : sources) {
            if (observed.startsWith(source)) {
                reject(source, pid, evidence);
            }
        }
    }

    private static void reject(Path source, String pid, String evidence)
            throws WorkspaceException {
        throw new WorkspaceException("Refusing SSTable source " + source
                + " because active Cassandra process PID " + pid + " matches " + evidence
                + ". Stop the owning Cassandra process, or copy a completed snapshot or "
                + "backup outside every live Cassandra data directory.");
    }
}
