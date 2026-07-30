package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.WorkspaceException;
import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Requires an explicit acknowledgement before publishing into a live Cassandra directory. */
final class LiveCassandraPublicationPolicy {
    private final Path procRoot;
    private final PrintStream err;
    private final ConfirmationReader confirmationReader;
    private final Set<String> acknowledgedMatches = new HashSet<>();

    LiveCassandraPublicationPolicy(PrintStream err) {
        this(Paths.get("/proc"), err, systemConfirmationReader());
    }

    LiveCassandraPublicationPolicy(Path procRoot,
                                   PrintStream err,
                                   ConfirmationReader confirmationReader) {
        this.procRoot = procRoot;
        this.err = err;
        this.confirmationReader = confirmationReader;
    }

    void requireSafe(List<Path> sources, boolean allowLiveOutput)
            throws WorkspaceException {
        handle(LiveCassandraDirectoryDetector.findCanonical(
                LiveCassandraDirectoryDetector.canonicalDirectories(sources), procRoot),
                allowLiveOutput);
    }

    private void handle(LiveCassandraDirectoryDetector.Match match, boolean allowLiveOutput)
            throws WorkspaceException {
        if (match == null || acknowledgedMatches.contains(match.key())) {
            return;
        }
        if (allowLiveOutput) {
            printWarning(match);
            err.println("Continuing because --allow-live-cassandra-output was supplied.");
            acknowledgedMatches.add(match.key());
            return;
        }
        if (confirmationReader == null) {
            throw new WorkspaceException(match.rejectionMessage()
                    + " No interactive terminal is available; to acknowledge the risk and "
                    + "continue, supply --allow-live-cassandra-output.");
        }

        printWarning(match);
        err.print("Type 'yes' to continue, or anything else to abort: ");
        err.flush();
        final String response;
        try {
            response = confirmationReader.readLine();
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read live Cassandra output confirmation", e);
        }
        if (response == null || !"yes".equalsIgnoreCase(response.trim())) {
            throw new WorkspaceException("Live Cassandra output confirmation was not accepted");
        }
        err.println("Live Cassandra output risk acknowledged; continuing.");
        acknowledgedMatches.add(match.key());
    }

    private void printWarning(LiveCassandraDirectoryDetector.Match match) {
        err.println("WARNING: LIVE CASSANDRA OUTPUT DIRECTORY DETECTED");
        err.println(match.description() + ".");
        err.println("Existing SSTables are immutable and safe to read, but publishing new");
        err.println("SSTables into a running node's live table directory can damage the node.");
    }

    private static ConfirmationReader systemConfirmationReader() {
        final Console console = System.console();
        return console == null ? null : new ConfirmationReader() {
            @Override
            public String readLine() {
                return console.readLine();
            }
        };
    }

    interface ConfirmationReader {
        String readLine() throws IOException;
    }
}
