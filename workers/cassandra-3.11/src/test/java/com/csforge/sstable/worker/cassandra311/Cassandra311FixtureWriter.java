package com.csforge.sstable.worker.cassandra311;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/** Generates pinned-release integration fixtures through Cassandra's offline writer. */
public final class Cassandra311FixtureWriter {
    private Cassandra311FixtureWriter() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected output directory and timestamp");
        }
        Path output = Paths.get(args[0]).toAbsolutePath().normalize();
        Files.createDirectories(output);
        long timestampMicros = Long.parseLong(args[1]);
        String schema = "CREATE TABLE future_fixture.users ("
                + "user_name text PRIMARY KEY, password text)";
        String insert = "INSERT INTO future_fixture.users (user_name, password) "
                + "VALUES (?, ?) USING TIMESTAMP ?";
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(output.toFile())
                .forTable(schema)
                .using(insert)
                .build()) {
            writer.addRow("future", "future-base", timestampMicros);
        }
    }
}
