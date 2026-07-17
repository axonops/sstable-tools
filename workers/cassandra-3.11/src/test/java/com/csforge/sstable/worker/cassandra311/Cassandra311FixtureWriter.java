package com.csforge.sstable.worker.cassandra311;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/** Generates pinned-release integration fixtures through Cassandra's offline writer. */
public final class Cassandra311FixtureWriter {
    static final String SHAPE_TYPE = "CREATE TYPE shape_fixture.address ("
            + "street text, zip int)";
    static final String SHAPE_TABLE = "CREATE TABLE shape_fixture.items ("
            + "tenant text, item_id int, category text static, name text, "
            + "tags set<text>, scores list<int>, attrs map<text, text>, "
            + "location frozen<address>, pair tuple<text, int>, expiring text, "
            + "PRIMARY KEY (tenant, item_id))";

    private Cassandra311FixtureWriter() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Expected fixture kind and output directory");
        }
        Path output = Paths.get(args[1]).toAbsolutePath().normalize();
        Files.createDirectories(output);
        if ("future".equals(args[0]) && args.length == 3) {
            writeFuture(output, Long.parseLong(args[2]));
        } else if ("shapes".equals(args[0]) && args.length == 2) {
            writeShapes(output);
        } else {
            throw new IllegalArgumentException("Unsupported fixture arguments");
        }
    }

    private static void writeFuture(Path output, long timestampMicros) throws Exception {
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

    private static void writeShapes(Path output) throws Exception {
        String insert = "INSERT INTO shape_fixture.items (tenant, item_id, category, "
                + "name, tags, scores, attrs, location, pair, expiring) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(output.toFile())
                .withType(SHAPE_TYPE)
                .forTable(SHAPE_TABLE)
                .using(insert)
                .build()) {
            Set<String> tags = new LinkedHashSet<>(Arrays.asList("seed", "source"));
            Map<String, String> attrs = new LinkedHashMap<>();
            attrs.put("origin", "offline-writer");
            UDTValue address = writer.getUDType("address").newValue()
                    .setString("street", "Source Street")
                    .setInt("zip", 100);
            TupleType pairType = TupleType.of(ProtocolVersion.NEWEST_SUPPORTED,
                    CodecRegistry.DEFAULT_INSTANCE, DataType.text(), DataType.cint());
            TupleValue pair = pairType.newValue("source-pair", 1);
            writer.addRow("base", 0, "source-category", "source-name", tags,
                    Arrays.asList(1, 2), attrs, address, pair, "source-expiring");
        }
    }
}
