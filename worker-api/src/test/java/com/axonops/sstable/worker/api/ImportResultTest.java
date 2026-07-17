package com.axonops.sstable.worker.api;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImportResultTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void atomicallyRoundTripsSuccessfulImport() throws Exception {
        UUID workspaceId = UUID.randomUUID();
        ImportResult expected = new ImportResult(WorkerProtocol.CURRENT_VERSION,
                workspaceId, "3.11.19", "blog", "users", UUID.randomUUID(),
                "org.apache.cassandra.dht.Murmur3Partitioner",
                "data/blog/users-0123456789abcdef", 1, 1, 1, 123456789L,
                true, false);
        Path path = temporary.getRoot().toPath().resolve("state/import.properties");

        expected.writeAtomically(path);
        ImportResult actual = ImportResult.read(path);

        Assert.assertEquals(workspaceId, actual.workspaceId());
        Assert.assertEquals("blog", actual.keyspace());
        Assert.assertEquals("users", actual.table());
        Assert.assertEquals(1, actual.logicalRows());
        Assert.assertEquals(123456789L, actual.sourceMaxTimestampMicros());
        Assert.assertTrue(actual.autoCompactionDisabled());
        Assert.assertFalse(actual.nativeTransportStarted());
    }

    @Test
    public void rejectsUnsafeOrUnprovenResults() throws Exception {
        assertInvalid("../outside", true, false);
        assertInvalid("data/blog/users-id", false, false);
        assertInvalid("data/blog/users-id", true, true);

        Path malformed = temporary.newFile("malformed.properties").toPath();
        Files.write(malformed, "protocol=1\n".getBytes("UTF-8"));
        try {
            ImportResult.read(malformed);
            Assert.fail("Expected incomplete import result to fail");
        } catch (java.io.IOException e) {
            Assert.assertTrue(e.getMessage().contains("missing or unknown"));
        }
    }

    @Test
    public void rejectsNonCanonicalBooleanValues() throws Exception {
        Path path = temporary.getRoot().toPath().resolve("state/import.properties");
        ImportResult result = new ImportResult(WorkerProtocol.CURRENT_VERSION,
                UUID.randomUUID(), "3.11.19", "blog", "users", UUID.randomUUID(),
                "org.apache.cassandra.dht.Murmur3Partitioner",
                "data/blog/users-0123456789abcdef", 1, 1, 1, 123456789L,
                true, false);
        result.writeAtomically(path);
        String malformed = new String(Files.readAllBytes(path), StandardCharsets.ISO_8859_1)
                .replace("native.transport.started=false",
                        "native.transport.started=not-a-boolean");
        Files.write(path, malformed.getBytes(StandardCharsets.ISO_8859_1));

        try {
            ImportResult.read(path);
            Assert.fail("Expected malformed boolean to fail");
        } catch (java.io.IOException e) {
            Assert.assertTrue(e.getMessage().contains("must be a boolean"));
        }
    }

    @Test
    public void rejectsMalformedSourceTimestamp() throws Exception {
        Path path = temporary.getRoot().toPath().resolve("state/import.properties");
        ImportResult result = new ImportResult(WorkerProtocol.CURRENT_VERSION,
                UUID.randomUUID(), "3.11.19", "blog", "users", UUID.randomUUID(),
                "org.apache.cassandra.dht.Murmur3Partitioner",
                "data/blog/users-0123456789abcdef", 1, 1, 1, 123456789L,
                true, false);
        result.writeAtomically(path);
        String malformed = new String(Files.readAllBytes(path), StandardCharsets.ISO_8859_1)
                .replace("source.max-timestamp-micros=123456789",
                        "source.max-timestamp-micros=not-a-long");
        Files.write(path, malformed.getBytes(StandardCharsets.ISO_8859_1));

        try {
            ImportResult.read(path);
            Assert.fail("Expected malformed source timestamp to fail");
        } catch (java.io.IOException e) {
            Assert.assertTrue(e.getMessage().contains("invalid values"));
        }
    }

    private static void assertInvalid(String directory,
                                      boolean compactionDisabled,
                                      boolean nativeStarted) {
        try {
            new ImportResult(WorkerProtocol.CURRENT_VERSION, UUID.randomUUID(), "3.11.19",
                    "blog", "users", UUID.randomUUID(), "partitioner", directory,
                    1, 1, 1, 123456789L, compactionDisabled, nativeStarted);
            Assert.fail("Expected invalid import result");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Invalid import result"));
        }
    }
}
