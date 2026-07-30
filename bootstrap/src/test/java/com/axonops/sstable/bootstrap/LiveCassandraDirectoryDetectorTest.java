package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.WorkspaceException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LiveCassandraDirectoryDetectorTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void detectsSourceInsideActiveCassandraStorageDirectory() throws Exception {
        Path storage = temporary.newFolder("storage").toPath().toRealPath();
        Path source = Files.createDirectories(storage.resolve("data/blog/users-id"))
                .toRealPath();
        Path proc = temporary.newFolder("proc-storage").toPath();
        writeProcess(proc, "101", Arrays.asList("java",
                "-Dcassandra.storagedir=" + storage,
                "org.apache.cassandra.service.CassandraDaemon"));

        assertDetected(source, proc, "storage directory");
    }

    @Test
    public void detectsOpenSourceWithoutStorageDirectoryProperty() throws Exception {
        Path source = temporary.newFolder("open-source").toPath().toRealPath();
        Path component = Files.write(source.resolve("me-1-big-Data.db"),
                new byte[]{1}).toRealPath();
        Path proc = temporary.newFolder("proc-open").toPath();
        Path process = writeProcess(proc, "202", Arrays.asList("java",
                "org.apache.cassandra.service.CassandraDaemon"));
        Path descriptors = Files.createDirectories(process.resolve("fd"));
        Files.createSymbolicLink(descriptors.resolve("7"), component);

        assertDetected(source, proc, "open file");
    }

    @Test
    public void detectsMappedSourceWithoutStorageDirectoryProperty() throws Exception {
        Path source = temporary.newFolder("mapped-source").toPath().toRealPath();
        Path component = Files.write(source.resolve("me-2-big-Data.db"),
                new byte[]{2}).toRealPath();
        Path proc = temporary.newFolder("proc-mapped").toPath();
        Path process = writeProcess(proc, "252", Arrays.asList("java",
                "org.apache.cassandra.service.CassandraDaemon"));
        Files.write(process.resolve("maps"), Collections.singletonList(
                "7f000000-7f001000 r--s 00000000 00:00 1 " + component),
                StandardCharsets.UTF_8);

        assertDetected(source, proc, "mapped file");
    }

    @Test
    public void ignoresNonCassandraProcessesAndUnrelatedStorage() throws Exception {
        Path source = temporary.newFolder("stable-backup").toPath().toRealPath();
        Path proc = temporary.newFolder("proc-safe").toPath();
        writeProcess(proc, "303", Arrays.asList("java",
                "-Dcassandra.storagedir=" + temporary.newFolder("other-storage"),
                "org.apache.cassandra.service.CassandraDaemon"));
        writeProcess(proc, "404", Arrays.asList("java",
                "-Dcassandra.storagedir=" + source,
                "example.NotCassandra"));

        Assert.assertNull(LiveCassandraDirectoryDetector.findCanonical(
                Collections.singletonList(source), proc));
    }

    @Test
    public void publicationPolicyAcceptsInteractiveConfirmationOnce() throws Exception {
        Path source = temporary.newFolder("confirmed-output").toPath().toRealPath();
        Path proc = temporary.newFolder("proc-confirmed").toPath();
        writeProcess(proc, "505", Arrays.asList("java",
                "-Dcassandra.storagedir=" + source,
                "org.apache.cassandra.service.CassandraDaemon"));
        ByteArrayOutputStream warning = new ByteArrayOutputStream();
        AtomicInteger reads = new AtomicInteger();
        LiveCassandraPublicationPolicy policy = new LiveCassandraPublicationPolicy(
                proc, new PrintStream(warning), () -> {
                    reads.incrementAndGet();
                    return "yes";
                });

        policy.requireSafe(Collections.singletonList(source), false);
        policy.requireSafe(Collections.singletonList(source), false);

        Assert.assertEquals(1, reads.get());
        Assert.assertTrue(warning.toString("UTF-8").contains(
                "LIVE CASSANDRA OUTPUT DIRECTORY DETECTED"));
        Assert.assertTrue(warning.toString("UTF-8").contains("risk acknowledged"));
    }

    @Test
    public void publicationPolicyRejectsDeclinedConfirmation() throws Exception {
        Path source = temporary.newFolder("declined-output").toPath().toRealPath();
        Path proc = temporary.newFolder("proc-declined").toPath();
        writeProcess(proc, "606", Arrays.asList("java",
                "-Dcassandra.storagedir=" + source,
                "org.apache.cassandra.service.CassandraDaemon"));
        LiveCassandraPublicationPolicy policy = new LiveCassandraPublicationPolicy(
                proc, new PrintStream(new ByteArrayOutputStream()), () -> "no");

        try {
            policy.requireSafe(Collections.singletonList(source), false);
            Assert.fail("Expected declined publication confirmation");
        } catch (WorkspaceException failure) {
            Assert.assertTrue(failure.getMessage(),
                    failure.getMessage().contains("was not accepted"));
        }
    }

    @Test
    public void publicationPolicyRequiresFlagWithoutTerminal() throws Exception {
        Path source = temporary.newFolder("noninteractive-output").toPath().toRealPath();
        Path proc = temporary.newFolder("proc-noninteractive").toPath();
        writeProcess(proc, "707", Arrays.asList("java",
                "-Dcassandra.storagedir=" + source,
                "org.apache.cassandra.service.CassandraDaemon"));
        ByteArrayOutputStream warning = new ByteArrayOutputStream();
        LiveCassandraPublicationPolicy policy = new LiveCassandraPublicationPolicy(
                proc, new PrintStream(warning), null);

        try {
            policy.requireSafe(Collections.singletonList(source), false);
            Assert.fail("Expected non-interactive publication rejection");
        } catch (WorkspaceException failure) {
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(
                    "--allow-live-cassandra-output"));
        }

        policy.requireSafe(Collections.singletonList(source), true);
        Assert.assertTrue(warning.toString("UTF-8").contains(
                "--allow-live-cassandra-output was supplied"));
    }

    private void assertDetected(Path source, Path proc, String evidence) throws Exception {
        LiveCassandraDirectoryDetector.Match match =
                LiveCassandraDirectoryDetector.findCanonical(
                        Collections.singletonList(source), proc);
        Assert.assertNotNull("Expected active Cassandra directory detection", match);
        Assert.assertTrue(match.description(),
                match.description().contains("active Cassandra process"));
        Assert.assertTrue(match.description(), match.description().contains(evidence));
        Assert.assertTrue(match.rejectionMessage(),
                match.rejectionMessage().contains("outside every live Cassandra data"));
    }

    private Path writeProcess(Path proc, String pid, java.util.List<String> arguments)
            throws Exception {
        Path process = Files.createDirectories(proc.resolve(pid));
        String commandLine = String.join("\0", arguments) + "\0";
        Files.write(process.resolve("cmdline"),
                commandLine.getBytes(StandardCharsets.UTF_8));
        return process;
    }
}
