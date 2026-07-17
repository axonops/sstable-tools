package com.csforge.sstable.bootstrap;

import com.csforge.sstable.workspace.WorkspaceException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LiveCassandraSourceGuardTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void rejectsSourceInsideActiveCassandraStorageDirectory() throws Exception {
        Path storage = temporary.newFolder("storage").toPath().toRealPath();
        Path source = Files.createDirectories(storage.resolve("data/blog/users-id"))
                .toRealPath();
        Path proc = temporary.newFolder("proc-storage").toPath();
        writeProcess(proc, "101", Arrays.asList("java",
                "-Dcassandra.storagedir=" + storage,
                "org.apache.cassandra.service.CassandraDaemon"));

        assertRejected(source, proc, "storage directory");
    }

    @Test
    public void rejectsOpenSourceWithoutStorageDirectoryProperty() throws Exception {
        Path source = temporary.newFolder("open-source").toPath().toRealPath();
        Path component = Files.write(source.resolve("me-1-big-Data.db"),
                new byte[]{1}).toRealPath();
        Path proc = temporary.newFolder("proc-open").toPath();
        Path process = writeProcess(proc, "202", Arrays.asList("java",
                "org.apache.cassandra.service.CassandraDaemon"));
        Path descriptors = Files.createDirectories(process.resolve("fd"));
        Files.createSymbolicLink(descriptors.resolve("7"), component);

        assertRejected(source, proc, "open file");
    }

    @Test
    public void rejectsMappedSourceWithoutStorageDirectoryProperty() throws Exception {
        Path source = temporary.newFolder("mapped-source").toPath().toRealPath();
        Path component = Files.write(source.resolve("me-2-big-Data.db"),
                new byte[]{2}).toRealPath();
        Path proc = temporary.newFolder("proc-mapped").toPath();
        Path process = writeProcess(proc, "252", Arrays.asList("java",
                "org.apache.cassandra.service.CassandraDaemon"));
        Files.write(process.resolve("maps"), Collections.singletonList(
                "7f000000-7f001000 r--s 00000000 00:00 1 " + component),
                StandardCharsets.UTF_8);

        assertRejected(source, proc, "mapped file");
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

        LiveCassandraSourceGuard.reject(Collections.singletonList(source), proc);
    }

    private void assertRejected(Path source, Path proc, String evidence) throws Exception {
        try {
            LiveCassandraSourceGuard.reject(Collections.singletonList(source), proc);
            Assert.fail("Expected active Cassandra source rejection");
        } catch (WorkspaceException failure) {
            Assert.assertTrue(failure.getMessage(),
                    failure.getMessage().contains("active Cassandra process"));
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(evidence));
            Assert.assertTrue(failure.getMessage(),
                    failure.getMessage().contains("outside every live Cassandra data"));
        }
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
