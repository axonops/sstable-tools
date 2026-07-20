package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.ExportRecord;
import com.axonops.sstable.workspace.ManifestFile;
import com.axonops.sstable.workspace.SchemaBundle;
import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.WorkspaceException;
import com.axonops.sstable.workspace.WorkspaceFileInventory;
import com.axonops.sstable.workspace.WorkspaceFlushResult;
import com.axonops.sstable.workspace.WorkspaceLock;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import com.axonops.sstable.workspace.WorkspaceState;
import com.axonops.sstable.workspace.WorkspaceVerificationResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceExportPublisherTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void atomicallyPublishesAndReconcilesDeltaAndSnapshot() throws Exception {
        Fixture fixture = createFixture();
        WorkspaceExportPublisher publisher = new WorkspaceExportPublisher();
        Path deltaOutput = temporary.getRoot().toPath().resolve("delta-export");

        ExportRecord delta = publisher.publish(fixture.repository, fixture.manifest,
                fixture.flush, fixture.verification, ExportMode.DELTA, deltaOutput);
        Assert.assertEquals("delta", delta.outputFormat());
        Assert.assertEquals(7, delta.files().size());
        Assert.assertTrue(Files.isRegularFile(
                deltaOutput.resolve(".sstable-tools-export")));
        Assert.assertTrue(Files.isRegularFile(deltaOutput.resolve("export-manifest.json")));
        Assert.assertTrue(Files.isRegularFile(
                deltaOutput.resolve("sstables/mc-2-big-Data.db")));
        String deltaManifest = new String(Files.readAllBytes(
                deltaOutput.resolve("export-manifest.json")), StandardCharsets.UTF_8);
        Assert.assertTrue(deltaManifest.contains("\"requiredSources\""));
        Assert.assertTrue(deltaManifest.contains(fixture.source.toString()));
        Assert.assertEquals(delta, publisher.publish(fixture.repository, fixture.manifest,
                fixture.flush, fixture.verification, ExportMode.DELTA, deltaOutput));

        Path snapshotOutput = temporary.getRoot().toPath().resolve("snapshot-export");
        ExportRecord snapshot = publisher.publish(fixture.repository, fixture.manifest,
                fixture.flush, fixture.verification, ExportMode.SNAPSHOT, snapshotOutput);
        Assert.assertEquals(11, snapshot.files().size());
        Assert.assertTrue(Files.isRegularFile(
                snapshotOutput.resolve(".sstable-tools-export")));
        Assert.assertTrue(Files.isRegularFile(
                snapshotOutput.resolve("sstables/mc-1-big-Data.db")));
        String snapshotManifest = new String(Files.readAllBytes(
                snapshotOutput.resolve("export-manifest.json")), StandardCharsets.UTF_8);
        Assert.assertTrue(snapshotManifest.contains("\"requiredSources\": []"));
    }

    @Test
    public void refusesToAdoptCorruptOrOverlappingOutput() throws Exception {
        Fixture fixture = createFixture();
        WorkspaceExportPublisher publisher = new WorkspaceExportPublisher();
        Path output = temporary.getRoot().toPath().resolve("corrupt-export");
        publisher.publish(fixture.repository, fixture.manifest, fixture.flush,
                fixture.verification, ExportMode.DELTA, output);
        Files.write(output.resolve("sstables/mc-2-big-Data.db"), new byte[]{99});

        assertPublishFailure(publisher, fixture, output, "does not match");
        assertPublishFailure(publisher, fixture,
                fixture.repository.root().resolve("exports/unsafe"), "overlaps the workspace");

        Path stagedOutput = temporary.getRoot().toPath().resolve("staged-export");
        ExportRecord stagedRecord = publisher.publish(fixture.repository, fixture.manifest,
                fixture.flush, fixture.verification, ExportMode.DELTA, stagedOutput);
        Path staging = stagedOutput.getParent().resolve("." + stagedOutput.getFileName()
                + "." + stagedRecord.exportId() + ".tmp");
        Files.move(stagedOutput, staging);
        Path unexpected = staging.resolve("do-not-delete");
        Files.write(unexpected, new byte[]{42});

        assertPublishFailure(publisher, fixture, stagedOutput, "unexpected file");
        Assert.assertTrue("Unsafe staging tree was deleted", Files.exists(staging));
        Assert.assertTrue("Unexpected staging content was deleted", Files.exists(unexpected));
    }

    @Test
    public void publishesVerifiedDeltaBesideTheExplicitSourceWithNewIdentifier()
            throws Exception {
        Fixture fixture = createFixture();
        Path conf = temporary.newFolder("conf").toPath();
        Files.write(conf.resolve("cassandra.yaml"), Collections.singletonList(
                "uuid_sstable_identifiers_enabled: false"), StandardCharsets.UTF_8);
        CassandraInstallation installation = new CassandraInstallation(null, conf, null,
                CassandraVersion.parse("3.11.19"), null, null,
                Collections.<Path>emptyList());

        List<String> descriptors = new WorkspaceExportPublisher().publishDeltaAdjacent(
                fixture.repository, fixture.manifest, fixture.flush, fixture.verification,
                installation);

        Assert.assertEquals(Collections.singletonList("mc-2-big"), descriptors);
        Assert.assertTrue(Files.isRegularFile(fixture.source.resolve("ma-1-big-Data.db")));
        Assert.assertTrue(Files.isRegularFile(fixture.source.resolve("mc-2-big-Data.db")));
        Assert.assertTrue(Files.isRegularFile(fixture.source.resolve("mc-2-big-TOC.txt")));
        Assert.assertFalse(Files.exists(fixture.source.resolve(".sstable-tools-mc-2-big-"
                + "Data.db.tmp")));
    }

    private static void assertPublishFailure(WorkspaceExportPublisher publisher,
                                             Fixture fixture,
                                             Path output,
                                             String expected) throws Exception {
        try {
            publisher.publish(fixture.repository, fixture.manifest, fixture.flush,
                    fixture.verification, ExportMode.DELTA, output);
            Assert.fail("Expected export failure containing " + expected);
        } catch (WorkspaceException failure) {
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(expected));
        }
    }

    private Fixture createFixture() throws Exception {
        Path source = temporary.newFolder("source").toPath().toRealPath();
        writeDescriptor(source, "ma-1-big", (byte) 7);
        SourceInventory sourceInventory = SourceInventory.capture(
                Collections.singletonList(source));
        WorkspaceRepository repository = WorkspaceRepository.createAt(
                temporary.newFolder("workspace").toPath());
        WorkspaceManifest manifest = WorkspaceManifest.create(sourceInventory);
        String tableDirectory = "data/blog/users-id";

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            byte[] schema = "CREATE KEYSPACE blog WITH replication = "
                    .concat("{'class':'SimpleStrategy','replication_factor':1};\n")
                    .concat("CREATE TABLE blog.users (id int PRIMARY KEY);\n")
                    .getBytes(StandardCharsets.UTF_8);
            repository.writeOwnedFile(lock, SchemaBundle.WORKSPACE_PATH, schema);
            Path table = Files.createDirectories(repository.root().resolve(tableDirectory));
            writeDescriptor(table, "mc-1-big", (byte) 1);
            List<ManifestFile> baseline = WorkspaceFileInventory.capture(
                    repository.root(), tableDirectory);

            manifest = manifest.transitionTo(WorkspaceState.VALIDATED);
            repository.save(lock, manifest);
            Map<String, String> schemaIdentity = new LinkedHashMap<>();
            schemaIdentity.put("keyspace", "blog");
            schemaIdentity.put("table", "users");
            schemaIdentity.put("table.id", "00000000-0000-0000-0000-000000000001");
            schemaIdentity.put("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
            schemaIdentity.put("table.directory", tableDirectory);
            schemaIdentity.put(SourceTimestampStatus.MANIFEST_KEY, "1000");
            schemaIdentity.put(TimestampPolicy.MANIFEST_KEY, "after-source");
            manifest = manifest.withImportResult(schemaIdentity, baseline)
                    .transitionTo(WorkspaceState.IMPORTED);
            repository.save(lock, manifest);
            manifest = manifest.withRuntimeIdentity(
                    Collections.singletonMap("cassandra.version", "3.11.19"),
                    Collections.singletonMap("export.contract", "test-v1"));
            repository.save(lock, manifest);
            manifest = manifest.transitionTo(WorkspaceState.RUNNING);
            repository.save(lock, manifest);

            writeDescriptor(table, "mc-2-big", (byte) 2);
            WorkspaceFlushResult flush = WorkspaceFlushResult.capture(repository.root(),
                    manifest.workspaceId(), "3.11.19", "blog", "users", tableDirectory);
            flush.writeAtomically(repository.root());
            WorkspaceVerificationResult verification = new WorkspaceVerificationResult(
                    manifest.workspaceId(), "3.11.19", "blog", "users", flush.sha256(),
                    Instant.parse("2026-07-16T12:00:00Z"), 2, 1, 2,
                    Collections.singletonList("mc"),
                    Collections.singletonList("mc-2-big"));
            verification.writeAtomically(repository.root());
            manifest = manifest.transitionTo(WorkspaceState.FLUSHED);
            repository.save(lock, manifest);
            return new Fixture(source, repository, manifest, flush, verification);
        }
    }

    private static void writeDescriptor(Path directory, String descriptor, byte value)
            throws Exception {
        String toc = "Data.db\nDigest.crc32\nStatistics.db\nTOC.txt\n";
        Files.write(directory.resolve(descriptor + "-Data.db"), new byte[]{value});
        Files.write(directory.resolve(descriptor + "-Digest.crc32"),
                Integer.toString(value).getBytes(StandardCharsets.US_ASCII));
        Files.write(directory.resolve(descriptor + "-Statistics.db"),
                new byte[]{value, value});
        Files.write(directory.resolve(descriptor + "-TOC.txt"),
                toc.getBytes(StandardCharsets.UTF_8));
    }

    private static final class Fixture {
        private final Path source;
        private final WorkspaceRepository repository;
        private final WorkspaceManifest manifest;
        private final WorkspaceFlushResult flush;
        private final WorkspaceVerificationResult verification;

        private Fixture(Path source,
                        WorkspaceRepository repository,
                        WorkspaceManifest manifest,
                        WorkspaceFlushResult flush,
                        WorkspaceVerificationResult verification) {
            this.source = source;
            this.repository = repository;
            this.manifest = manifest;
            this.flush = flush;
            this.verification = verification;
        }
    }
}
