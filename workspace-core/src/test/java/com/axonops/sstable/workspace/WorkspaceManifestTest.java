package com.axonops.sstable.workspace;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceManifestTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void strictCodecRoundTripsManifest() throws Exception {
        Path root = temporary.newFolder("roundtrip").toPath();
        WorkspaceManifest manifest = WorkspaceManifest.create(
                UUID.fromString("20a0d99c-f07a-4ef3-8999-e063aad5c183"),
                Instant.parse("2026-07-15T10:00:00Z"),
                WorkspaceTestFixtures.inventory(root));
        WorkspaceManifestCodec codec = new WorkspaceManifestCodec();

        WorkspaceManifest decoded = codec.decode(codec.encode(manifest));

        Assert.assertEquals(manifest, decoded);
    }

    @Test
    public void rejectsMalformedAndUnknownManifestFields() throws Exception {
        WorkspaceManifestCodec codec = new WorkspaceManifestCodec();
        assertDecodeFailure(codec, "{not json", "Malformed workspace manifest JSON");
        assertDecodeFailure(codec, "{}", "Missing manifest field");

        Path root = temporary.newFolder("unknown").toPath();
        String valid = new String(codec.encode(WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root))), "UTF-8");
        String unknown = valid.replaceFirst("\\{", "{\n  \"unexpected\": true,");
        assertDecodeFailure(codec, unknown, "Unknown manifest field");
    }

    @Test
    public void rejectsSourcePathTraversalInManifest() throws Exception {
        Path root = temporary.newFolder("path-attack").toPath();
        WorkspaceManifestCodec codec = new WorkspaceManifestCodec();
        WorkspaceManifest manifest = WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root));
        String valid = new String(codec.encode(manifest), "UTF-8");
        String componentPath = manifest.sourceInventory().sets().get(0)
                .components().get(0).path().toString();
        String attacked = valid.replace(componentPath.replace("\\", "\\\\"),
                "/tmp/../etc/passwd");

        assertDecodeFailure(codec, attacked, "absolute and normalized");
    }

    @Test
    public void validatesTransitionsAndFailureRecovery() throws Exception {
        Path root = temporary.newFolder("transitions").toPath();
        Instant start = Instant.parse("2026-07-15T10:00:00Z");
        WorkspaceManifest manifest = WorkspaceManifest.create(UUID.randomUUID(), start,
                WorkspaceTestFixtures.inventory(root));

        WorkspaceManifest validated = manifest.transitionTo(WorkspaceState.VALIDATED,
                start.plusSeconds(1));
        WorkspaceManifest failed = validated.fail("interrupted import", start.plusSeconds(2));
        WorkspaceManifest recovered = failed.recover(start.plusSeconds(3));

        Assert.assertEquals(WorkspaceState.FAILED_RECOVERABLE, failed.state());
        Assert.assertEquals(WorkspaceState.VALIDATED, failed.lastStableState());
        Assert.assertEquals(RecoveryAction.RECOVER_LAST_STABLE_STATE,
                failed.recoveryAction());
        Assert.assertEquals(WorkspaceState.VALIDATED, recovered.state());
        Assert.assertNull(recovered.failureMessage());

        try {
            manifest.transitionTo(WorkspaceState.RUNNING, start.plusSeconds(1));
            Assert.fail("Expected invalid transition");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("NEW -> RUNNING"));
        }

        for (WorkspaceState state : WorkspaceState.values()) {
            Assert.assertNotNull(state.recoveryAction());
            Assert.assertFalse(state.recoveryAction().description().isEmpty());
        }
    }

    @Test
    public void failedRunningWorkspaceCanRecoverAsStoppedAfterReconciliation()
            throws Exception {
        Path root = temporary.newFolder("worker-recovery").toPath();
        WorkspaceManifest manifest = WorkspaceManifest.create(
                        WorkspaceTestFixtures.inventory(root))
                .transitionTo(WorkspaceState.VALIDATED)
                .transitionTo(WorkspaceState.IMPORTED)
                .transitionTo(WorkspaceState.RUNNING)
                .fail("worker terminated");

        WorkspaceManifest recovered = manifest.recoverTo(WorkspaceState.STOPPED);

        Assert.assertEquals(WorkspaceState.STOPPED, recovered.state());
        try {
            manifest.recoverTo(WorkspaceState.VALIDATED);
            Assert.fail("Expected unsafe recovery target to be rejected");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot recover"));
        }
    }

    @Test
    public void rejectsIncompleteDecodedInventoryAndUnsafeOwnedPaths() throws Exception {
        Path root = temporary.newFolder("manifest-invariants").toPath();
        WorkspaceManifestCodec codec = new WorkspaceManifestCodec();
        String valid = new String(codec.encode(WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root))), "UTF-8");

        assertDecodeFailure(codec, valid.replace("Statistics.db", "Summary.db"),
                "Incomplete component inventory");

        String hash = "0123456789abcdef0123456789abcdef"
                + "0123456789abcdef0123456789abcdef";
        Assert.assertEquals("exports/part..name.db",
                new ManifestFile("exports/part..name.db", 1, hash).relativePath());
        assertManifestFileFailure("../outside.db", hash);
        assertManifestFileFailure("C:\\outside.db", hash);
        assertManifestFileFailure("\\\\server\\share.db", hash);
    }

    @Test
    public void recordsImmutableSchemaAndBaselineImportIdentity() throws Exception {
        Path root = temporary.newFolder("import-identity").toPath();
        Map<String, String> bundle = new LinkedHashMap<>();
        bundle.put("bundle.sha256", hash('a'));
        WorkspaceManifest validated = WorkspaceManifest.create(
                        WorkspaceTestFixtures.inventory(root))
                .withSchemaIdentity(bundle)
                .transitionTo(WorkspaceState.VALIDATED);
        Map<String, String> parsed = new LinkedHashMap<>();
        parsed.put("table", "users");
        List<ManifestFile> baseline = Arrays.asList(
                new ManifestFile("data/ks/table/mc-2-big-Data.db", 2, hash('b')),
                new ManifestFile("data/ks/table/mc-1-big-Data.db", 1, hash('c')));

        WorkspaceManifest imported = validated.withImportResult(parsed, baseline)
                .transitionTo(WorkspaceState.IMPORTED);
        WorkspaceManifest roundTripped = new WorkspaceManifestCodec().decode(
                new WorkspaceManifestCodec().encode(imported));

        Assert.assertEquals("users", imported.schemaIdentity().get("table"));
        Assert.assertEquals("data/ks/table/mc-1-big-Data.db",
                imported.baselineInventory().get(0).relativePath());
        Assert.assertEquals(imported, roundTripped);

        try {
            validated.withImportResult(Collections.singletonMap("bundle.sha256", hash('d')),
                    baseline);
            Assert.fail("Expected schema identity change to be rejected");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("cannot change"));
        }
    }

    @Test
    public void recordsImmutableOutputFormatBeforeRuntimeSelection() throws Exception {
        Path root = temporary.newFolder("output-format").toPath();
        WorkspaceManifest manifest = WorkspaceManifest.create(
                WorkspaceTestFixtures.inventory(root)).withOutputIdentity(
                Collections.singletonMap("sstable.format", "bti"));
        WorkspaceManifest withRuntime = manifest.withRuntimeIdentity(
                Collections.singletonMap("runtime.release", "5.0"),
                outputIdentity("bti"));

        Assert.assertEquals("bti", withRuntime.outputIdentity().get("sstable.format"));
        Assert.assertEquals("refresh-v2", withRuntime.outputIdentity().get("import.contract"));
        try {
            withRuntime.withOutputIdentity(Collections.singletonMap("sstable.format", "big"));
            Assert.fail("Expected output format change to be rejected");
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("output identity entry cannot change"));
        }
    }

    @Test
    public void recordsAndRoundTripsPublishedExportIdentity() throws Exception {
        Path root = temporary.newFolder("export-record").toPath().toRealPath();
        WorkspaceManifest flushed = WorkspaceManifest.create(
                        WorkspaceTestFixtures.inventory(root))
                .transitionTo(WorkspaceState.VALIDATED)
                .transitionTo(WorkspaceState.IMPORTED)
                .transitionTo(WorkspaceState.RUNNING)
                .transitionTo(WorkspaceState.FLUSHED);
        Path output = root.resolveSibling("published-export").toAbsolutePath().normalize();
        ExportRecord record = new ExportRecord(UUID.randomUUID(), Instant.now(), "delta",
                output, Collections.singletonList(
                new ManifestFile("export-manifest.json", 10, hash('e'))));

        WorkspaceManifest exported = flushed.withExport(record)
                .transitionTo(WorkspaceState.EXPORTED);
        WorkspaceManifest restored = new WorkspaceManifestCodec().decode(
                new WorkspaceManifestCodec().encode(exported));

        Assert.assertEquals(exported, restored);
        Assert.assertEquals(output, restored.exports().get(0).outputPath());
    }

    private static String hash(char value) {
        char[] hash = new char[64];
        Arrays.fill(hash, value);
        return new String(hash);
    }

    private static Map<String, String> outputIdentity(String format) {
        Map<String, String> identity = new LinkedHashMap<>();
        identity.put("sstable.format", format);
        identity.put("import.contract", "refresh-v2");
        return identity;
    }

    private static void assertManifestFileFailure(String path, String hash) throws Exception {
        try {
            new ManifestFile(path, 1, hash);
            Assert.fail("Expected unsafe manifest path failure for " + path);
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage().contains("Unsafe manifest relative path"));
        }
    }

    private static void assertDecodeFailure(WorkspaceManifestCodec codec,
                                            String json,
                                            String expected) throws Exception {
        try {
            codec.decode(json);
            Assert.fail("Expected decode failure containing: " + expected);
        } catch (WorkspaceException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expected));
        }
    }
}
