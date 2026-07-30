package com.axonops.sstable.worker.cassandra41;

import com.axonops.sstable.bootstrap.AdapterMetadata;
import com.axonops.sstable.bootstrap.BootstrapException;
import com.axonops.sstable.bootstrap.CassandraVersion;
import com.axonops.sstable.worker.api.DirectSandboxRuntimeAdapter;
import java.io.File;
import java.nio.file.Paths;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId.Builder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Cassandra41RuntimeTest {
    @BeforeClass
    public static void initializeCassandraFormatRegistry() {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra41Runtime runtime = new Cassandra41Runtime();

        Assert.assertEquals("4.1.11", runtime.installedVersion());
        Assert.assertTrue(runtime instanceof DirectSandboxRuntimeAdapter);
        runtime.verifyLinkage();
    }

    @Test
    public void adapterMetadataAcceptsEveryReleasedPatchInThe41Line() throws Exception {
        AdapterMetadata metadata = AdapterMetadata.loadRequired(
                Cassandra41RuntimeTest.class.getClassLoader());

        for (int patch = 0; patch <= 11; patch++) {
            metadata.validate(CassandraVersion.parse("4.1." + patch), 11);
        }
        assertUnsupportedRuntime(metadata, "4.0.18");
        assertUnsupportedRuntime(metadata, "4.1.12");
        assertUnsupportedRuntime(metadata, "5.0.0");
    }

    @Test
    public void acceptsDocumentedBigFormatMigrationRange() {
        for (String version : new String[] {"ma", "mb", "mc", "md", "me", "na", "nb"}) {
            Assert.assertEquals(version,
                    Cassandra41Importer.requireCompatibleVersion(version, "big").toString());
        }
    }

    @Test
    public void rejectsFormatsOutsideTheDocumentedMigrationRange() {
        assertUnsupported("lz", "big");
        assertUnsupported("nc", "big");
        assertUnsupported("ma", "bti");
    }

    @Test
    public void preservesSourceSstableIdWhenStagingIntoTheWorkspace() {
        Descriptor source = new Descriptor(
                Cassandra41Importer.requireCompatibleVersion("nb", "big"),
                new org.apache.cassandra.io.util.File("/source/system/local"),
                "system", "local", new SequenceBasedSSTableId(41),
                org.apache.cassandra.io.sstable.format.SSTableFormat.Type.BIG);

        Descriptor staged = Cassandra41Importer.workspaceDescriptor(source,
                Paths.get("/workspace/data/system/local"), "system", "local");

        Assert.assertEquals(source.id, staged.id);
        Assert.assertEquals("nb-41-big", new File(staged.baseFilename()).getName());
    }

    @Test
    public void advancesSequenceGeneratorPastImportedSourceIdentifier() throws Exception {
        java.util.function.Supplier<SequenceBasedSSTableId> generator =
                Builder.instance.generator(java.util.stream.Stream.of(
                        new SequenceBasedSSTableId(7)));

        Cassandra41Importer.advanceSequenceGenerator(generator, 41);

        Assert.assertEquals(42, generator.get().generation);
    }

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra41Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 4.1"));
        }
    }

    private static void assertUnsupportedRuntime(AdapterMetadata metadata, String version)
            throws Exception {
        try {
            metadata.validate(CassandraVersion.parse(version), 11);
            Assert.fail("Expected Cassandra " + version + " to be rejected");
        } catch (BootstrapException expected) {
            Assert.assertEquals(BootstrapException.COMPATIBILITY_EXIT_CODE,
                    expected.exitCode());
        }
    }
}
