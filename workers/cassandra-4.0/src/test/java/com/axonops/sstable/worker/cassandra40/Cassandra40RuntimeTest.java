package com.axonops.sstable.worker.cassandra40;

import com.axonops.sstable.bootstrap.AdapterMetadata;
import com.axonops.sstable.bootstrap.BootstrapException;
import com.axonops.sstable.bootstrap.CassandraVersion;
import com.axonops.sstable.worker.api.DirectSandboxRuntimeAdapter;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Cassandra40RuntimeTest {
    @BeforeClass
    public static void initializeCassandraFormatRegistry() {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra40Runtime runtime = new Cassandra40Runtime();

        Assert.assertEquals("4.0.0", runtime.installedVersion());
        Assert.assertTrue(runtime instanceof DirectSandboxRuntimeAdapter);
        runtime.verifyLinkage();
    }

    @Test
    public void adapterMetadataAcceptsEveryReleasedPatchInThe40Line() throws Exception {
        AdapterMetadata metadata = AdapterMetadata.loadRequired(
                Cassandra40RuntimeTest.class.getClassLoader());

        for (int patch = 0; patch <= 18; patch++) {
            metadata.validate(CassandraVersion.parse("4.0." + patch), 11);
        }
        assertUnsupportedRuntime(metadata, "3.11.19");
        assertUnsupportedRuntime(metadata, "4.0.19");
        assertUnsupportedRuntime(metadata, "4.1.0");
    }

    @Test
    public void acceptsDocumentedBigFormatMigrationRange() {
        for (String version : new String[] {"ma", "mb", "mc", "md", "me", "na", "nb"}) {
            Assert.assertEquals(version,
                    Cassandra40Importer.requireCompatibleVersion(version, "big").toString());
        }
    }

    @Test
    public void rejectsFormatsOutsideTheDocumentedMigrationRange() {
        assertUnsupported("lz", "big");
        assertUnsupported("nc", "big");
        assertUnsupported("ma", "bti");
    }

    @Test
    public void preservesSourceSstableGenerationWhenStagingIntoTheWorkspace() {
        Descriptor source = new Descriptor(
                Cassandra40Importer.requireCompatibleVersion("nb", "big"),
                new File("/source/system/local"), "system", "local", 41,
                org.apache.cassandra.io.sstable.format.SSTableFormat.Type.BIG);

        Descriptor staged = Cassandra40Importer.workspaceDescriptor(source,
                Paths.get("/workspace/data/system/local"), "system", "local");

        Assert.assertEquals(41, staged.generation);
        Assert.assertEquals("nb-41-big", new File(staged.baseFilename()).getName());
    }

    @Test
    public void advancesGenerationCounterPastImportedSourceGeneration() {
        AtomicInteger generator = new AtomicInteger(7);

        Cassandra40Importer.advanceFileIndexGenerator(generator, 41);

        Assert.assertEquals(42, generator.incrementAndGet());
    }

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra40Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 4.0"));
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
