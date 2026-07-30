package com.axonops.sstable.worker.cassandra50;

import com.axonops.sstable.bootstrap.AdapterMetadata;
import com.axonops.sstable.bootstrap.CassandraVersion;
import java.nio.file.Paths;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId.Builder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Cassandra50RuntimeTest {
    @BeforeClass
    public static void initializeCassandraFormatRegistry() {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra50Runtime runtime = new Cassandra50Runtime();

        Assert.assertEquals("5.0.8", runtime.installedVersion());
        runtime.verifyLinkage();
    }

    @Test
    public void adapterMetadataAcceptsDocumentedPatchRange() throws Exception {
        AdapterMetadata metadata = AdapterMetadata.loadRequired(
                Cassandra50RuntimeTest.class.getClassLoader());

        metadata.validate(CassandraVersion.parse("5.0.4"), 17);
        metadata.validate(CassandraVersion.parse("5.0.8"), 17);
    }

    @Test
    public void acceptsDocumentedBigAndBtiFormatVersions() {
        for (String version : new String[] {"ma", "mb", "mc", "md", "me", "na", "nb", "oa"}) {
            Assert.assertEquals(version,
                    Cassandra50Importer.requireCompatibleVersion(version, "big").toString());
        }
        Assert.assertEquals("da",
                Cassandra50Importer.requireCompatibleVersion("da", "bti").toString());
    }

    @Test
    public void rejectsUnsupportedFormatVersionsAndTypes() {
        assertUnsupported("lz", "big");
        assertUnsupported("ob", "big");
        assertUnsupported("db", "bti");
        assertUnsupported("da", "unknown");
    }

    @Test
    public void rejectsVectorTypesWithAnExplicitError() {
        try {
            CqlSchemaBundle.requireSupportedType(VectorType.getInstance(FloatType.instance, 3),
                    "embedding");
            Assert.fail("Expected vector type rejection");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("vector type is not supported"));
            Assert.assertTrue(expected.getMessage().contains("embedding"));
        }
    }

    @Test
    public void disablesAutomaticCompactionForTheIsolatedTableSchema() {
        Assert.assertEquals("ALTER TABLE ci_source.events WITH compaction = "
                        + "{'class': 'org.apache.cassandra.db.compaction."
                        + "SizeTieredCompactionStrategy', 'enabled': 'false'}",
                CqlSchemaBundle.disableAutomaticCompactionStatement("ci_source", "events"));
    }

    @Test
    public void preservesSourceSstableIdWhenStagingIntoTheWorkspace() {
        Descriptor source = new Descriptor(
                Cassandra50Importer.requireCompatibleVersion("da", "bti"),
                new org.apache.cassandra.io.util.File("/source/system/local"), "system", "local",
                new SequenceBasedSSTableId(41));

        Descriptor staged = Cassandra50Importer.workspaceDescriptor(source,
                Paths.get("/workspace/data/system/local"), "system", "local");

        Assert.assertEquals(source.id, staged.id);
        Assert.assertEquals("da-41-bti", staged.baseFile().name());
    }

    @Test
    public void advancesSequenceGeneratorPastImportedSourceIdentifier() throws Exception {
        java.util.function.Supplier<SequenceBasedSSTableId> generator =
                Builder.instance.generator(java.util.stream.Stream.of(
                        new SequenceBasedSSTableId(7)));

        Cassandra50Importer.advanceSequenceGenerator(generator, 41);

        Assert.assertEquals(42, generator.get().generation);
    }

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra50Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 5.0"));
        }
    }
}
