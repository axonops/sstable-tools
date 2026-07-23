package com.axonops.sstable.worker.cassandra50;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
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

        Assert.assertEquals("5.0.4", runtime.installedVersion());
        runtime.verifyLinkage();
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

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra50Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 5.0"));
        }
    }
}
