package com.axonops.sstable.worker.cassandra50;

import org.apache.cassandra.config.DatabaseDescriptor;
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

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra50Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 5.0"));
        }
    }
}
