package com.axonops.sstable.worker.cassandra40;

import org.junit.Assert;
import org.junit.Test;

public class Cassandra40RuntimeTest {
    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra40Runtime runtime = new Cassandra40Runtime();

        Assert.assertEquals("4.0.17", runtime.installedVersion());
        runtime.verifyLinkage();
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

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra40Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 4.0"));
        }
    }
}
