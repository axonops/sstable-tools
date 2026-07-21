package com.axonops.sstable.worker.cassandra41;

import org.junit.Assert;
import org.junit.Test;

public class Cassandra41RuntimeTest {
    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra41Runtime runtime = new Cassandra41Runtime();

        Assert.assertEquals("4.1.3", runtime.installedVersion());
        runtime.verifyLinkage();
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

    private static void assertUnsupported(String version, String format) {
        try {
            Cassandra41Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected " + version + "-" + format + " to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("Cassandra 4.1"));
        }
    }
}
