package com.axonops.sstable.worker.cassandra311;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Cassandra311RuntimeTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra311Runtime runtime = new Cassandra311Runtime();

        Assert.assertEquals("3.11.19", runtime.installedVersion());
        runtime.verifyLinkage();
    }

    @Test
    public void installedRuntimeDefinesCompatibleBigFormatRange() throws Exception {
        for (String version : Arrays.asList("ma", "mb", "mc", "md", "me")) {
            Assert.assertEquals(version,
                    Cassandra311Importer.requireCompatibleVersion(version, "big").toString());
        }
        assertUnsupported("la", "big");
        assertUnsupported("zz", "big");
        assertUnsupported("ma", "bti");
    }

    @Test
    public void serializedHeaderMayContainADeclaredColumnSubset() {
        Map<ByteBuffer, AbstractType<?>> declared = new LinkedHashMap<>();
        declared.put(ByteBufferUtil.bytes("name"), UTF8Type.instance);
        declared.put(ByteBufferUtil.bytes("year"), LongType.instance);
        Map<ByteBuffer, AbstractType<?>> subset = new LinkedHashMap<>();
        subset.put(ByteBufferUtil.bytes("name"), UTF8Type.instance);

        Assert.assertTrue(Cassandra311Importer.storedColumnsMatch(declared, subset));
        subset.put(ByteBufferUtil.bytes("unknown"), UTF8Type.instance);
        Assert.assertFalse(Cassandra311Importer.storedColumnsMatch(declared, subset));
        subset.remove(ByteBufferUtil.bytes("unknown"));
        subset.put(ByteBufferUtil.bytes("name"), LongType.instance);
        Assert.assertFalse(Cassandra311Importer.storedColumnsMatch(declared, subset));
    }

    @Test
    public void generationScanIncludesEveryPublishedComponent() throws Exception {
        Path directory = temporary.newFolder("generations").toPath();
        Files.write(directory.resolve("me-2-big-CompressionInfo.db"), new byte[]{1});
        Files.write(directory.resolve("ma-9-big-Data.db"), new byte[]{1});
        Files.write(directory.resolve("not-an-sstable"), new byte[]{1});

        Assert.assertEquals(10, Cassandra311Importer.nextGeneration(directory));
    }

    private void assertUnsupported(String version, String format) throws Exception {
        try {
            Cassandra311Importer.requireCompatibleVersion(version, format);
            Assert.fail("Expected unsupported format " + version + "-" + format);
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("Unsupported Cassandra 3.11"));
        }
    }
}
