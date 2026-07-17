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
}
