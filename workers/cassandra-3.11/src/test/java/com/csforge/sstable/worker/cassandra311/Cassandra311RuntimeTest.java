package com.csforge.sstable.worker.cassandra311;

import org.junit.Assert;
import org.junit.Test;

public class Cassandra311RuntimeTest {
    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra311Runtime runtime = new Cassandra311Runtime();

        Assert.assertEquals("3.11.19", runtime.installedVersion());
        runtime.verifyLinkage();
    }
}
