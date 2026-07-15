package com.csforge.sstable.worker.cassandra41;

import org.junit.Assert;
import org.junit.Test;

public class Cassandra41RuntimeTest {
    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra41Runtime runtime = new Cassandra41Runtime();

        Assert.assertEquals("4.1.3", runtime.installedVersion());
        runtime.verifyLinkage();
    }
}
