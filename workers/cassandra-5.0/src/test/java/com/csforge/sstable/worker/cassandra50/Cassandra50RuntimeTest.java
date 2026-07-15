package com.csforge.sstable.worker.cassandra50;

import org.junit.Assert;
import org.junit.Test;

public class Cassandra50RuntimeTest {
    @Test
    public void pinnedRuntimeSatisfiesLinkageContract() {
        Cassandra50Runtime runtime = new Cassandra50Runtime();

        Assert.assertEquals("5.0.4", runtime.installedVersion());
        runtime.verifyLinkage();
    }
}
