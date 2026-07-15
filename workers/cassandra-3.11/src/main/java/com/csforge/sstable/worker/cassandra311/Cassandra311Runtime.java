package com.csforge.sstable.worker.cassandra311;

import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 3.11 API. */
public final class Cassandra311Runtime {
    private Cassandra311Runtime() {
    }

    public static String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }
}
