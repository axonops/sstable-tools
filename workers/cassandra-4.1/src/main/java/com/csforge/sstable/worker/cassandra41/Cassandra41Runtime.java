package com.csforge.sstable.worker.cassandra41;

import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 4.1 API. */
public final class Cassandra41Runtime {
    private Cassandra41Runtime() {
    }

    public static String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }
}
