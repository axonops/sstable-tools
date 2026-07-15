package com.csforge.sstable.worker.cassandra50;

import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 5.0 API. */
public final class Cassandra50Runtime {
    private Cassandra50Runtime() {
    }

    public static String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }
}
