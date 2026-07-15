package com.csforge.sstable.worker.cassandra40;

import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 4.0 API. */
public final class Cassandra40Runtime {
    private Cassandra40Runtime() {
    }

    public static String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }
}
