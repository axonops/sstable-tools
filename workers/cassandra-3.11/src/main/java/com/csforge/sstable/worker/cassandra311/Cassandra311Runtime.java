package com.csforge.sstable.worker.cassandra311;

import com.csforge.sstable.worker.api.LinkageVerifier;
import com.csforge.sstable.worker.api.RuntimeAdapter;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;

/** Linkage probe compiled against the supported Cassandra 3.11 API. */
public final class Cassandra311Runtime implements RuntimeAdapter {
    public Cassandra311Runtime() {
    }

    @Override
    public String installedVersion() {
        return FBUtilities.getReleaseVersionString();
    }

    @Override
    public void verifyLinkage() {
        verifySharedRuntimeContract();
    }

    private static void verifySharedRuntimeContract() {
        LinkageVerifier.requirePublicStaticMethod(FBUtilities.class,
                "getReleaseVersionString", String.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class, "activate", void.class);
        LinkageVerifier.requirePublicMethod(CassandraDaemon.class,
                "startNativeTransport", void.class);
        LinkageVerifier.requirePublicStaticField(QueryProcessor.class,
                "instance", QueryProcessor.class);
        LinkageVerifier.requireAssignable(QueryHandler.class, QueryProcessor.class);
    }
}
