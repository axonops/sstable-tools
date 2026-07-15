package com.csforge.sstable.worker.api;

/** Cassandra-neutral contract implemented by each release-linked worker adapter. */
public interface RuntimeAdapter {
    String installedVersion();

    void verifyLinkage();
}
