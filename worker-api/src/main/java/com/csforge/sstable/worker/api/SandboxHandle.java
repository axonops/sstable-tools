package com.csforge.sstable.worker.api;

/** Running release-specific Cassandra sandbox owned by the worker process. */
public interface SandboxHandle {
    String nativeAddress();

    int nativePort();

    boolean isRunning();

    void stop() throws Exception;
}
