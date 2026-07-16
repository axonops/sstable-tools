package com.csforge.sstable.worker.api;

/** Running release-specific Cassandra sandbox owned by the worker process. */
public interface SandboxHandle {
    String nativeAddress();

    int nativePort();

    boolean isRunning();

    void flush() throws Exception;

    boolean isFlushed();

    void verify() throws Exception;

    boolean isVerified();

    void stop() throws Exception;
}
