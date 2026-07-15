package com.csforge.sstable.worker.api;

/** Optional adapter capability for starting an isolated Cassandra daemon. */
public interface SandboxRuntimeAdapter extends RuntimeAdapter {
    SandboxHandle startSandbox(SandboxOptions options) throws Exception;
}
