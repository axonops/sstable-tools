package com.axonops.sstable.worker.api;

/** Optional release-specific capability for direct CQL against a selected SSTable set. */
public interface DirectSandboxRuntimeAdapter extends RuntimeAdapter {
    ImportedSandbox importAndStart(ImportOptions importOptions, SandboxOptions sandboxOptions)
            throws Exception;
}
