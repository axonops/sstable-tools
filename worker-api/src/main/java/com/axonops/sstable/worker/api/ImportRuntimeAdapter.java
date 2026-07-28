package com.axonops.sstable.worker.api;

/** Release-specific schema validation and SSTable import contract. */
public interface ImportRuntimeAdapter extends RuntimeAdapter {
    ImportResult importSstables(ImportOptions options) throws Exception;
}
