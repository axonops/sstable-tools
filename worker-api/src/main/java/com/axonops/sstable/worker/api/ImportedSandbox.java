package com.axonops.sstable.worker.api;

/** Result of importing selected SSTables and starting their isolated CQL endpoint in one JVM. */
public final class ImportedSandbox {
    private final ImportResult importResult;
    private final SandboxHandle sandboxHandle;

    public ImportedSandbox(ImportResult importResult, SandboxHandle sandboxHandle) {
        if (importResult == null || sandboxHandle == null) {
            throw new IllegalArgumentException("Imported sandbox result and handle are required");
        }
        this.importResult = importResult;
        this.sandboxHandle = sandboxHandle;
    }

    public ImportResult importResult() {
        return importResult;
    }

    public SandboxHandle sandboxHandle() {
        return sandboxHandle;
    }
}
