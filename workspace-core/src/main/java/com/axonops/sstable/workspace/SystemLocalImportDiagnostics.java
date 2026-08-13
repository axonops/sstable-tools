package com.axonops.sstable.workspace;

/** Actionable diagnostics for the required {@code system.local} identity cell. */
public final class SystemLocalImportDiagnostics {
    private SystemLocalImportDiagnostics() {
    }

    public static String noLocalRow(SourceInventory source) {
        return "Imported system.local contains no local row; selected SSTables: "
                + source.describeSelectedSstables();
    }

    public static String partialClusterNameSelection(SourceInventory source) {
        return "Imported system.local contains the local row but no live cluster_name cell; "
                + "the selection is partial. Select every SSTable for system.local, including "
                + "SSTables in other Cassandra data directories; selected SSTables: "
                + source.describeSelectedSstables();
    }

    public static String emptyClusterName(SourceInventory source) {
        return "Imported system.local cluster_name is null or empty; selected SSTables: "
                + source.describeSelectedSstables();
    }
}
