package com.axonops.sstable.bootstrap;

/** Test-only hard-stop boundaries used by the real export recovery suite. */
final class ExportFailpoint {
    static final String ENABLE_PROPERTY = "sstable.tools.test.enable-export-failpoints";
    static final String NAME_PROPERTY = "sstable.tools.test.export-failpoint";
    static final int EXIT_CODE = 97;

    private ExportFailpoint() {
    }

    static void hit(String name) {
        if (Boolean.getBoolean(ENABLE_PROPERTY)
                && name.equals(System.getProperty(NAME_PROPERTY))) {
            Runtime.getRuntime().halt(EXIT_CODE);
        }
    }
}
