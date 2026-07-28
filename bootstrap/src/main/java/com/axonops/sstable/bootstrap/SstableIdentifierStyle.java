package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.SstableSet;
import com.axonops.sstable.workspace.WorkspaceException;
import java.util.Map;
import java.util.regex.Pattern;

/** Infers the Cassandra SSTable identifier builder from explicitly selected descriptors. */
enum SstableIdentifierStyle {
    NUMERIC("numeric"),
    UUID("uuid");

    private static final Pattern NUMERIC_IDENTIFIER = Pattern.compile("[0-9]+");
    private static final Pattern UUID_IDENTIFIER = Pattern.compile(
            "[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{18}",
            Pattern.CASE_INSENSITIVE);

    private final String manifestValue;

    SstableIdentifierStyle(String manifestValue) {
        this.manifestValue = manifestValue;
    }

    String manifestValue() {
        return manifestValue;
    }

    boolean usesUuidIdentifiers() {
        return this == UUID;
    }

    static SstableIdentifierStyle infer(SourceInventory source) throws WorkspaceException {
        boolean uuid = false;
        for (SstableSet set : source.sets()) {
            String identifier = identifier(set.descriptor());
            if (isUuidIdentifier(identifier)) {
                uuid = true;
            } else if (!NUMERIC_IDENTIFIER.matcher(identifier).matches()) {
                throw new WorkspaceException("Cannot infer SSTable identifier style from "
                        + "selected descriptor " + set.descriptor());
            }
        }
        return uuid ? UUID : NUMERIC;
    }

    static SstableIdentifierStyle forImport(SourceInventory source, String releaseLine)
            throws WorkspaceException {
        if (!source.sets().isEmpty()) {
            return infer(source);
        }
        return "5.0".equals(releaseLine) ? UUID : NUMERIC;
    }

    static SstableIdentifierStyle recorded(Map<String, String> outputIdentity)
            throws WorkspaceException {
        String value = outputIdentity.get("sstable.identifier-style");
        for (SstableIdentifierStyle style : values()) {
            if (style.manifestValue.equals(value)) {
                return style;
            }
        }
        throw new WorkspaceException("Workspace output identity has no valid SSTable "
                + "identifier style");
    }

    static boolean isUuidIdentifier(String identifier) {
        return identifier != null && UUID_IDENTIFIER.matcher(identifier).matches();
    }

    static String identifier(String descriptor) throws WorkspaceException {
        int first = descriptor.indexOf('-');
        int last = descriptor.lastIndexOf('-');
        if (first <= 0 || last <= first || last == descriptor.length() - 1) {
            throw new WorkspaceException("Malformed SSTable descriptor name: " + descriptor);
        }
        return descriptor.substring(first + 1, last);
    }
}
