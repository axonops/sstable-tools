package com.csforge.sstable.workspace;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** One complete descriptor and its TOC-declared component set. */
public final class SstableSet {
    private static final Pattern COMPONENT_NAME = Pattern.compile(
            "[A-Za-z0-9][A-Za-z0-9+_-]*(?:\\.[A-Za-z0-9]+)+");
    private final String descriptor;
    private final String formatVersion;
    private final String format;
    private final Path directory;
    private final List<SourceComponent> components;

    SstableSet(String descriptor,
               String formatVersion,
               String format,
               Path directory,
               List<SourceComponent> components) throws WorkspaceException {
        if (descriptor == null || formatVersion == null || format == null
                || directory == null || components == null || components.isEmpty()) {
            throw new WorkspaceException("SSTable set fields must not be null or empty");
        }
        int firstSeparator = descriptor.indexOf('-');
        int lastSeparator = descriptor.lastIndexOf('-');
        if (firstSeparator <= 0 || lastSeparator <= firstSeparator
                || !formatVersion.equals(descriptor.substring(0, firstSeparator))
                || !format.equals(descriptor.substring(lastSeparator + 1))) {
            throw new WorkspaceException("SSTable descriptor identity is inconsistent: "
                    + descriptor);
        }
        if (!directory.isAbsolute() || !directory.equals(directory.normalize())) {
            throw new WorkspaceException("SSTable source directory must be absolute and "
                    + "normalized: " + directory);
        }

        Set<String> names = new HashSet<>();
        for (SourceComponent component : components) {
            if (component == null || !COMPONENT_NAME.matcher(component.name()).matches()
                    || component.name().contains("..") || !names.add(component.name())) {
                throw new WorkspaceException("Invalid or duplicate SSTable component in "
                        + descriptor);
            }
            Path expected = directory.resolve(descriptor + "-" + component.name());
            if (!expected.equals(component.path())) {
                throw new WorkspaceException("Source component path does not match descriptor: "
                        + component.path());
            }
        }
        if (!names.contains("TOC.txt") || !names.contains("Data.db")
                || !names.contains("Statistics.db")) {
            throw new WorkspaceException("Incomplete component inventory for " + descriptor);
        }
        this.descriptor = descriptor;
        this.formatVersion = formatVersion;
        this.format = format;
        this.directory = directory;
        this.components = Collections.unmodifiableList(new ArrayList<>(components));
    }

    public void verifyUnchanged() throws WorkspaceException {
        for (SourceComponent component : components) {
            component.verifyUnchanged();
        }
    }

    public String descriptor() {
        return descriptor;
    }

    public String formatVersion() {
        return formatVersion;
    }

    public String format() {
        return format;
    }

    public Path directory() {
        return directory;
    }

    public List<SourceComponent> components() {
        return components;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SstableSet)) {
            return false;
        }
        SstableSet that = (SstableSet) other;
        return descriptor.equals(that.descriptor)
                && formatVersion.equals(that.formatVersion)
                && format.equals(that.format)
                && directory.equals(that.directory)
                && components.equals(that.components);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptor, formatVersion, format, directory, components);
    }
}
