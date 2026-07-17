package com.axonops.sstable.workspace;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Complete, checksummed SSTable descriptor sets captured from stable directories. */
public final class SourceInventory {
    private static final String TOC_SUFFIX = "-TOC.txt";
    private static final String DATA_SUFFIX = "-Data.db";
    private static final Pattern COMPONENT_NAME = Pattern.compile(
            "[A-Za-z0-9][A-Za-z0-9+_-]*(?:\\.[A-Za-z0-9]+)+");

    private final List<SstableSet> sets;

    SourceInventory(List<SstableSet> sets) throws WorkspaceException {
        if (sets == null || sets.isEmpty()) {
            throw new WorkspaceException("Source inventory must contain at least one SSTable set");
        }
        List<SstableSet> sorted = new ArrayList<>(sets);
        if (sorted.contains(null)) {
            throw new WorkspaceException("Source inventory contains a null SSTable set");
        }
        sorted.sort(Comparator.comparing(set -> set.directory() + "/" + set.descriptor()));
        Set<String> identities = new HashSet<>();
        for (SstableSet set : sorted) {
            if (!identities.add(set.directory() + "/" + set.descriptor())) {
                throw new WorkspaceException("Duplicate SSTable descriptor in source inventory");
            }
        }
        this.sets = Collections.unmodifiableList(sorted);
    }

    public static SourceInventory capture(List<Path> sourceDirectories)
            throws WorkspaceException {
        if (sourceDirectories == null || sourceDirectories.isEmpty()) {
            throw new WorkspaceException("At least one SSTable source directory is required");
        }

        Set<Path> canonicalDirectories = new LinkedHashSet<>();
        for (Path source : sourceDirectories) {
            try {
                Path canonical = source.toRealPath();
                if (!Files.isDirectory(canonical, LinkOption.NOFOLLOW_LINKS)) {
                    throw new WorkspaceException("Source must be a stable snapshot or backup directory, not a standalone component: "
                            + source);
                }
                canonicalDirectories.add(canonical);
            } catch (IOException e) {
                throw new WorkspaceException("Cannot resolve source directory " + source, e);
            }
        }

        List<SstableSet> sets = new ArrayList<>();
        for (Path directory : canonicalDirectories) {
            sets.addAll(captureDirectory(directory));
        }
        sets.sort(Comparator.comparing(set -> set.directory() + "/" + set.descriptor()));
        if (sets.isEmpty()) {
            throw new WorkspaceException("No complete SSTable descriptors were found");
        }
        return new SourceInventory(sets);
    }

    private static List<SstableSet> captureDirectory(Path directory)
            throws WorkspaceException {
        List<Path> tocFiles = new ArrayList<>();
        List<Path> dataFiles = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (name.startsWith("tmp-") || name.contains("-tmp-")
                        || name.endsWith(".tmp")) {
                    throw new WorkspaceException("Temporary SSTable component is not a stable source: "
                            + entry);
                }
                if (Files.isSymbolicLink(entry)) {
                    throw new WorkspaceException("Symlinked SSTable components are not accepted: " + entry);
                }
                if (Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                    if (name.endsWith(TOC_SUFFIX)) {
                        tocFiles.add(entry);
                    }
                    if (name.endsWith(DATA_SUFFIX)) {
                        dataFiles.add(entry);
                    }
                }
            }
        } catch (IOException e) {
            throw new WorkspaceException("Cannot inventory source directory " + directory, e);
        }

        tocFiles.sort(Comparator.comparing(path -> path.getFileName().toString()));
        Set<String> completeDescriptors = new HashSet<>();
        List<SstableSet> result = new ArrayList<>();
        for (Path tocFile : tocFiles) {
            String fileName = tocFile.getFileName().toString();
            String descriptor = fileName.substring(0, fileName.length() - TOC_SUFFIX.length());
            completeDescriptors.add(descriptor);
            result.add(captureSet(directory, descriptor, tocFile));
        }

        for (Path dataFile : dataFiles) {
            String name = dataFile.getFileName().toString();
            String descriptor = name.substring(0, name.length() - DATA_SUFFIX.length());
            if (!completeDescriptors.contains(descriptor)) {
                throw new WorkspaceException("Incomplete SSTable descriptor " + descriptor
                        + " has Data.db but no TOC.txt in " + directory);
            }
        }
        return result;
    }

    private static SstableSet captureSet(Path directory, String descriptor, Path tocFile)
            throws WorkspaceException {
        int firstSeparator = descriptor.indexOf('-');
        int lastSeparator = descriptor.lastIndexOf('-');
        if (firstSeparator <= 0 || lastSeparator <= firstSeparator
                || lastSeparator == descriptor.length() - 1) {
            throw new WorkspaceException("Malformed SSTable descriptor name: " + descriptor);
        }
        String version = descriptor.substring(0, firstSeparator);
        String format = descriptor.substring(lastSeparator + 1);

        List<String> componentNames;
        try {
            componentNames = Files.readAllLines(tocFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read SSTable TOC " + tocFile, e);
        }

        Set<String> uniqueNames = new LinkedHashSet<>();
        for (String line : componentNames) {
            String name = line.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (!COMPONENT_NAME.matcher(name).matches() || name.contains("..")) {
                throw new WorkspaceException("Unsafe component name '" + name
                        + "' in " + tocFile);
            }
            if (!uniqueNames.add(name)) {
                throw new WorkspaceException("Duplicate component '" + name + "' in " + tocFile);
            }
        }
        if (!uniqueNames.contains("TOC.txt") || !uniqueNames.contains("Data.db")
                || !uniqueNames.contains("Statistics.db")) {
            throw new WorkspaceException("Incomplete TOC for " + descriptor
                    + "; TOC.txt, Data.db, and Statistics.db are required");
        }

        List<String> sortedNames = new ArrayList<>(uniqueNames);
        Collections.sort(sortedNames);
        List<SourceComponent> components = new ArrayList<>();
        for (String componentName : sortedNames) {
            Path componentPath = directory.resolve(descriptor + "-" + componentName);
            if (!Files.isRegularFile(componentPath, LinkOption.NOFOLLOW_LINKS)) {
                throw new WorkspaceException("TOC declares missing component " + componentPath);
            }
            components.add(SourceComponent.capture(componentName, componentPath));
        }
        return new SstableSet(descriptor, version, format, directory, components);
    }

    public void verifyUnchanged() throws WorkspaceException {
        for (SstableSet set : sets) {
            set.verifyUnchanged();
        }
    }

    public int componentCount() {
        int count = 0;
        for (SstableSet set : sets) {
            count += set.components().size();
        }
        return count;
    }

    public List<SstableSet> sets() {
        return sets;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SourceInventory
                && sets.equals(((SourceInventory) other).sets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sets);
    }
}
