package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.WorkspaceException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Cassandra50StorageCompatibilityTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void followsEveryConfiguredCassandra50StorageMode() throws Exception {
        Assert.assertEquals(Cassandra50StorageCompatibility.CASSANDRA_4,
                resolveConfigured("CASSANDRA_4", "big"));
        Assert.assertEquals(Cassandra50StorageCompatibility.UPGRADING,
                resolveConfigured("UPGRADING", "big"));
        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                resolveConfigured("NONE", "big"));
        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                resolveConfigured("'NONE'", "big"));
        Assert.assertEquals("nb", Cassandra50StorageCompatibility.bigVersion(
                Cassandra50StorageCompatibility.CASSANDRA_4));
        Assert.assertEquals("oa", Cassandra50StorageCompatibility.bigVersion(
                Cassandra50StorageCompatibility.UPGRADING));
        Assert.assertEquals("oa", Cassandra50StorageCompatibility.bigVersion(
                Cassandra50StorageCompatibility.NONE));
    }

    @Test
    public void omittedSettingUsesCassandra50DefaultCompatibility() throws Exception {
        Path home = temporary.newFolder("default-home").toPath();
        Path conf = Files.createDirectories(home.resolve("conf"));
        Files.write(conf.resolve("cassandra.yaml"),
                Arrays.asList("cluster_name: test", "# storage_compatibility_mode: NONE"),
                StandardCharsets.UTF_8);

        Assert.assertEquals(Cassandra50StorageCompatibility.CASSANDRA_4,
                Cassandra50StorageCompatibility.resolve(home, emptyInventory(), "big",
                        Collections.<String, String>emptyMap()));
        Assert.assertEquals("big", Cassandra50StorageCompatibility.resolveSettings(home,
                emptyInventory(), null, Collections.<String, String>emptyMap()).outputFormat());
    }

    @Test
    public void configuredSelectedFormatIsAuthoritative() throws Exception {
        Path btiHome = configuredHome("configured-bti", "NONE", "bti");
        Cassandra50StorageCompatibility.Settings bti =
                Cassandra50StorageCompatibility.resolveSettings(btiHome, emptyInventory(), null,
                        Collections.<String, String>emptyMap());
        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                bti.storageCompatibilityMode());
        Assert.assertEquals("bti", bti.outputFormat());

        Path bigHome = configuredHome("configured-big", "UPGRADING", "big");
        Cassandra50StorageCompatibility.Settings big =
                Cassandra50StorageCompatibility.resolveSettings(bigHome, emptyInventory(), null,
                        Collections.<String, String>emptyMap());
        Assert.assertEquals(Cassandra50StorageCompatibility.UPGRADING,
                big.storageCompatibilityMode());
        Assert.assertEquals("big", big.outputFormat());
    }

    @Test
    public void outputFormatOptionCannotRequestAConversion() throws Exception {
        Path home = configuredHome("configured-bti-mismatch", "NONE", "bti");
        try {
            Cassandra50StorageCompatibility.resolveSettings(home, emptyInventory(), "big",
                    Collections.<String, String>emptyMap());
            Assert.fail("Expected selected_format mismatch rejection");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("does not match"));
            Assert.assertTrue(expected.getMessage().contains("conversion is not supported"));
        }
    }

    @Test
    public void configuredModeOverridesInputDescriptorVersion() throws Exception {
        Path home = configuredHome("configured-native", "NONE");
        SourceInventory nb = inventory("configured-nb", "nb-1-big");

        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                Cassandra50StorageCompatibility.resolve(home, nb, "big",
                        Collections.<String, String>emptyMap()));
    }

    @Test
    public void infersModeWhenNoCassandraConfigurationIsAvailable() throws Exception {
        Path home = temporary.newFolder("configuration-free-home").toPath();
        Assert.assertEquals(Cassandra50StorageCompatibility.CASSANDRA_4,
                Cassandra50StorageCompatibility.resolve(home,
                        inventory("nb-source", "nb-1-big"), "big",
                        Collections.<String, String>emptyMap()));
        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                Cassandra50StorageCompatibility.resolve(home,
                        inventory("oa-source", "oa-1-big"), "big",
                        Collections.<String, String>emptyMap()));
        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                Cassandra50StorageCompatibility.resolve(home,
                        inventory("da-source", "da-1-bti"), "bti",
                        Collections.<String, String>emptyMap()));
    }

    @Test
    public void cassandraConfEnvironmentTakesPrecedence() throws Exception {
        Path home = configuredHome("home-mode", "CASSANDRA_4");
        Path external = temporary.newFolder("external-conf").toPath();
        Files.write(external.resolve("cassandra.yaml"),
                Arrays.asList("sstable:", "  selected_format: bti",
                        "storage_compatibility_mode: NONE"),
                StandardCharsets.UTF_8);
        Map<String, String> environment = new HashMap<>();
        environment.put("CASSANDRA_CONF", external.toString());

        Assert.assertEquals(Cassandra50StorageCompatibility.NONE,
                Cassandra50StorageCompatibility.resolve(home, emptyInventory(), "bti",
                        environment));
        Assert.assertEquals("bti", Cassandra50StorageCompatibility.resolveSettings(home,
                emptyInventory(), null, environment).outputFormat());
    }

    @Test
    public void rejectsBtiInCassandra4CompatibilityMode() throws Exception {
        Path home = configuredHome("cassandra-4-bti", "CASSANDRA_4", "bti");
        try {
            Cassandra50StorageCompatibility.resolveSettings(home, emptyInventory(), null,
                    Collections.<String, String>emptyMap());
            Assert.fail("Expected BTI compatibility rejection");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("BTI output is unavailable"));
            Assert.assertTrue(expected.getMessage().contains("CASSANDRA_4"));
        }
    }

    @Test
    public void rejectsUnsupportedAndDuplicateSettings() throws Exception {
        Path invalid = configuredHome("invalid-home", "CASSANDRA_3");
        try {
            Cassandra50StorageCompatibility.resolve(invalid, emptyInventory(), "big",
                    Collections.<String, String>emptyMap());
            Assert.fail("Expected invalid mode rejection");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("Unsupported"));
        }

        Path duplicate = configuredHome("duplicate-home", "NONE");
        Files.write(duplicate.resolve("conf/cassandra.yaml"),
                Arrays.asList("storage_compatibility_mode: NONE",
                        "storage_compatibility_mode: UPGRADING"),
                StandardCharsets.UTF_8);
        try {
            Cassandra50StorageCompatibility.resolve(duplicate, emptyInventory(), "big",
                    Collections.<String, String>emptyMap());
            Assert.fail("Expected duplicate mode rejection");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("Duplicate"));
        }

        Path malformed = configuredHome("malformed-home", "[NONE]");
        try {
            Cassandra50StorageCompatibility.resolve(malformed, emptyInventory(), "big",
                    Collections.<String, String>emptyMap());
            Assert.fail("Expected malformed mode rejection");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("Malformed"));
        }

        Path duplicateFormat = configuredHome("duplicate-format", "NONE", "big");
        Files.write(duplicateFormat.resolve("conf/cassandra.yaml"),
                Arrays.asList("sstable:", "  selected_format: big",
                        "  selected_format: bti", "storage_compatibility_mode: NONE"),
                StandardCharsets.UTF_8);
        try {
            Cassandra50StorageCompatibility.resolveSettings(duplicateFormat,
                    emptyInventory(), null, Collections.<String, String>emptyMap());
            Assert.fail("Expected duplicate format rejection");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("Duplicate selected_format"));
        }
    }

    private String resolveConfigured(String mode, String format) throws Exception {
        Path home = configuredHome("configured-" + mode + "-" + format, mode, format);
        return Cassandra50StorageCompatibility.resolve(home, emptyInventory(), format,
                Collections.<String, String>emptyMap());
    }

    private Path configuredHome(String name, String mode) throws Exception {
        return configuredHome(name, mode, "big");
    }

    private Path configuredHome(String name, String mode, String format) throws Exception {
        Path home = temporary.newFolder(name).toPath();
        Path conf = Files.createDirectories(home.resolve("conf"));
        Files.write(conf.resolve("cassandra.yaml"),
                Arrays.asList("sstable:", "  selected_format: " + format,
                        "storage_compatibility_mode: " + mode),
                StandardCharsets.UTF_8);
        return home;
    }

    private SourceInventory emptyInventory() throws Exception {
        Path directory = temporary.newFolder().toPath();
        return SourceInventory.captureDirectoryAllowEmpty(directory);
    }

    private SourceInventory inventory(String name, String descriptor) throws Exception {
        Path directory = temporary.newFolder(name).toPath();
        Files.write(directory.resolve(descriptor + "-TOC.txt"),
                Arrays.asList("TOC.txt", "Data.db", "Statistics.db"),
                StandardCharsets.UTF_8);
        Files.write(directory.resolve(descriptor + "-Data.db"), new byte[]{1});
        Files.write(directory.resolve(descriptor + "-Statistics.db"), new byte[]{2});
        return SourceInventory.capture(Collections.singletonList(directory));
    }
}
