package com.axonops.sstable.bootstrap;

import java.nio.file.Paths;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class BootstrapArgumentsTest {
    @Test
    public void parsesSchemaCaptureAndWorkspaceImport() throws Exception {
        BootstrapArguments create = BootstrapArguments.parse(new String[]{
                "workspace", "create", "workspace",
                "--sstables", "sstables", "--schema", "schema.cql"
        });
        Assert.assertEquals(BootstrapArguments.Action.WORKSPACE_CREATE, create.action());
        Assert.assertEquals(Paths.get("schema.cql"), create.schemaPath());

        BootstrapArguments importArguments = BootstrapArguments.parse(new String[]{
                "workspace", "import", "workspace", "--cassandra-home", "cassandra"
        });
        Assert.assertEquals(BootstrapArguments.Action.WORKSPACE_IMPORT,
                importArguments.action());
        Assert.assertEquals(Paths.get("cassandra"),
                importArguments.runtimeOptions().cassandraHome());
    }

    @Test
    public void rejectsSchemaOutsideWorkspaceCreate() throws Exception {
        try {
            BootstrapArguments.parse(new String[]{
                    "workspace", "import", "workspace", "--schema", "schema.cql"
            });
            Assert.fail("Expected --schema to be rejected");
        } catch (BootstrapException e) {
            Assert.assertEquals(BootstrapException.USAGE_EXIT_CODE, e.exitCode());
            Assert.assertTrue(e.getMessage().contains("only valid with workspace create"));
        }
    }

    @Test
    public void parsesOnlySupportedStartTimestampPolicies() throws Exception {
        BootstrapArguments afterSource = BootstrapArguments.parse(new String[]{
                "workspace", "start", "workspace", "--timestamp-policy", "after-source"
        });
        Assert.assertEquals(TimestampPolicy.AFTER_SOURCE, afterSource.timestampPolicy());

        try {
            BootstrapArguments.parse(new String[]{
                    "workspace", "status", "workspace", "--timestamp-policy", "wall-clock"
            });
            Assert.fail("Expected timestamp policy outside start to fail");
        } catch (BootstrapException e) {
            Assert.assertTrue(e.getMessage().contains("only valid with workspace start"));
        }

        try {
            BootstrapArguments.parse(new String[]{
                    "workspace", "start", "workspace", "--timestamp-policy", "unsafe"
            });
            Assert.fail("Expected unknown timestamp policy to fail");
        } catch (BootstrapException e) {
            Assert.assertTrue(e.getMessage().contains("wall-clock or after-source"));
        }
    }

    @Test
    public void parsesAndValidatesWorkspaceExportOptions() throws Exception {
        BootstrapArguments export = BootstrapArguments.parse(new String[]{
                "workspace", "export", "workspace", "--mode", "delta",
                "--output", "published"
        });
        Assert.assertEquals(BootstrapArguments.Action.WORKSPACE_EXPORT, export.action());
        Assert.assertEquals(ExportMode.DELTA, export.exportMode());
        Assert.assertEquals(Paths.get("published"), export.outputPath());

        assertUsageFailure(new String[]{"workspace", "export", "workspace"},
                "requires --mode");
        assertUsageFailure(new String[]{"workspace", "export", "workspace", "--mode",
                "archive", "--output", "published"}, "delta or snapshot");
        assertUsageFailure(new String[]{"workspace", "status", "workspace", "--mode",
                "snapshot", "--output", "published"}, "only valid with workspace export");
    }

    @Test
    public void parsesWorkspaceCqlshAndRestrictsExecuteOption() throws Exception {
        BootstrapArguments cqlsh = BootstrapArguments.parse(new String[]{
                "--cassandra-home", "cassandra", "workspace", "cqlsh", "workspace",
                "--execute", "SELECT * FROM blog.users"
        });
        Assert.assertEquals(BootstrapArguments.Action.WORKSPACE_CQLSH, cqlsh.action());
        Assert.assertEquals("SELECT * FROM blog.users", cqlsh.executeCql());
        Assert.assertEquals(Paths.get("cassandra"),
                cqlsh.runtimeOptions().cassandraHome());

        assertUsageFailure(new String[]{"workspace", "status", "workspace",
                "--execute", "SELECT now() FROM system.local"},
                "only valid with workspace cqlsh");
    }

    @Test
    public void parsesDirectCqlshWithSelectedSstablesSchemaAndTemporaryParent()
            throws Exception {
        BootstrapArguments direct = BootstrapArguments.parse(new String[]{
                "--cassandra-home", "cassandra", "--tmp-dir", "/var/tmp/sstable-tools",
                "--sstables", "source/nb-1-big-Data.db", "--schema", "schema.cql",
                "cqlsh", "--execute", "INSERT INTO test.items (id) VALUES (1)"
        });

        Assert.assertEquals(BootstrapArguments.Action.DIRECT_CQLSH, direct.action());
        Assert.assertEquals(Paths.get("/var/tmp/sstable-tools"), direct.temporaryDirectory());
        Assert.assertEquals(Paths.get("schema.cql"), direct.schemaPath());
        Assert.assertEquals(1, direct.sourceDirectories().size());
        Assert.assertTrue(direct.executeCql().startsWith("INSERT INTO"));
    }

    @Test
    public void parsesSstableOutputFormatOnlyForCreationAndDirectCqlsh() throws Exception {
        BootstrapArguments create = BootstrapArguments.parse(new String[]{
                "workspace", "create", "workspace", "--sstables", "source-Data.db",
                "--output-format", "bti"
        });
        Assert.assertEquals(BootstrapArguments.SstableOutputFormat.BTI,
                create.sstableOutputFormat());

        BootstrapArguments direct = BootstrapArguments.parse(new String[]{
                "cqlsh", "--sstables", "source-Data.db", "--schema", "schema.cql",
                "--output-format", "big"
        });
        Assert.assertEquals(BootstrapArguments.SstableOutputFormat.BIG,
                direct.sstableOutputFormat());

        assertUsageFailure(new String[]{"workspace", "start", "workspace",
                "--output-format", "bti"}, "only valid with workspace create or cqlsh");
        assertUsageFailure(new String[]{"cqlsh", "--sstables", "source-Data.db",
                "--schema", "schema.cql", "--output-format", "unknown"}, "big or bti");
    }

    @Test
    public void directCqlshRequiresExplicitSourcesAndSchema() throws Exception {
        assertUsageFailure(new String[]{"cqlsh", "--schema", "schema.cql"},
                "requires at least one --sstables");
        assertUsageFailure(new String[]{"cqlsh", "--sstables", "source-Data.db"},
                "requires --schema");
        assertUsageFailure(new String[]{"workspace", "status", "workspace", "--tmp-dir",
                "/var/tmp/sstable-tools"}, "only valid with cqlsh");
    }

    @Test
    public void acceptsCommaSeparatedSstableSources() throws Exception {
        BootstrapArguments direct = BootstrapArguments.parse(new String[]{
                "--sstables", "source/nb-1-big-Data.db,source/nb-2-big-Data.db",
                "--schema", "schema.cql", "cqlsh"
        });
        Assert.assertEquals(2, direct.sourceDirectories().size());
        Assert.assertEquals(Paths.get("source/nb-1-big-Data.db"),
                direct.sourceDirectories().get(0));
        Assert.assertEquals(Paths.get("source/nb-2-big-Data.db"),
                direct.sourceDirectories().get(1));
        assertUsageFailure(new String[]{"--sstables", "first,,second", "--schema",
                "schema.cql", "cqlsh"}, "contains an empty path");
    }

    @Test
    public void requiresExactUuidConfirmationOnlyForWorkspaceDestroy() throws Exception {
        UUID workspaceId = UUID.fromString("20a0d99c-f07a-4ef3-8999-e063aad5c183");
        BootstrapArguments destroy = BootstrapArguments.parse(new String[]{
                "workspace", "destroy", "workspace",
                "--confirm-workspace-id", workspaceId.toString()
        });
        Assert.assertEquals(BootstrapArguments.Action.WORKSPACE_DESTROY, destroy.action());
        Assert.assertEquals(workspaceId, destroy.confirmedWorkspaceId());

        assertUsageFailure(new String[]{"workspace", "destroy", "workspace"},
                "requires --confirm-workspace-id");
        assertUsageFailure(new String[]{"workspace", "destroy", "workspace",
                "--confirm-workspace-id", "not-a-uuid"}, "requires a valid UUID");
        assertUsageFailure(new String[]{"workspace", "status", "workspace",
                "--confirm-workspace-id", workspaceId.toString()},
                "only valid with workspace destroy");
    }

    private static void assertUsageFailure(String[] arguments, String expected)
            throws Exception {
        try {
            BootstrapArguments.parse(arguments);
            Assert.fail("Expected usage failure containing " + expected);
        } catch (BootstrapException failure) {
            Assert.assertEquals(BootstrapException.USAGE_EXIT_CODE, failure.exitCode());
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(expected));
        }
    }
}
