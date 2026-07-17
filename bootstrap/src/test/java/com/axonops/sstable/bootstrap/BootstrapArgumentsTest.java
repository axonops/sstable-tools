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
