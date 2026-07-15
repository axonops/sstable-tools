package com.csforge.sstable.bootstrap;

import java.nio.file.Paths;
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
}
