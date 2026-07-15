package com.csforge.sstable.bootstrap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

public class BootstrapMainTest {
    @Test
    public void helpDoesNotRequireCassandraClasses() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(
                new String[]{"--help"},
                new PrintStream(output, true, "UTF-8"),
                System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8").contains("--cassandra-home"));
        Assert.assertTrue(output.toString("UTF-8").contains("runtime preflight"));
    }

    @Test
    public void invalidCommandReturnsUsageErrorWithoutLoadingCassandra() throws Exception {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = BootstrapMain.run(
                new String[]{"workspace", "start"},
                System.out,
                new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(BootstrapException.USAGE_EXIT_CODE, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("runtime inspect"));
    }
}
