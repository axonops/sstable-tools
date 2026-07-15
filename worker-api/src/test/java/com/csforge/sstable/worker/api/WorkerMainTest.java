package com.csforge.sstable.worker.api;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

public class WorkerMainTest {
    @Test
    public void selfTestReportsReadyForMatchingRuntime() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exitCode = WorkerMain.run(
                new String[]{"--self-test", "--expected-version", "9.9.9"},
                new PrintStream(output, true, "UTF-8"),
                System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8")
                .contains("WORKER_READY protocol=1 release=9.9.9"));
    }

    @Test
    public void selfTestRejectsClasspathVersionMismatch() throws Exception {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = WorkerMain.run(
                new String[]{"--self-test", "--expected-version", "9.9.8"},
                System.out,
                new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(4, exitCode);
        Assert.assertTrue(error.toString("UTF-8").contains("worker loaded 9.9.9"));
    }

    @Test
    public void selfTestRejectsInvalidArguments() throws Exception {
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = WorkerMain.run(
                new String[]{"--self-test"},
                System.out,
                new PrintStream(error, true, "UTF-8"));

        Assert.assertEquals(2, exitCode);
    }

    public static final class FakeAdapter implements RuntimeAdapter {
        @Override
        public String installedVersion() {
            return "9.9.9";
        }

        @Override
        public void verifyLinkage() {
        }
    }
}
