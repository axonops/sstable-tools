package com.csforge.sstable.worker.api;

import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkerMainTest {
    private static final String CONTROL_TOKEN =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void selfTestReportsReadyForMatchingRuntime() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exitCode = WorkerMain.run(
                new String[]{"--self-test", "--expected-version", "9.9.9"},
                new PrintStream(output, true, "UTF-8"),
                System.err);

        Assert.assertEquals(0, exitCode);
        Assert.assertTrue(output.toString("UTF-8")
                .contains("WORKER_READY protocol=3 release=9.9.9"));
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

    @Test
    public void sandboxPublishesEndpointAndRequiresTokenForStatusAndStop() throws Exception {
        Path workspace = temporary.newFolder("sandbox").toPath().toRealPath();
        Files.createDirectories(workspace.resolve("runtime"));
        Files.createDirectories(workspace.resolve("state"));
        Files.write(workspace.resolve("runtime/cassandra.yaml"),
                "cluster_name: test\n".getBytes(StandardCharsets.UTF_8));
        Files.write(workspace.resolve("state/control.token"),
                CONTROL_TOKEN.getBytes(StandardCharsets.US_ASCII));
        UUID workspaceId = UUID.randomUUID();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Integer> result = executor.submit(() -> WorkerMain.run(new String[]{
                        "--sandbox", "--expected-version", "9.9.9",
                        "--workspace", workspace.toString(),
                        "--workspace-id", workspaceId.toString(),
                        "--native-port", "19042"
                }, new PrintStream(output, true, "UTF-8"),
                new PrintStream(error, true, "UTF-8")));
        try {
            Path endpointPath = workspace.resolve("state/worker.properties");
            WorkerEndpoint endpoint = waitForEndpoint(endpointPath, result, error);
            Assert.assertEquals(WorkerEndpoint.Status.RUNNING, endpoint.status());
            Assert.assertEquals(workspaceId, endpoint.workspaceId());
            Assert.assertEquals("ERROR unauthorized-or-unsupported-command",
                    control(endpoint, "wrong STATUS"));
            Assert.assertEquals("OK RUNNING",
                    control(endpoint, CONTROL_TOKEN + " STATUS"));
            Assert.assertEquals("OK FLUSHED",
                    control(endpoint, CONTROL_TOKEN + " FLUSH"));
            endpoint = WorkerEndpoint.read(endpointPath);
            Assert.assertEquals(WorkerEndpoint.Status.FLUSHED, endpoint.status());
            Assert.assertEquals("OK FLUSHED",
                    control(endpoint, CONTROL_TOKEN + " STATUS"));
            Assert.assertEquals("OK VERIFIED",
                    control(endpoint, CONTROL_TOKEN + " VERIFY"));
            Assert.assertEquals("OK STOPPING",
                    control(endpoint, CONTROL_TOKEN + " STOP"));

            Assert.assertEquals(Integer.valueOf(0), result.get(5, TimeUnit.SECONDS));
            Assert.assertEquals(WorkerEndpoint.Status.STOPPED,
                    WorkerEndpoint.read(endpointPath).status());
            Assert.assertTrue(output.toString("UTF-8").contains("SANDBOX_READY"));
            Assert.assertEquals("", error.toString("UTF-8"));
        } finally {
            executor.shutdownNow();
        }
    }

    private static WorkerEndpoint waitForEndpoint(Path path,
                                                  Future<Integer> result,
                                                  ByteArrayOutputStream error) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(path)) {
                return WorkerEndpoint.read(path);
            }
            if (result.isDone()) {
                throw new AssertionError("Worker exited with " + result.get()
                        + " before publishing endpoint: " + error.toString("UTF-8"));
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Worker endpoint was not published: "
                + error.toString("UTF-8"));
    }

    private static String control(WorkerEndpoint endpoint, String command) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.controlAddress(),
                    endpoint.controlPort()), 2000);
            socket.setSoTimeout(2000);
            OutputStreamWriter output = new OutputStreamWriter(socket.getOutputStream(),
                    StandardCharsets.US_ASCII);
            output.write(command + "\n");
            output.flush();
            return new BufferedReader(new InputStreamReader(socket.getInputStream(),
                    StandardCharsets.US_ASCII)).readLine();
        }
    }

    public static final class FakeAdapter implements SandboxRuntimeAdapter {
        private volatile boolean running;

        @Override
        public String installedVersion() {
            return "9.9.9";
        }

        @Override
        public void verifyLinkage() {
        }

        @Override
        public SandboxHandle startSandbox(SandboxOptions options) {
            running = true;
            return new SandboxHandle() {
                @Override
                public String nativeAddress() {
                    return "127.0.0.1";
                }

                @Override
                public int nativePort() {
                    return options.nativePort();
                }

                @Override
                public boolean isRunning() {
                    return running;
                }

                @Override
                public void flush() {
                    running = false;
                }

                @Override
                public boolean isFlushed() {
                    return !running;
                }

                @Override
                public void verify() {
                }

                @Override
                public boolean isVerified() {
                    return !running;
                }

                @Override
                public void stop() {
                    running = false;
                }
            };
        }
    }
}
