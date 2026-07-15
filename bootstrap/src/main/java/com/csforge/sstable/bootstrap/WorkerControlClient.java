package com.csforge.sstable.bootstrap;

import com.csforge.sstable.worker.api.WorkerEndpoint;
import com.csforge.sstable.workspace.WorkspaceException;
import com.csforge.sstable.workspace.WorkspaceRepository;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Authenticated loopback client for a running workspace worker. */
final class WorkerControlClient {
    private final long stopTimeoutMillis;

    WorkerControlClient() {
        this(TimeUnit.MINUTES.toMillis(2));
    }

    WorkerControlClient(long stopTimeoutMillis) {
        this.stopTimeoutMillis = stopTimeoutMillis;
    }

    WorkerEndpoint status(WorkspaceRepository repository, UUID workspaceId)
            throws WorkspaceException {
        WorkerEndpoint endpoint = readEndpoint(repository, workspaceId);
        if (endpoint.status() != WorkerEndpoint.Status.RUNNING) {
            throw new WorkspaceException("Worker endpoint is not RUNNING: "
                    + endpoint.status());
        }
        String response = command(repository, endpoint, "STATUS");
        if (!"OK RUNNING".equals(response)) {
            throw new WorkspaceException("Worker status failed: " + response);
        }
        return endpoint;
    }

    WorkerEndpoint stop(WorkspaceRepository repository, UUID workspaceId)
            throws WorkspaceException {
        WorkerEndpoint endpoint = readEndpoint(repository, workspaceId);
        if (endpoint.status() == WorkerEndpoint.Status.STOPPED) {
            return endpoint;
        }
        String response = command(repository, endpoint, "STOP");
        if (!"OK STOPPING".equals(response)) {
            throw new WorkspaceException("Worker stop failed: " + response);
        }

        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(stopTimeoutMillis);
        while (System.nanoTime() < deadline) {
            WorkerEndpoint current = readEndpoint(repository, workspaceId);
            if (current.status() == WorkerEndpoint.Status.STOPPED) {
                return current;
            }
            if (current.status() == WorkerEndpoint.Status.FAILED) {
                throw new WorkspaceException("Worker failed while stopping: "
                        + current.message());
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new WorkspaceException("Interrupted while waiting for worker stop", e);
            }
        }
        throw new WorkspaceException("Timed out waiting for worker to stop");
    }

    private static String command(WorkspaceRepository repository,
                                  WorkerEndpoint endpoint,
                                  String operation) throws WorkspaceException {
        String token = readToken(repository);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.controlAddress(),
                    endpoint.controlPort()), 3000);
            socket.setSoTimeout(5000);
            OutputStreamWriter output = new OutputStreamWriter(socket.getOutputStream(),
                    StandardCharsets.US_ASCII);
            output.write(token + " " + operation + "\n");
            output.flush();
            String response = new BufferedReader(new InputStreamReader(socket.getInputStream(),
                    StandardCharsets.US_ASCII)).readLine();
            if (response == null || response.length() > 1024) {
                throw new WorkspaceException("Worker returned an invalid control response");
            }
            return response;
        } catch (IOException e) {
            throw new WorkspaceException("Cannot contact worker control endpoint at "
                    + endpoint.controlAddress() + ":" + endpoint.controlPort(), e);
        }
    }

    private static String readToken(WorkspaceRepository repository)
            throws WorkspaceException {
        Path path = repository.resolveInside(Cassandra311SandboxConfig.CONTROL_TOKEN_PATH);
        try {
            String token = new String(Files.readAllBytes(path), StandardCharsets.US_ASCII).trim();
            if (!token.matches("[0-9a-f]{64}")) {
                throw new WorkspaceException("Worker control token is invalid");
            }
            return token;
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read worker control token " + path, e);
        }
    }

    static WorkerEndpoint readEndpoint(WorkspaceRepository repository, UUID workspaceId)
            throws WorkspaceException {
        Path path = repository.resolveInside(Cassandra311SandboxConfig.ENDPOINT_PATH);
        try {
            WorkerEndpoint endpoint = WorkerEndpoint.read(path);
            if (!workspaceId.equals(endpoint.workspaceId())) {
                throw new WorkspaceException("Worker endpoint belongs to workspace "
                        + endpoint.workspaceId() + ", not " + workspaceId);
            }
            return endpoint;
        } catch (IOException e) {
            throw new WorkspaceException("Cannot read worker endpoint " + path, e);
        }
    }
}
