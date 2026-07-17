package com.axonops.sstable.workspace;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;

/** Held exclusive OS lock for one workspace controller operation. */
public final class WorkspaceLock implements AutoCloseable {
    private final Path workspaceRoot;
    private final FileChannel channel;
    private final FileLock lock;
    private boolean closed;

    WorkspaceLock(Path workspaceRoot, FileChannel channel, FileLock lock) {
        this.workspaceRoot = workspaceRoot;
        this.channel = channel;
        this.lock = lock;
    }

    boolean isHeldFor(Path root) {
        return !closed && workspaceRoot.equals(root) && lock.isValid();
    }

    @Override
    public void close() throws WorkspaceException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            lock.release();
        } catch (IOException e) {
            failure = e;
        }
        try {
            channel.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw new WorkspaceException("Cannot release workspace lock for " + workspaceRoot,
                    failure);
        }
    }
}
