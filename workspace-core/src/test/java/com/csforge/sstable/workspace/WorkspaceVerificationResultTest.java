package com.csforge.sstable.workspace;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceVerificationResultTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void persistsStrictEvidenceBoundToFlush() throws Exception {
        Path workspace = temporary.newFolder("verification").toPath().toRealPath();
        Files.createDirectory(workspace.resolve("state"));
        UUID workspaceId = UUID.randomUUID();
        WorkspaceVerificationResult result = new WorkspaceVerificationResult(
                workspaceId, "3.11.19", "blog", "users", hash('a'),
                Instant.parse("2026-07-16T12:00:00Z"), 3, 2, 42,
                Collections.singletonList("mc"),
                Arrays.asList("mc-3-big", "mc-2-big"));

        result.writeAtomically(workspace);
        WorkspaceVerificationResult restored = WorkspaceVerificationResult.read(workspace);
        restored.requireIdentity(workspaceId, "3.11.19", "blog", "users", hash('a'));

        Assert.assertEquals(3, restored.liveSstables());
        Assert.assertEquals(2, restored.deltaSstables());
        Assert.assertEquals(42, restored.logicalRows());
        Assert.assertEquals(Arrays.asList("mc-2-big", "mc-3-big"),
                restored.deltaDescriptors());
        try {
            restored.requireIdentity(workspaceId, "3.11.19", "blog", "users", hash('b'));
            Assert.fail("Expected a different flush hash to be rejected");
        } catch (WorkspaceException expected) {
            Assert.assertTrue(expected.getMessage().contains("committed flush"));
        }
    }

    @Test
    public void rejectsUnknownFieldsAndBroadPermissions() throws Exception {
        Path workspace = temporary.newFolder("invalid-verification").toPath().toRealPath();
        Path state = Files.createDirectory(workspace.resolve("state"));
        Path result = state.resolve("verification-result.properties");
        Files.write(result, ("version=1\nunknown=true\n").getBytes(StandardCharsets.US_ASCII));
        try {
            Files.setPosixFilePermissions(result,
                    PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The implementation also supports ACL-based non-POSIX workspaces.
        }
        assertReadFailure(workspace, "fields are invalid");

        WorkspaceVerificationResult valid = new WorkspaceVerificationResult(
                UUID.randomUUID(), "3.11.19", "blog", "users", hash('c'), Instant.now(),
                1, 0, 0, Collections.<String>emptyList(),
                Collections.<String>emptyList());
        valid.writeAtomically(workspace);
        try {
            Files.setPosixFilePermissions(result,
                    PosixFilePermissions.fromString("rw-r--r--"));
            assertReadFailure(workspace, "owner-only");
        } catch (UnsupportedOperationException ignored) {
            // Permission enforcement is POSIX-specific.
        }
    }

    private static void assertReadFailure(Path workspace, String expected) throws Exception {
        try {
            WorkspaceVerificationResult.read(workspace);
            Assert.fail("Expected verification result failure containing " + expected);
        } catch (WorkspaceException failure) {
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(expected)
                    || failure.getCause() != null
                    && failure.getCause().getMessage().contains(expected));
        }
    }

    private static String hash(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }
}
