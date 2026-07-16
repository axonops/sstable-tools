package com.csforge.sstable.worker.cassandra311;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkspaceAuthenticatorTest {
    private static final String PASSWORD =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void authenticatesOnlyMatchingLoopbackCredentials() throws Exception {
        Path workspace = temporary.newFolder("workspace-auth").toPath().toRealPath();
        Path state = Files.createDirectory(workspace.resolve("state"));
        Path cqlshrc = Files.write(state.resolve("cqlshrc"), (
                "[authentication]\nusername = sstable_workspace\npassword = "
                        + PASSWORD + "\n").getBytes(StandardCharsets.US_ASCII));
        try {
            Files.setPosixFilePermissions(cqlshrc,
                    PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // The implementation also supports ACL-based non-POSIX workspaces.
        }

        String previous = System.getProperty("cassandra.storagedir");
        System.setProperty("cassandra.storagedir", workspace.toString());
        try {
            DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
            WorkspaceAuthenticator authenticator = new WorkspaceAuthenticator();
            authenticator.validateConfiguration();
            authenticator.setup();

            IAuthenticator.SaslNegotiator accepted = authenticator.newSaslNegotiator(
                    InetAddress.getByName("127.0.0.1"));
            accepted.evaluateResponse(plain("sstable_workspace", PASSWORD));
            Assert.assertTrue(accepted.isComplete());
            AuthenticatedUser user = accepted.getAuthenticatedUser();
            Assert.assertEquals("sstable_workspace", user.getName());

            IAuthenticator.SaslNegotiator rejected = authenticator.newSaslNegotiator(
                    InetAddress.getByName("127.0.0.1"));
            rejected.evaluateResponse(plain("sstable_workspace", "wrong"));
            try {
                rejected.getAuthenticatedUser();
                Assert.fail("Wrong workspace password was accepted");
            } catch (AuthenticationException expected) {
                Assert.assertTrue(expected.getMessage().contains("incorrect"));
            }

            IAuthenticator.SaslNegotiator remote = authenticator.newSaslNegotiator(
                    InetAddress.getByName("192.0.2.1"));
            try {
                remote.evaluateResponse(plain("sstable_workspace", PASSWORD));
                Assert.fail("Non-loopback authentication was accepted");
            } catch (AuthenticationException expected) {
                Assert.assertTrue(expected.getMessage().contains("loopback"));
            }
        } finally {
            if (previous == null) {
                System.clearProperty("cassandra.storagedir");
            } else {
                System.setProperty("cassandra.storagedir", previous);
            }
        }
    }

    private static byte[] plain(String username, String password) {
        return ("\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8);
    }
}
