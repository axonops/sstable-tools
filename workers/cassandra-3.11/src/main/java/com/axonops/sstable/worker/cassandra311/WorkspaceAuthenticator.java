package com.axonops.sstable.worker.cassandra311;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

/** Owner-file credential authenticator for the loopback-only workspace endpoint. */
public final class WorkspaceAuthenticator implements IAuthenticator {
    static final String CQLSHRC_PATH = "state/cqlshrc";
    static final String USERNAME = "sstable_workspace";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final int MAX_SASL_TOKEN_BYTES = 1024;
    private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS = EnumSet.of(
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ,
            PosixFilePermission.OTHERS_WRITE,
            PosixFilePermission.OTHERS_EXECUTE);

    private volatile String password;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public Set<? extends IResource> protectedResources() {
        return Collections.emptySet();
    }

    @Override
    public void validateConfiguration() throws ConfigurationException {
        String storage = System.getProperty("cassandra.storagedir");
        if (storage == null || storage.trim().isEmpty()) {
            throw new ConfigurationException("cassandra.storagedir is required for workspace "
                    + "authentication");
        }
        try {
            Path workspace = Paths.get(storage).toRealPath();
            Path configured = workspace.resolve(CQLSHRC_PATH).normalize();
            if (!configured.startsWith(workspace)
                    || Files.isSymbolicLink(configured)
                    || !Files.isRegularFile(configured, LinkOption.NOFOLLOW_LINKS)
                    || !configured.toRealPath().equals(configured)) {
                throw new ConfigurationException("Workspace credential file is missing or "
                        + "unsafe: " + configured);
            }
            requireOwnerOnly(configured);
            Properties properties = new Properties();
            try (InputStream input = Files.newInputStream(configured)) {
                properties.load(input);
            }
            String username = properties.getProperty(USERNAME_KEY);
            String loadedPassword = properties.getProperty(PASSWORD_KEY);
            if (!USERNAME.equals(username)
                    || loadedPassword == null
                    || !loadedPassword.matches("[0-9a-f]{64}")) {
                throw new ConfigurationException("Workspace credential file has invalid "
                        + "contents");
            }
            password = loadedPassword;
        } catch (IOException e) {
            throw new ConfigurationException("Cannot load workspace credentials", e);
        }
    }

    @Override
    public void setup() {
        requireConfigured();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress) {
        requireConfigured();
        return new PlainNegotiator(clientAddress);
    }

    @Override
    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials)
            throws AuthenticationException {
        requireConfigured();
        return authenticate(credentials.get(USERNAME_KEY), credentials.get(PASSWORD_KEY));
    }

    private AuthenticatedUser authenticate(String username, String suppliedPassword)
            throws AuthenticationException {
        boolean usernameMatches = constantTimeEquals(USERNAME, username);
        boolean passwordMatches = constantTimeEquals(password, suppliedPassword);
        if (!usernameMatches || !passwordMatches) {
            throw new AuthenticationException("Workspace username and/or password are incorrect");
        }
        return new AuthenticatedUser(USERNAME);
    }

    private void requireConfigured() {
        if (password == null) {
            throw new IllegalStateException("Workspace authenticator was not validated");
        }
    }

    private static boolean constantTimeEquals(String expected, String supplied) {
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
        byte[] suppliedBytes = supplied == null
                ? new byte[0] : supplied.getBytes(StandardCharsets.UTF_8);
        return java.security.MessageDigest.isEqual(expectedBytes, suppliedBytes);
    }

    private static void requireOwnerOnly(Path path) throws IOException,
            ConfigurationException {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            if (!permissions.contains(PosixFilePermission.OWNER_READ)
                    || !Collections.disjoint(permissions, FORBIDDEN_PERMISSIONS)) {
                throw new ConfigurationException("Workspace credential file must be readable "
                        + "only by its owner: " + path);
            }
        } catch (UnsupportedOperationException ignored) {
            // Workspace ACLs remain the boundary on non-POSIX filesystems.
        }
    }

    private final class PlainNegotiator implements SaslNegotiator {
        private final InetAddress clientAddress;
        private String username;
        private String suppliedPassword;
        private boolean complete;

        private PlainNegotiator(InetAddress clientAddress) {
            this.clientAddress = clientAddress;
        }

        @Override
        public byte[] evaluateResponse(byte[] response) throws AuthenticationException {
            if (clientAddress == null || !clientAddress.isLoopbackAddress()) {
                throw new AuthenticationException("Workspace authentication requires loopback");
            }
            if (response == null || response.length == 0
                    || response.length > MAX_SASL_TOKEN_BYTES) {
                throw new AuthenticationException("Invalid workspace authentication response");
            }
            int first = indexOf(response, 0, 0);
            int second = indexOf(response, 0, first + 1);
            if (first < 0 || second <= first + 1 || second == response.length - 1
                    || indexOf(response, 0, second + 1) >= 0) {
                throw new AuthenticationException("Invalid workspace authentication response");
            }
            username = new String(Arrays.copyOfRange(response, first + 1, second),
                    StandardCharsets.UTF_8);
            suppliedPassword = new String(Arrays.copyOfRange(response, second + 1,
                    response.length), StandardCharsets.UTF_8);
            complete = true;
            return null;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!complete) {
                throw new AuthenticationException("Workspace authentication is incomplete");
            }
            return authenticate(username, suppliedPassword);
        }
    }

    private static int indexOf(byte[] bytes, int value, int start) {
        if (start < 0) {
            return -1;
        }
        for (int index = start; index < bytes.length; index++) {
            if ((bytes[index] & 0xff) == value) {
                return index;
            }
        }
        return -1;
    }
}
