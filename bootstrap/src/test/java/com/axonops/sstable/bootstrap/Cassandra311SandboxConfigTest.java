package com.axonops.sstable.bootstrap;

import com.axonops.sstable.workspace.SourceInventory;
import com.axonops.sstable.workspace.WorkspaceLock;
import com.axonops.sstable.workspace.WorkspaceManifest;
import com.axonops.sstable.workspace.WorkspaceRepository;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Cassandra311SandboxConfigTest {
    private static final String TOKEN =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String PASSWORD =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void writesOnlyLoopbackAndWorkspaceOwnedConfiguration() throws Exception {
        Path source = temporary.newFolder("source").toPath();
        Files.write(source.resolve("mc-1-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("mc-1-big-Data.db"), new byte[]{1});
        Files.write(source.resolve("mc-1-big-Statistics.db"), new byte[]{2});
        Path root = temporary.newFolder("workspace").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(SourceInventory.capture(
                Collections.singletonList(source)));

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            Cassandra311SandboxConfig.write(repository, lock, manifest.workspaceId(),
                    19042, TOKEN, PASSWORD);
        }

        String yaml = new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONFIG_PATH)), StandardCharsets.UTF_8);
        Assert.assertTrue(yaml.contains("listen_address: 127.0.0.1"));
        Assert.assertTrue(yaml.contains("start_rpc: false"));
        Assert.assertTrue(yaml.contains("start_native_transport: true"));
        Assert.assertTrue(yaml.contains("authenticator: "
                + "com.axonops.sstable.worker.cassandra311.WorkspaceAuthenticator"));
        Assert.assertTrue(yaml.contains("role_manager: "
                + "com.axonops.sstable.worker.cassandra311.WorkspaceRoleManager"));
        Assert.assertTrue(yaml.contains("internode_authenticator: "
                + "org.apache.cassandra.auth.AllowAllInternodeAuthenticator"));
        Assert.assertTrue(yaml.contains("native_transport_port: 19042"));
        Assert.assertTrue(yaml.contains(root.toRealPath().resolve("data").toString()));
        Assert.assertTrue(yaml.contains(root.toRealPath().resolve("commitlog").toString()));
        Assert.assertFalse(yaml.contains("/var/lib/cassandra"));
        Assert.assertEquals(TOKEN + "\n", new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONTROL_TOKEN_PATH)),
                StandardCharsets.US_ASCII));
        Assert.assertEquals("[authentication]\nusername = sstable_workspace\npassword = "
                        + PASSWORD + "\n",
                new String(Files.readAllBytes(root.resolve(
                        Cassandra311SandboxConfig.CQLSHRC_PATH)),
                        StandardCharsets.US_ASCII));
    }

    @Test
    public void importConfigurationKeepsNativeTransportDisabled() throws Exception {
        Path source = temporary.newFolder("import-source").toPath();
        Files.write(source.resolve("mc-1-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("mc-1-big-Data.db"), new byte[]{1});
        Files.write(source.resolve("mc-1-big-Statistics.db"), new byte[]{2});
        Path root = temporary.newFolder("import-workspace").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(SourceInventory.capture(
                Collections.singletonList(source)));

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            Cassandra311SandboxConfig.writeImport(repository, lock, manifest.workspaceId(),
                    19042);
        }

        String yaml = new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONFIG_PATH)), StandardCharsets.UTF_8);
        Assert.assertTrue(yaml.contains("start_native_transport: false"));
        Assert.assertTrue(yaml.contains("authenticator: AllowAllAuthenticator"));
        Assert.assertTrue(yaml.contains("role_manager: CassandraRoleManager"));
        Assert.assertFalse(Files.exists(root.resolve(
                Cassandra311SandboxConfig.CONTROL_TOKEN_PATH)));
        Assert.assertFalse(Files.exists(root.resolve(
                Cassandra311SandboxConfig.CQLSHRC_PATH)));
    }

    @Test
    public void cassandra40ImportConfigurationOmitsRemovedThriftProperties() throws Exception {
        Path source = temporary.newFolder("cassandra40-source").toPath();
        Files.write(source.resolve("nb-1-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("nb-1-big-Data.db"), new byte[]{1});
        Files.write(source.resolve("nb-1-big-Statistics.db"), new byte[]{2});
        Path root = temporary.newFolder("cassandra40-workspace").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(SourceInventory.capture(
                Collections.singletonList(source)));

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            Cassandra311SandboxConfig.writeImport(repository, lock, manifest.workspaceId(),
                    19042, "4.0");
        }

        String yaml = new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONFIG_PATH)), StandardCharsets.UTF_8);
        Assert.assertFalse(yaml.contains("start_rpc:"));
        Assert.assertFalse(yaml.contains("rpc_port:"));
        Assert.assertTrue(yaml.contains("role_manager: "
                + "com.axonops.sstable.worker.cassandra40.OfflineRoleManager"));

        try (WorkspaceLock lock = repository.acquire()) {
            Cassandra311SandboxConfig.writeImport(repository, lock, manifest.workspaceId(),
                    19042, "4.1");
        }

        yaml = new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONFIG_PATH)), StandardCharsets.UTF_8);
        Assert.assertFalse(yaml.contains("start_rpc:"));
        Assert.assertFalse(yaml.contains("rpc_port:"));
        Assert.assertTrue(yaml.contains("role_manager: "
                + "com.axonops.sstable.worker.cassandra41.OfflineRoleManager"));

        try (WorkspaceLock lock = repository.acquire()) {
            Cassandra311SandboxConfig.writeImport(repository, lock, manifest.workspaceId(),
                    19042, "5.0");
        }

        yaml = new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONFIG_PATH)), StandardCharsets.UTF_8);
        Assert.assertFalse(yaml.contains("start_rpc:"));
        Assert.assertFalse(yaml.contains("rpc_port:"));
        Assert.assertFalse(yaml.contains("commitlog_sync_batch_window_in_ms:"));
        Assert.assertTrue(yaml.contains("role_manager: "
                + "com.axonops.sstable.worker.cassandra50.OfflineRoleManager"));
        Assert.assertTrue(yaml.contains("cidr_authorizer: "
                + "com.axonops.sstable.worker.cassandra50.OfflineCIDRAuthorizer"));
    }

    @Test
    public void cassandra40SandboxConfigurationUsesLocalNoOpAuthentication() throws Exception {
        Path source = temporary.newFolder("cassandra40-sandbox-source").toPath();
        Files.write(source.resolve("nb-1-big-TOC.txt"), Arrays.asList(
                "TOC.txt", "Data.db", "Statistics.db"), StandardCharsets.UTF_8);
        Files.write(source.resolve("nb-1-big-Data.db"), new byte[]{1});
        Files.write(source.resolve("nb-1-big-Statistics.db"), new byte[]{2});
        Path root = temporary.newFolder("cassandra40-sandbox-workspace").toPath();
        WorkspaceRepository repository = WorkspaceRepository.createAt(root);
        WorkspaceManifest manifest = WorkspaceManifest.create(SourceInventory.capture(
                Collections.singletonList(source)));

        try (WorkspaceLock lock = repository.acquire()) {
            repository.initialize(lock, manifest);
            Cassandra311SandboxConfig.write(repository, lock, manifest.workspaceId(),
                    19042, TOKEN, PASSWORD, "4.0");
        }

        String yaml = new String(Files.readAllBytes(
                root.resolve(Cassandra311SandboxConfig.CONFIG_PATH)), StandardCharsets.UTF_8);
        Assert.assertTrue(yaml.contains("start_native_transport: true"));
        Assert.assertFalse(yaml.contains("start_rpc:"));
        Assert.assertTrue(yaml.contains("authenticator: AllowAllAuthenticator"));
        Assert.assertTrue(yaml.contains("role_manager: "
                + "com.axonops.sstable.worker.cassandra40.OfflineRoleManager"));
    }
}
