package com.axonops.sstable.worker.cassandra50;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;

import org.apache.cassandra.auth.CIDRGroupsMappingManager;
import org.apache.cassandra.auth.CIDRPermissions;
import org.apache.cassandra.auth.ICIDRAuthorizer;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.metrics.CIDRAuthorizerMetrics;

/**
 * Keeps the isolated workspace from initializing system_auth for Cassandra 5 CIDR support.
 */
public final class OfflineCIDRAuthorizer implements ICIDRAuthorizer {
    @Override
    public void setup() {
        // No persistent CIDR state is valid in an offline workspace.
    }

    @Override
    public void initCaches() {
        // No caches are required when every local workspace request is allowed.
    }

    @Override
    public CIDRGroupsMappingManager getCidrGroupsMappingManager() {
        return null;
    }

    @Override
    public CIDRAuthorizerMetrics getCidrAuthorizerMetrics() {
        return null;
    }

    @Override
    public boolean requireAuthorization() {
        return false;
    }

    @Override
    public void setCidrGroupsForRole(RoleResource role, CIDRPermissions cidrPermissions) {
        throw unsupported();
    }

    @Override
    public void dropCidrPermissionsForRole(RoleResource role) {
        throw unsupported();
    }

    @Override
    public boolean invalidateCidrPermissionsCache(String roleName) {
        return false;
    }

    @Override
    public void validateConfiguration() {
        // The workspace deliberately has no CIDR authorization configuration.
    }

    @Override
    public void loadCidrGroupsCache() {
        throw unsupported();
    }

    @Override
    public Set<String> lookupCidrGroupsForIp(InetAddress ip) {
        return Collections.emptySet();
    }

    @Override
    public boolean hasAccessFromIp(RoleResource role, InetAddress ipAddress) {
        return true;
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("CIDR authorization is unavailable in an offline workspace");
    }
}
