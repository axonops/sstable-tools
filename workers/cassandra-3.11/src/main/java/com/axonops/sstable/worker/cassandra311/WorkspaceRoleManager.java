package com.axonops.sstable.worker.cassandra311;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/** Fixed in-memory role used only by the isolated workspace native transport. */
public final class WorkspaceRoleManager implements IRoleManager {
    static final String USERNAME = "sstable_workspace";
    private static final RoleResource WORKSPACE_ROLE = RoleResource.role(USERNAME);

    @Override
    public Set<Option> supportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<Option> alterableOptions() {
        return Collections.emptySet();
    }

    @Override
    public void createRole(AuthenticatedUser performer,
                           RoleResource role,
                           RoleOptions options)
            throws RequestValidationException, RequestExecutionException {
        throw immutable();
    }

    @Override
    public void dropRole(AuthenticatedUser performer, RoleResource role)
            throws RequestValidationException, RequestExecutionException {
        throw immutable();
    }

    @Override
    public void alterRole(AuthenticatedUser performer,
                          RoleResource role,
                          RoleOptions options)
            throws RequestValidationException, RequestExecutionException {
        throw immutable();
    }

    @Override
    public void grantRole(AuthenticatedUser performer,
                          RoleResource role,
                          RoleResource grantee)
            throws RequestValidationException, RequestExecutionException {
        throw immutable();
    }

    @Override
    public void revokeRole(AuthenticatedUser performer,
                           RoleResource role,
                           RoleResource revokee)
            throws RequestValidationException, RequestExecutionException {
        throw immutable();
    }

    @Override
    public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) {
        return isWorkspaceRole(grantee)
                ? Collections.singleton(WORKSPACE_ROLE)
                : Collections.emptySet();
    }

    @Override
    public Set<RoleResource> getAllRoles() {
        return Collections.singleton(WORKSPACE_ROLE);
    }

    @Override
    public boolean isSuper(RoleResource role) {
        return false;
    }

    @Override
    public boolean canLogin(RoleResource role) {
        return isWorkspaceRole(role);
    }

    @Override
    public Map<String, String> getCustomOptions(RoleResource role) {
        return Collections.emptyMap();
    }

    @Override
    public boolean isExistingRole(RoleResource role) {
        return isWorkspaceRole(role);
    }

    @Override
    public Set<? extends IResource> protectedResources() {
        return Collections.emptySet();
    }

    @Override
    public void validateConfiguration() throws ConfigurationException {
    }

    @Override
    public void setup() {
    }

    private static boolean isWorkspaceRole(RoleResource role) {
        return WORKSPACE_ROLE.equals(role);
    }

    private static InvalidRequestException immutable() {
        return new InvalidRequestException("Workspace roles are fixed and cannot be modified");
    }
}
