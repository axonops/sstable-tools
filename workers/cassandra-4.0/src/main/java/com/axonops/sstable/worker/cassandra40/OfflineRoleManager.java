package com.axonops.sstable.worker.cassandra40;

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

/** No-op role manager for offline import workers with no client transport. */
public final class OfflineRoleManager implements IRoleManager {
    @Override public Set<Option> supportedOptions() { return Collections.emptySet(); }
    @Override public Set<Option> alterableOptions() { return Collections.emptySet(); }
    @Override public void createRole(AuthenticatedUser p, RoleResource r, RoleOptions o)
            throws RequestValidationException, RequestExecutionException { throw immutable(); }
    @Override public void dropRole(AuthenticatedUser p, RoleResource r)
            throws RequestValidationException, RequestExecutionException { throw immutable(); }
    @Override public void alterRole(AuthenticatedUser p, RoleResource r, RoleOptions o)
            throws RequestValidationException, RequestExecutionException { throw immutable(); }
    @Override public void grantRole(AuthenticatedUser p, RoleResource r, RoleResource g)
            throws RequestValidationException, RequestExecutionException { throw immutable(); }
    @Override public void revokeRole(AuthenticatedUser p, RoleResource r, RoleResource v)
            throws RequestValidationException, RequestExecutionException { throw immutable(); }
    @Override public Set<RoleResource> getRoles(RoleResource r, boolean inherited) {
        return Collections.emptySet();
    }
    @Override public Set<RoleResource> getAllRoles() { return Collections.emptySet(); }
    @Override public boolean isSuper(RoleResource r) { return false; }
    @Override public boolean canLogin(RoleResource r) { return false; }
    @Override public Map<String, String> getCustomOptions(RoleResource r) {
        return Collections.emptyMap();
    }
    @Override public boolean isExistingRole(RoleResource r) { return false; }
    @Override public Set<? extends IResource> protectedResources() { return Collections.emptySet(); }
    @Override public void validateConfiguration() throws ConfigurationException { }
    @Override public void setup() { }

    private static InvalidRequestException immutable() {
        return new InvalidRequestException("Offline import roles are immutable");
    }
}
