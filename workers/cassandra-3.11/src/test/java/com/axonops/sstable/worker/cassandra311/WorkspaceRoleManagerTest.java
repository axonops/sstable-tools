package com.axonops.sstable.worker.cassandra311;

import org.apache.cassandra.auth.RoleResource;
import org.junit.Assert;
import org.junit.Test;

public class WorkspaceRoleManagerTest {
    @Test
    public void exposesOnlyFixedNonSuperuserWorkspaceRole() {
        WorkspaceRoleManager roles = new WorkspaceRoleManager();
        RoleResource workspace = RoleResource.role("sstable_workspace");
        RoleResource unrelated = RoleResource.role("cassandra");

        Assert.assertTrue(roles.canLogin(workspace));
        Assert.assertTrue(roles.isExistingRole(workspace));
        Assert.assertEquals(1, roles.getRoles(workspace, true).size());
        Assert.assertEquals(1, roles.getAllRoles().size());
        Assert.assertFalse(roles.isSuper(workspace));

        Assert.assertFalse(roles.canLogin(unrelated));
        Assert.assertFalse(roles.isExistingRole(unrelated));
        Assert.assertTrue(roles.getRoles(unrelated, true).isEmpty());
    }
}
