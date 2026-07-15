package com.csforge.sstable.worker.cassandra311;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class CqlSchemaBundleTest {
    @Test
    public void splitsCommentsAndQuotedSemicolonsWithoutChangingStatements() {
        List<String> statements = CqlSchemaBundle.split(
                "-- bundle header\n"
                        + "CREATE KEYSPACE blog WITH replication = {'class': "
                        + "'Simple;Strategy'};\n"
                        + "/* table comment ; */ CREATE TABLE blog.users (\n"
                        + "  id text PRIMARY KEY, value text\n"
                        + ") WITH comment = 'semi;colon'; // trailing comment\n");

        Assert.assertEquals(2, statements.size());
        Assert.assertTrue(statements.get(0).contains("'Simple;Strategy'"));
        Assert.assertTrue(statements.get(1).contains("'semi;colon'"));
        Assert.assertFalse(statements.get(0).contains("bundle header"));
        Assert.assertFalse(statements.get(1).contains("table comment"));
    }

    @Test
    public void rejectsUnterminatedQuotesAndComments() {
        assertInvalid("CREATE TABLE blog.users (id text PRIMARY KEY) "
                + "WITH comment = 'unterminated");
        assertInvalid("/* unterminated");
    }

    private static void assertInvalid(String cql) {
        try {
            CqlSchemaBundle.split(cql);
            Assert.fail("Expected invalid schema bundle");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Unterminated"));
        }
    }
}
