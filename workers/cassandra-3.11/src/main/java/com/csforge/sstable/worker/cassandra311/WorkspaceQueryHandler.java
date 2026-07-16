package com.csforge.sstable.worker.cassandra311;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

/** Native-protocol policy boundary for one imported workspace table. */
public final class WorkspaceQueryHandler implements QueryHandler {
    static final String HANDLER_PROPERTY = "cassandra.custom_query_handler_class";
    static final String KEYSPACE_PROPERTY = "sstable.tools.workspace.keyspace";
    static final String TABLE_PROPERTY = "sstable.tools.workspace.table";
    static final String POLICY_PREFIX = "SSTABLE_TOOLS_POLICY: ";

    private final QueryHandler delegate = QueryProcessor.instance;
    private final String keyspace;
    private final String table;

    public WorkspaceQueryHandler() {
        keyspace = requiredProperty(KEYSPACE_PROPERTY);
        table = requiredProperty(TABLE_PROPERTY);
    }

    @Override
    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload,
                                 long queryStartNanoTime)
            throws RequestExecutionException, RequestValidationException {
        ParsedStatement.Prepared parsed = QueryProcessor.parseStatement(query, state);
        requireAllowed(parsed.statement);
        return delegate.process(query, state, options, customPayload, queryStartNanoTime);
    }

    @Override
    public ResultMessage.Prepared prepare(String query,
                                          QueryState state,
                                          Map<String, ByteBuffer> customPayload)
            throws RequestValidationException {
        ParsedStatement.Prepared parsed = QueryProcessor.parseStatement(query, state);
        requireAllowed(parsed.statement);
        return delegate.prepare(query, state, customPayload);
    }

    @Override
    public ParsedStatement.Prepared getPrepared(MD5Digest id) {
        return delegate.getPrepared(id);
    }

    @Override
    public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
        return delegate.getPreparedForThrift(id);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime)
            throws RequestExecutionException, RequestValidationException {
        requireAllowed(statement);
        return delegate.processPrepared(statement, state, options, customPayload,
                queryStartNanoTime);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime) throws InvalidRequestException {
        throw rejected("BATCH statements are disabled");
    }

    private void requireAllowed(CQLStatement statement) throws InvalidRequestException {
        if (statement instanceof UseStatement) {
            return;
        }
        if (statement instanceof SelectStatement) {
            CFMetaData metadata = ((SelectStatement) statement).cfm;
            if (isWorkspaceTable(metadata) || isRequiredSystemRead(metadata)) {
                return;
            }
            throw rejected("SELECT is limited to " + keyspace + "." + table
                    + " and cqlsh metadata tables");
        }
        if (statement instanceof DeleteStatement) {
            throw rejected("DELETE statements are disabled");
        }
        if (statement instanceof UpdateStatement) {
            ModificationStatement modification = (ModificationStatement) statement;
            if (!isWorkspaceTable(modification.cfm)) {
                throw rejected("writes are limited to " + keyspace + "." + table);
            }
            if (modification.hasConditions()) {
                throw rejected("conditional INSERT and UPDATE statements are disabled");
            }
            if (modification.isCounter()) {
                throw rejected("counter mutations are disabled");
            }
            return;
        }
        if (statement instanceof ModificationStatement) {
            throw rejected("only non-conditional INSERT and UPDATE mutations are allowed");
        }
        throw rejected(statement.getClass().getSimpleName() + " statements are disabled");
    }

    private boolean isWorkspaceTable(CFMetaData metadata) {
        return keyspace.equals(metadata.ksName) && table.equals(metadata.cfName);
    }

    private static boolean isRequiredSystemRead(CFMetaData metadata) {
        return "system_schema".equals(metadata.ksName)
                || ("system".equals(metadata.ksName)
                    && ("local".equals(metadata.cfName)
                        || "peers".equals(metadata.cfName)));
    }

    private static InvalidRequestException rejected(String reason) {
        return new InvalidRequestException(POLICY_PREFIX + reason);
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(name + " is required by the workspace query guard");
        }
        return value;
    }
}
