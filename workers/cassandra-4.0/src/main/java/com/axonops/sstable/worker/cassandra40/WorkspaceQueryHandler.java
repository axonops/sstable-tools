package com.axonops.sstable.worker.cassandra40;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

/** Native-protocol policy boundary for the Cassandra 4.0 workspace sandbox. */
public final class WorkspaceQueryHandler implements QueryHandler {
    static final String HANDLER_PROPERTY = "cassandra.custom_query_handler_class";
    static final String KEYSPACE_PROPERTY = "sstable.tools.workspace.keyspace";
    static final String TABLE_PROPERTY = "sstable.tools.workspace.table";
    static final String POLICY_PREFIX = "SSTABLE_TOOLS_POLICY: ";

    private final QueryHandler delegate = QueryProcessor.instance;
    private final String keyspace = requiredProperty(KEYSPACE_PROPERTY);
    private final String table = requiredProperty(TABLE_PROPERTY);

    @Override
    public CQLStatement parse(String query, QueryState state, QueryOptions options) {
        CQLStatement statement = delegate.parse(query, state, options);
        try {
            requireAllowed(statement);
        } catch (InvalidRequestException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        return statement;
    }

    @Override
    public ResultMessage process(CQLStatement statement,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload,
                                 long queryStartNanoTime)
            throws RequestExecutionException, RequestValidationException {
        requireAllowed(statement);
        requireLocalConsistency(options);
        return delegate.process(statement, state, options, customPayload, queryStartNanoTime);
    }

    @Override
    public ResultMessage.Prepared prepare(String query,
                                          ClientState state,
                                          Map<String, ByteBuffer> customPayload)
            throws RequestValidationException {
        return delegate.prepare(query, state, customPayload);
    }

    @Override
    public QueryHandler.Prepared getPrepared(MD5Digest id) {
        return delegate.getPrepared(id);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime)
            throws RequestExecutionException, RequestValidationException {
        requireAllowed(statement);
        requireLocalConsistency(options);
        return delegate.processPrepared(statement, state, options, customPayload,
                queryStartNanoTime);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime)
            throws RequestExecutionException, RequestValidationException {
        throw rejected("BATCH statements are disabled");
    }

    private void requireAllowed(CQLStatement statement) throws InvalidRequestException {
        if (statement instanceof UseStatement) {
            return;
        }
        if (statement instanceof SelectStatement) {
            TableMetadata metadata = ((SelectStatement) statement).table;
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
            if (!isWorkspaceTable(modification.metadata)) {
                throw rejected("writes are limited to " + keyspace + "." + table);
            }
            if (modification.hasConditions()) {
                throw rejected("conditional INSERT and UPDATE statements are disabled");
            }
            if (modification.isCounter()) {
                throw rejected("counter mutations are disabled");
            }
            if (!modification.isTimestampSet()) {
                throw rejected("INSERT and UPDATE require USING TIMESTAMP");
            }
            return;
        }
        throw rejected(statement.getClass().getSimpleName() + " statements are disabled");
    }

    private boolean isWorkspaceTable(TableMetadata metadata) {
        return keyspace.equals(metadata.keyspace) && table.equals(metadata.name);
    }

    private static boolean isRequiredSystemRead(TableMetadata metadata) {
        return "system_schema".equals(metadata.keyspace)
                || ("system".equals(metadata.keyspace)
                && ("local".equals(metadata.name) || "peers".equals(metadata.name)));
    }

    private static void requireLocalConsistency(QueryOptions options)
            throws InvalidRequestException {
        ConsistencyLevel consistency = options.getConsistency();
        if (consistency != ConsistencyLevel.ONE && consistency != ConsistencyLevel.LOCAL_ONE) {
            throw rejected("consistency level " + consistency + " is disabled; use ONE or LOCAL_ONE");
        }
    }

    private static InvalidRequestException rejected(String reason) {
        return new InvalidRequestException(POLICY_PREFIX + reason);
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required property " + name);
        }
        return value;
    }
}
