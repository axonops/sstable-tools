package com.csforge.sstable.worker.cassandra311;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

/** Native-protocol policy boundary for one imported workspace table. */
public final class WorkspaceQueryHandler implements QueryHandler {
    static final String HANDLER_PROPERTY = "cassandra.custom_query_handler_class";
    static final String KEYSPACE_PROPERTY = "sstable.tools.workspace.keyspace";
    static final String TABLE_PROPERTY = "sstable.tools.workspace.table";
    static final String WORKSPACE_ID_PROPERTY = "sstable.tools.workspace.id";
    static final String TIMESTAMP_POLICY_PROPERTY =
            "sstable.tools.workspace.timestamp-policy";
    static final String SOURCE_MAX_TIMESTAMP_PROPERTY =
            "sstable.tools.workspace.source-max-timestamp-micros";
    static final String POLICY_PREFIX = "SSTABLE_TOOLS_POLICY: ";
    private static volatile WorkspaceQueryHandler active;

    private final QueryHandler delegate = QueryProcessor.instance;
    private final String keyspace;
    private final String table;
    private final WorkspaceTimestampAllocator timestampAllocator;
    private final Object requestMonitor = new Object();
    private boolean acceptingRequests = true;
    private int inFlightRequests;

    public WorkspaceQueryHandler() {
        keyspace = requiredProperty(KEYSPACE_PROPERTY);
        table = requiredProperty(TABLE_PROPERTY);
        String policy = requiredProperty(TIMESTAMP_POLICY_PROPERTY);
        long sourceMaximum = requiredLongProperty(SOURCE_MAX_TIMESTAMP_PROPERTY);
        if ("after-source".equals(policy)) {
            timestampAllocator = WorkspaceTimestampAllocator.open(
                    java.nio.file.Paths.get(requiredProperty("cassandra.storagedir")),
                    java.util.UUID.fromString(requiredProperty(WORKSPACE_ID_PROPERTY)),
                    sourceMaximum);
        } else if ("wall-clock".equals(policy)) {
            timestampAllocator = null;
        } else {
            throw new IllegalStateException("Unsupported workspace timestamp policy " + policy);
        }
        active = this;
    }

    @Override
    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload,
                                 long queryStartNanoTime)
            throws RequestExecutionException, RequestValidationException {
        enterRequest();
        try {
            ParsedStatement.Prepared parsed = QueryProcessor.parseStatement(query, state);
            requireAllowed(parsed.statement);
            requireLocalConsistency(options);
            return delegate.process(query, timestampState(parsed.statement, state, options),
                    options, customPayload, queryStartNanoTime);
        } finally {
            exitRequest();
        }
    }

    @Override
    public ResultMessage.Prepared prepare(String query,
                                          QueryState state,
                                          Map<String, ByteBuffer> customPayload)
            throws RequestValidationException {
        enterRequest();
        try {
            ParsedStatement.Prepared parsed = QueryProcessor.parseStatement(query, state);
            requireAllowed(parsed.statement);
            return delegate.prepare(query, state, customPayload);
        } finally {
            exitRequest();
        }
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
        enterRequest();
        try {
            requireAllowed(statement);
            requireLocalConsistency(options);
            return delegate.processPrepared(statement, timestampState(statement, state, options),
                    options, customPayload, queryStartNanoTime);
        } finally {
            exitRequest();
        }
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime) throws InvalidRequestException {
        enterRequest();
        try {
            throw rejected("BATCH statements are disabled");
        } finally {
            exitRequest();
        }
    }

    static void quiesceAndAwaitRequests() throws InterruptedException {
        WorkspaceQueryHandler handler = active;
        if (handler == null) {
            throw new IllegalStateException("Workspace query guard is not active");
        }
        handler.quiesceAndAwait();
    }

    static boolean isQuiesced() {
        WorkspaceQueryHandler handler = active;
        return handler != null && handler.quiesced();
    }

    private void enterRequest() throws InvalidRequestException {
        synchronized (requestMonitor) {
            if (!acceptingRequests) {
                throw rejected("workspace is quiesced for flush");
            }
            inFlightRequests++;
        }
    }

    private void exitRequest() {
        synchronized (requestMonitor) {
            inFlightRequests--;
            if (inFlightRequests == 0) {
                requestMonitor.notifyAll();
            }
        }
    }

    private void quiesceAndAwait() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        synchronized (requestMonitor) {
            acceptingRequests = false;
            while (inFlightRequests > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("Timed out waiting for workspace requests "
                            + "to quiesce");
                }
                TimeUnit.NANOSECONDS.timedWait(requestMonitor, remaining);
            }
        }
    }

    private boolean quiesced() {
        synchronized (requestMonitor) {
            return !acceptingRequests && inFlightRequests == 0;
        }
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

    private QueryState timestampState(CQLStatement statement,
                                      QueryState state,
                                      QueryOptions options)
            throws InvalidRequestException {
        if (timestampAllocator == null || !(statement instanceof ModificationStatement)) {
            return state;
        }
        ModificationStatement modification = (ModificationStatement) statement;
        if (!usesDefaultTimestamp(modification, state.getClientState(), options)) {
            return state;
        }
        return new AllocatingQueryState(state.getClientState(), timestampAllocator);
    }

    private static boolean usesDefaultTimestamp(ModificationStatement statement,
                                                ClientState clientState,
                                                QueryOptions options)
            throws InvalidRequestException {
        long first = statement.getTimestamp(options.getTimestamp(
                new FixedTimestampQueryState(clientState, 0L)), options);
        long second = statement.getTimestamp(options.getTimestamp(
                new FixedTimestampQueryState(clientState, 1L)), options);
        return first != second;
    }

    private static void requireLocalConsistency(QueryOptions options)
            throws InvalidRequestException {
        ConsistencyLevel consistency = options.getConsistency();
        if (consistency != ConsistencyLevel.ONE
                && consistency != ConsistencyLevel.LOCAL_ONE) {
            throw rejected("consistency level " + consistency
                    + " is disabled; use ONE or LOCAL_ONE");
        }
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

    private static long requiredLongProperty(String name) {
        String value = requiredProperty(name);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be a long", e);
        }
    }

    private static final class FixedTimestampQueryState extends QueryState {
        private final long timestamp;

        private FixedTimestampQueryState(ClientState clientState, long timestamp) {
            super(clientState);
            this.timestamp = timestamp;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }
    }

    private static final class AllocatingQueryState extends QueryState {
        private final WorkspaceTimestampAllocator allocator;

        private AllocatingQueryState(ClientState clientState,
                                     WorkspaceTimestampAllocator allocator) {
            super(clientState);
            this.allocator = allocator;
        }

        @Override
        public long getTimestamp() {
            return allocator.next();
        }
    }
}
