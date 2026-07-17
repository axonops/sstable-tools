package com.axonops.sstable.worker.cassandra311;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.service.QueryState;

/** Cassandra-parsed schema bundle constrained to one keyspace and one table. */
final class CqlSchemaBundle {
    private final String keyspace;
    private final String table;

    private CqlSchemaBundle(String keyspace,
                            String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    static CqlSchemaBundle install(Path path) throws Exception {
        String cql = decodeUtf8(Files.readAllBytes(path));
        List<String> statements = split(cql);
        String keyspace = null;
        String tableStatement = null;
        List<String> types = new ArrayList<>();

        for (String statement : statements) {
            ParsedStatement parsed = QueryProcessor.parseStatement(statement);
            if (parsed instanceof CreateKeyspaceStatement) {
                if (keyspace != null) {
                    throw new IllegalArgumentException("Schema bundle must declare exactly one "
                            + "keyspace");
                }
                keyspace = ((CreateKeyspaceStatement) parsed).keyspace();
            } else if (parsed instanceof CreateTypeStatement) {
                types.add(statement);
            } else if (parsed instanceof CreateTableStatement.RawStatement) {
                if (tableStatement != null) {
                    throw new IllegalArgumentException("Schema bundle must declare exactly one "
                            + "table");
                }
                tableStatement = statement;
            } else {
                throw new IllegalArgumentException("Unsupported schema statement: "
                        + parsed.getClass().getSimpleName());
            }
        }
        if (keyspace == null || tableStatement == null) {
            throw new IllegalArgumentException("Schema bundle must declare exactly one keyspace "
                    + "and exactly one table");
        }

        QueryProcessor.executeInternal("CREATE KEYSPACE IF NOT EXISTS "
                + quoteIdentifier(keyspace)
                + " WITH replication = {'class': 'SimpleStrategy', "
                + "'replication_factor': '1'} AND durable_writes = true");
        for (String type : types) {
            ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(
                    addIfNotExists(type, "TYPE"), QueryState.forInternalCalls());
            if (!(prepared.statement instanceof CreateTypeStatement)
                    || !keyspace.equals(((CreateTypeStatement) prepared.statement).keyspace())) {
                throw new IllegalArgumentException("Every UDT must be explicitly declared in "
                        + "keyspace " + keyspace);
            }
            prepared.statement.executeInternal(QueryState.forInternalCalls(),
                    QueryProcessor.makeInternalOptions(prepared, new Object[0]));
        }

        String idempotentTable = addIfNotExists(tableStatement, "TABLE");
        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(
                idempotentTable, QueryState.forInternalCalls());
        if (!(prepared.statement instanceof CreateTableStatement)) {
            throw new IllegalArgumentException("Schema bundle table did not prepare as CREATE "
                    + "TABLE");
        }
        CreateTableStatement createTable = (CreateTableStatement) prepared.statement;
        if (!keyspace.equals(createTable.keyspace())) {
            throw new IllegalArgumentException("Table must be explicitly declared in keyspace "
                    + keyspace);
        }
        createTable.executeInternal(QueryState.forInternalCalls(),
                QueryProcessor.makeInternalOptions(prepared, new Object[0]));
        if (Schema.instance.getCFMetaData(keyspace, createTable.columnFamily()) == null) {
            throw new IllegalStateException("Cassandra did not install the declared table");
        }
        return new CqlSchemaBundle(keyspace, createTable.columnFamily());
    }

    private static String addIfNotExists(String statement, String object) {
        String upper = statement.toUpperCase(Locale.ROOT);
        String marker = "CREATE " + object;
        int start = firstToken(statement);
        if (!upper.startsWith(marker, start)) {
            throw new IllegalArgumentException("Expected " + marker + " statement");
        }
        int insertion = start + marker.length();
        String remainder = statement.substring(insertion).trim();
        if (remainder.toUpperCase(Locale.ROOT).startsWith("IF NOT EXISTS ")) {
            return statement;
        }
        return statement.substring(0, insertion) + " IF NOT EXISTS"
                + statement.substring(insertion);
    }

    private static int firstToken(String statement) {
        int index = 0;
        while (index < statement.length() && Character.isWhitespace(statement.charAt(index))) {
            index++;
        }
        return index;
    }

    static List<String> split(String cql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean singleQuoted = false;
        boolean doubleQuoted = false;
        boolean lineComment = false;
        boolean blockComment = false;

        for (int i = 0; i < cql.length(); i++) {
            char ch = cql.charAt(i);
            char next = i + 1 < cql.length() ? cql.charAt(i + 1) : '\0';
            if (lineComment) {
                if (ch == '\n' || ch == '\r') {
                    lineComment = false;
                    current.append(' ');
                }
                continue;
            }
            if (blockComment) {
                if (ch == '*' && next == '/') {
                    blockComment = false;
                    current.append(' ');
                    i++;
                }
                continue;
            }
            if (!singleQuoted && !doubleQuoted && ch == '-' && next == '-') {
                lineComment = true;
                i++;
                continue;
            }
            if (!singleQuoted && !doubleQuoted && ch == '/' && next == '/') {
                lineComment = true;
                i++;
                continue;
            }
            if (!singleQuoted && !doubleQuoted && ch == '/' && next == '*') {
                blockComment = true;
                i++;
                continue;
            }
            if (!doubleQuoted && ch == '\'') {
                current.append(ch);
                if (singleQuoted && next == '\'') {
                    current.append(next);
                    i++;
                } else {
                    singleQuoted = !singleQuoted;
                }
                continue;
            }
            if (!singleQuoted && ch == '"') {
                current.append(ch);
                if (doubleQuoted && next == '"') {
                    current.append(next);
                    i++;
                } else {
                    doubleQuoted = !doubleQuoted;
                }
                continue;
            }
            if (!singleQuoted && !doubleQuoted && ch == ';') {
                addStatement(statements, current);
            } else {
                current.append(ch);
            }
        }
        if (singleQuoted || doubleQuoted || blockComment) {
            throw new IllegalArgumentException("Unterminated quote or comment in schema bundle");
        }
        addStatement(statements, current);
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("Schema bundle is empty");
        }
        return statements;
    }

    private static void addStatement(List<String> statements, StringBuilder current) {
        String statement = current.toString().trim();
        current.setLength(0);
        if (!statement.isEmpty()) {
            statements.add(statement);
        }
    }

    private static String decodeUtf8(byte[] bytes) throws IOException {
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return decoded.toString();
        } catch (CharacterCodingException e) {
            throw new IOException("Schema bundle is not valid UTF-8", e);
        }
    }

    private static String quoteIdentifier(String identifier) {
        if (identifier.matches("[a-z][a-z0-9_]*")) {
            return identifier;
        }
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    String keyspace() {
        return keyspace;
    }

    String table() {
        return table;
    }
}
