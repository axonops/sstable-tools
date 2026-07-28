# System Keyspace Direct CQLSH Design

## Requirement

The direct offline `cqlsh` workflow supports explicitly selected, stopped
SSTables from Cassandra's built-in `system` keyspace. `SELECT`, non-conditional
`INSERT`, and non-conditional `UPDATE` use the same private worker and sibling
publication lifecycle as a user table.

## Schema Resolution

A Cassandra runtime owns the `system` keyspace and rejects `CREATE KEYSPACE`,
`CREATE TABLE`, and `ALTER TABLE` against it. The worker therefore parses the
schema bundle only to identify the fully qualified target table, resolves that
table from the isolated matching runtime's preinstalled metadata, and validates
the selected SSTable headers against the resolved metadata.

The schema bundle may omit `CREATE KEYSPACE` for a fully qualified `system.*`
table. It must contain exactly one `CREATE TABLE` statement and cannot declare
UDTs. Unknown built-in table names fail before source components are copied or
output is published.

User keyspaces retain the existing schema installation path: one keyspace, one
table, optional UDTs, and automatic-compaction configuration created only in
the private worker.

## Safety

The source owner must be stopped. The tool copies only the explicit component
sets to a private workspace, starts a loopback-only isolated worker with gossip,
streaming, and JMX disabled, and verifies source hashes before and after work.
It publishes verified new component sets beside the explicit source only after
a successful cqlsh session and flush.

The worker never changes the installed Cassandra node, its process, or its
live data directory. System-table mutations affect only the isolated worker's
private copy and emitted sibling SSTables.

## Compatibility

The built-in-metadata path is implemented in the 3.11, 4.0, 4.1, and 5.0
adapters. The same release-matched Cassandra installation that reads the input
provides the system-table schema used for validation.
