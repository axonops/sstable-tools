# Cassandra 3.11 SSTable import implementation

## Status and scope

The Cassandra 3.11 worker implements the first real schema-validation and
copy-based import path. It is compiled against the final Cassandra 3.11.19
artifact and runs with the selected installed 3.11.19 distribution on JDK 8.
The importer recognizes Big `ma`, `mb`, and `mc` descriptors with `Data.db`,
`Index.db`, `Statistics.db`, `TOC.txt`, and the version-selected digest
component present. Real successful-import coverage currently includes the
repository's `ma` and `mc` fixtures. The repository's `mb` fixture explicitly
uses `LocalPartitioner` and is a partitioner-rejection fixture, not evidence of
a compatible `mb` import.

This is not yet a production-ready write workflow. The query guard, explicit
flush/export, live-data-directory detection, multi-fixture matrix, and other
Cassandra release adapters remain open work.

## Process boundary

The Cassandra-free controller captures the immutable schema/source identities,
writes an import-only `cassandra.yaml`, and launches the same thin JAR in a
child JVM. Cassandra and its transitive dependencies come from the installed
distribution. The import child is configured with gossip, ring join, saved ring
state, Thrift, internode messaging, JMX, and native transport disabled.

The child writes one strict atomic `state/import-result.properties` handoff.
The controller validates its workspace UUID, Cassandra release, source-set
count, table directory confinement, compaction state, and native-transport
state before committing the manifest transition to `IMPORTED`.

## Schema contract

The UTF-8 bundle allows one `CREATE KEYSPACE`, zero or more `CREATE TYPE`
statements in dependency order, and exactly one fully qualified `CREATE TABLE`.
Comments and quoted semicolons are handled by a quote-aware statement splitter;
each statement is then parsed and prepared by Cassandra's installed CQL parser.

The source keyspace name is retained, but the worker creates it locally with
`SimpleStrategy` and replication factor one. Schema statements are made
idempotent for recovery. The manifest records the original bundle path, size,
SHA-256, sandbox table ID, partitioner, table directory, source/live SSTable
counts, and logical imported row count.

## Validation and import order

1. Reverify every source component against its captured size and SHA-256.
2. Install the constrained local schema with native transport still disabled.
3. Require a compatible Big `ma`, `mb`, or `mc` descriptor and all mandatory
   components.
4. Deserialize validation and serialization-header metadata.
5. Compare every explicit validation partitioner, partition-key type,
   clustering types/order, and exact static/regular column name-to-type maps
   with the installed table metadata. Cassandra-compatible legacy metadata
   without a partitioner field proceeds through full digest/read validation.
6. Validate the complete data digest, open the SSTable in offline validation
   mode, and consume every partition and row.
7. Disable automatic compaction and prove that it remains disabled.
8. Copy every component to workspace-owned staging, `fsync` it, reverify the
   source, check all destination names for collisions, and atomically publish
   components into the table directory. Hard links are never used.
9. Invoke Cassandra 3.11 `ColumnFamilyStore.loadNewSSTables()`, which assigns a
   live generation and opens the copied set. Exactly one new live SSTable must
   appear per source set.
10. Run Cassandra extended verification, execute a logical `COUNT(*)`, reverify
    sources, and return the result with native transport still disabled.
11. Drain Cassandra, capture the checksummed table baseline in the controller,
    and atomically commit `IMPORTED`.

## Failure and recovery

All source sets are validated before the first table component is published.
Staged files use unique workspace paths. A detected destination collision
occurs before any component move. Handled publication failures remove moved
components, and any import failure removes only the workspace-owned user table
directory; Cassandra system-schema files remain available for an idempotent
retry. The controller records `FAILED_RECOVERABLE` before rechecking source and
schema integrity.

The real integration profile proves that an on-disk type mismatch and an
explicit partitioner mismatch leave no user `Data.db`, and that a destination
collision leaves no partial table directory and can be retried after recovery.
Source hashes remain unchanged, and a production Cassandra daemon using the
same installed distribution remains queryable with no worker peer. Success
paths import the repository's genuine `ma-2-big` and `mc-1-big` fixtures. The
`ma` path reads its row through stock cqlsh, applies `INSERT` and `UPDATE`, kills
the worker, restarts it, and verifies private commit-log replay.

## Manifest baseline

The baseline inventory contains every regular file in the imported table
directory after Cassandra has loaded and verified the copies. Later lifecycle
commands require every baseline path, size, and SHA-256 to remain unchanged.
Additional files are allowed so flushed mutation SSTables can be classified as
delta output without treating them as baseline corruption.
