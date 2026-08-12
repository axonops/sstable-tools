# Direct CQLSH Design

## Decision

The operator-facing interface is one command, `cqlsh`. It reads an explicit
list of SSTables, one or more complete table directories, or one complete
`--output-dir`; starts no production Cassandra services; and writes only newly
created SSTables into the selected publication directory. When sources span
physical directories, `--sstables` selects all source sets and `--output-dir`
selects one publication target.

```shell
java -jar sstable-tools-cassandra-5.0-0.1.0-SNAPSHOT.jar \
  --cassandra-home /opt/cassandra \
  --sstables /data/acme/users-<table-id>/nb-42-big-Data.db \
  --schema /backups/acme-users.cql \
  cqlsh
```

The command opens the normal stock `cqlsh` from that Cassandra installation.
The user can issue `SELECT`, `INSERT`, and `UPDATE` statements as usual, then
exit `cqlsh`.  The command flushes the changes and places the resulting
complete SSTable component sets beside `nb-42-big-Data.db`.  There is no
`workspace create`, `import`, `start`, `flush`, `export`, or manually managed
temporary directory in this normal workflow.

A read-only session succeeds without creating output components.

When one table spans Cassandra data directories, the same invocation selects
every source and names one publication directory:

```shell
java -jar sstable-tools-cassandra-4.0-0.1.0-SNAPSHOT.jar \
  --cassandra-home /opt/cassandra \
  --sstables /data/system/local-<table-id>/nb-1-big-Data.db \
  --sstables /data/system/local-<table-id>/nb-2-big-Data.db \
  --sstables /log/data/system/local-<table-id>/nb-3-big-Data.db \
  --sstables /log/data/system/local-<table-id>/nb-4-big-Data.db \
  --output-dir /data/system/local-<table-id> \
  --schema /backups/system-local.cql \
  cqlsh --execute "UPDATE system.local USING TIMESTAMP 1785170000000000 SET cluster_name = 'restored' WHERE key = 'local';"
```

For scripts, `--execute` uses the same lifecycle:

```shell
java -jar sstable-tools-cassandra-4.1-0.1.0-SNAPSHOT.jar \
  --cassandra-home /opt/cassandra \
  --sstables /archive/acme/users-<table-id>/na-17-big-Data.db \
  --schema /archive/acme-users.cql \
cqlsh --execute "UPDATE acme.users USING TIMESTAMP 1785170000000000 SET name = 'Ada' WHERE id = 1;"
```

By default, the command creates its private working directory below
`/tmp/sstable-tools/`.  Use `--tmp-dir` to choose a different parent directory,
for example when `/tmp` is too small or needs to be on an encrypted volume:

```shell
java -jar sstable-tools-cassandra-4.1-0.1.0-SNAPSHOT.jar \
  --tmp-dir /var/tmp/sstable-tools \
  --cassandra-home /opt/cassandra \
  --sstables /archive/acme/users-<table-id>/na-17-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh
```

## What The User Supplies

- One or more `Data.db`, `TOC.txt`, or table-directory paths supplied with
  `--sstables`, or one existing non-symlink table directory supplied with
  `--output-dir`. Explicit files expand only through their TOCs.
- `--output-dir` alone inventories every complete SSTable directly in that
  directory. Combined with `--sstables`, it is the publication destination and
  its existing SSTables must be included in the selected source inventory. An
  empty directory is valid for an executed `INSERT`.
- A schema CQL file for that table.
- The matching installed Cassandra home.  Java is inferred from that
  installation when possible, with `--java-home` retained as an override.

All selected source SSTables must describe the same logical table. They may
span Cassandra data directories when an explicit `--output-dir` makes the
publication destination unambiguous. Without `--output-dir`, the selected
sources must remain in one directory and output is published beside them.

## Internal Lifecycle

The tool creates a private temporary workspace itself below `/tmp/sstable-tools/`
by default, or below `--tmp-dir`. It is an implementation detail, not an input
or an output the operator needs to manage. Every invocation receives a unique
private child directory:

1. Verify the supplied source components or output-directory inventory and
   schema, and reject a detected live Cassandra owner.
2. Copy the selected baseline component sets into the private workspace. An
   empty output directory creates an empty baseline.
3. Start a loopback-only isolated Cassandra child with gossip, streaming,
   JMX, hints, and auto-compaction disabled.
4. Run the installed release-matched `cqlsh` against that child.
5. When `cqlsh` exits successfully, close client access, flush the imported
   table, verify the complete new component sets, and stop the child.
6. Allocate new SSTable identifiers above the complete selected baseline and
   publish the verified components into the chosen target atomically per set.
7. Remove the private workspace on success.  On failure, preserve it only for
   diagnostics and print its path; nothing is published beside the source.

This preserves the isolation properties already implemented while removing
their operational burden.

## Output Naming And Placement

Original components are never modified, renamed, or deleted. A mutation adds
one or more complete new component sets directly in the selected publication
directory.

- Numeric baselines use one above the highest numeric generation present in
  every selected source directory or the publication directory, including
  descriptors not explicitly selected for import.
- Cassandra 5.0 infers `uuid_sstable_identifiers_enabled` from the explicitly
  selected descriptors. If any selected descriptor has Cassandra's
  28-character UUID/ULID-style identifier, the private sandbox writes
  UUID-style deltas and direct publication preserves the Cassandra-generated
  identifier. Mixed old numeric and UUID-style selections therefore produce a
  UUID-style delta.
- Explicit mode, including a separate publication directory, infers from all
  supplied `--sstables`; output-directory-only mode infers from every complete
  SSTable in that directory. An empty Cassandra 5.0 directory defaults to
  UUID-style output; older release lines default to numeric. The installation's
  `cassandra.yaml` is not consulted.
- Allocation happens immediately before publication and rejects any collision.
  Publication-directory mode re-inventories the selected subset belonging to
  that target and fails if the directory changed after import.
  A stopped source node is a required precondition, so no concurrent Cassandra
  writer may allocate an identifier at the same time.

For example, a 4.1 source `na-17-big-Data.db` can result in a sibling set such
as `na-18-big-Data.db`, `na-18-big-Index.db`, and `na-18-big-TOC.txt` after an
`INSERT` or `UPDATE`.

## Safety Contract

This is intentionally an offline tool.  The source Cassandra node must be
stopped, or the input must be a completed snapshot/backup outside any live data
directory.  The command prints a prominent warning and keeps the existing
Linux live-process check, but neither replaces that requirement.

The isolated child has no cluster membership: gossip, seed discovery,
streaming, and JMX are disabled, and its native endpoint is loopback-only with
temporary credentials.  It cannot join or affect a production cluster.

Only the schema's selected table is exposed for CQL mutation.  Conditional
mutations, cluster-wide consistency semantics, repair, and cross-table writes
are not supported.  Consistency level is local to the isolated single-node
worker and is not a production replication guarantee.

## Compatibility And Delivery

The distribution remains one thin JAR per Cassandra release line:

- `sstable-tools-cassandra-3.11-0.1.0-SNAPSHOT.jar`
- `sstable-tools-cassandra-4.0-0.1.0-SNAPSHOT.jar`
- `sstable-tools-cassandra-4.1-0.1.0-SNAPSHOT.jar`
- `sstable-tools-cassandra-5.0-0.1.0-SNAPSHOT.jar`

Each JAR uses the supplied matching Cassandra installation's own libraries and
stock `cqlsh`.  There is no universal shaded Cassandra server JAR and no
dependency on a running Cassandra process.

The existing explicit workspace commands remain an advanced recovery and
diagnostic interface during development.  They will not be the documented
primary workflow once direct `cqlsh` is implemented.

## Implementation Plan

1. Add a top-level direct `cqlsh` command which validates the inputs and owns a
   temporary workspace lifecycle.
2. Add a release-specific publish operation that allocates target-directory
   identifiers and publishes verified delta component sets beside the source.
3. Keep publication blocked unless the `cqlsh` session and flush both succeed.
4. Add unit coverage for input scope, output naming, collision handling, and
   cleanup behaviour.
5. Extend the real-Cassandra CI matrix for 3.11, 4.0, 4.1, and 5.0: create a
   source SSTable with stock Cassandra, stop it, invoke this single command
   with stock `cqlsh` `SELECT`/`INSERT`/`UPDATE`, and verify the new sibling
   SSTable components and unchanged source hashes.
