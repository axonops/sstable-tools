# CQL Mutation Workspaces for SSTables

- **Status:** Proposed
- **Audience:** Maintainers and contributors
- **Scope:** Apache Cassandra 3.11, 4.0, 4.1, and 5.0 SSTables
- **Last updated:** 2026-07-14

## 1. Executive decision

Supporting `INSERT` and `UPDATE` against a set of SSTables is possible, but an
existing SSTable cannot be updated in place. SSTables are immutable. A mutation
must create one or more new SSTables whose cells are reconciled with the input
SSTables by timestamp, tombstone, and TTL rules.

This design introduces a **mutation workspace**:

1. Input SSTable components remain read-only and are inventoried by hash.
2. A version-specific thin JAR is deployed on a Cassandra node and uses that
   installation's Cassandra JARs as provided runtime dependencies.
3. The tool starts a separate, isolated single-node Cassandra sandbox matching
   the installed release line. It never attaches to the live Cassandra JVM.
4. The sandbox imports copies of the input SSTables and exposes its native
   protocol only on loopback.
5. The installation's Apache `cqlsh` connects to that endpoint. Cassandra itself
   executes `SELECT`, `INSERT`, and `UPDATE` and produces new SSTables.
6. On export, the tool flushes the table, validates the result, and copies either
   only the new SSTable generations or a complete rewritten snapshot to an
   output directory.

This is preferable to implementing a partial mutation engine in this project.
It preserves Cassandra's CQL type handling, cell timestamps, collection and
static-column behavior, TTLs, tombstones, and read reconciliation. It also
avoids loading incompatible Cassandra releases into one JVM.

The existing local `sstable-tools cqlsh` command should remain available for
fast offline inspection. Write mode is a separate workspace workflow because it
has different correctness and safety requirements.

## 2. Terminology and an important distinction

This repository currently provides a Java shell named `cqlsh`, but it is not
Apache Cassandra's `cqlsh`. The current shell parses a limited command set in
process and directly opens SSTables through Cassandra 3.11 internals. Apache
`cqlsh` is a Python native-protocol client and requires a Cassandra-compatible
server endpoint.

In this document:

- **local shell** means the existing `sstable-tools cqlsh` implementation;
- **Apache cqlsh** means the client shipped with an Apache Cassandra release;
- **source set** means all component files selected as the immutable input;
- **delta** means new SSTables produced by mutations in a workspace;
- **target release** means the Cassandra runtime and output format selected for
  the workspace, not necessarily the release that originally wrote the input.

## 3. Current state

The project is tightly coupled to Cassandra 3.11.0:

- `pom.xml` has one `cassandra-all` dependency and targets Java 8.
- `Query` uses internal parser, restriction, selection, and `SSTableReader`
  classes, then merges unfiltered iterators for reads.
- `CassandraUtils` reconstructs partial table metadata from `Statistics.db` or
  accepts a user-provided `CREATE TABLE` statement.
- `Cqlsh` explicitly rejects `INSERT`, `UPDATE`, and `DELETE` as read-only.
- `Compact` demonstrates that the process can write a new 3.11 SSTable, but it
  is not a transactional mutation path and always uses that dependency's latest
  format.
- Production query construction depends on reflection and Mockito. These are
  fragile across Cassandra internal API changes.

The current architecture can be retained as a legacy 3.11 reader, but it is not
a sound base for a four-release mutation engine.

## 4. Goals

1. Query imported SSTables with a stock, version-matched Apache `cqlsh`.
2. Support ordinary non-conditional `INSERT` and `UPDATE` CQL statements.
3. Preserve Cassandra reconciliation semantics across base and delta SSTables.
4. Read documented compatible Big-format variants for Cassandra 3.11, 4.0,
   4.1, and 5.0, plus the Cassandra 5.0 BTI format.
5. Emit SSTables native to a user-selected target release and format.
6. Guarantee that source component files are never modified, renamed, linked
   into a writable data directory, or deleted.
7. Detect missing components, incompatible schemas, corrupt files, and
   unsupported format combinations before accepting writes.
8. Make workspace creation, mutation, flush, export, and recovery auditable and
   repeatable.

## 5. Non-goals for the first release

- Editing bytes or components of an existing SSTable in place.
- Mutating a live Cassandra node's data directory.
- Reproducing distributed consistency, replica repair, or Paxos behavior.
- Acting as a general-purpose Cassandra node or accepting remote clients.
- `DELETE`, `TRUNCATE`, DDL, logged/unlogged batches, materialized views,
  secondary indexes, SAI queries, triggers, CDC, or user-defined functions.
- Counter-table mutations. Counter contexts require special operational care
  and are unsafe for a general-purpose export/import workflow.
- Preserving repaired, pending-repair, or transient-replica state on newly
  generated delta SSTables. Deltas are exported as unrepaired.
- Automatically inferring a complete writable schema solely from SSTable
  metadata.

Read-only inspection of some non-goal data shapes may still be possible.

## 6. Correctness invariants

The implementation must maintain these invariants:

1. **Source immutability:** the SHA-256 hash of every source component is the
   same before workspace creation and after export or failure.
2. **Complete component sets:** files are grouped by descriptor; a `Data.db`
   file is never treated independently from its required sidecars.
3. **One logical table:** all imported descriptors must validate against one
   canonical schema and partitioner.
4. **One version runtime per process:** Cassandra classes from different release
   lines never share a classloader or JVM.
5. **Cassandra owns mutation semantics:** accepted writes travel through the
   target release's normal CQL and storage engine path.
6. **No unflushed export:** export succeeds only after a successful flush and
   graceful storage-engine checkpoint.
7. **Atomic publication:** a partial component set is never published as an
   exported SSTable.
8. **Explicit compatibility:** write mode always records the tool artifact hash,
   installed Cassandra release and JAR hashes, Java runtime, and output format.
9. **Visible operator warning:** before source capture, import, mutation,
   compaction, or export, the CLI warns that files owned by a running production
   Cassandra process must never be used. Operators must stop the owning process
   or use a completed snapshot or backup copied outside every live data
   directory. On Linux, the controller also rejects sources under a visible
   Cassandra daemon's reported storage root or containing its open/mapped files
   before create, import, start, cqlsh, flush, and export. Restricted `/proc`
   visibility and non-Linux systems still require the operator invariant; the
   warning and automatic check are complementary controls.

## 7. Proposed architecture

```text
                       +-----------------------+
 source components --->| workspace controller  |---> validated export
      (read-only)       | manifest / lifecycle  |       (delta or snapshot)
                       +-----------+-----------+
                                   |
                         versioned control RPC
                                   |
                       +-----------v-----------+
 Apache cqlsh -------->| isolated Cassandra    |
 native protocol       | worker: 3.11 / 4.0 /  |
 loopback only         | 4.1 / 5.0             |
                       +-----------------------+
                         private copied data,
                         commit log, and deltas
```

### 7.1 Version-neutral controller

The controller is a small Java application with no `cassandra-all` dependency.
It owns:

- descriptor filename parsing and component grouping;
- source inventory and hashing;
- target selection and compatibility checks;
- schema file parsing only at the document level; semantic schema validation is
  delegated to the worker;
- sandbox provisioning, process lifecycle, health checks, and cleanup;
- loopback port allocation and generation of the matching `cqlsh` command;
- baseline/output inventories and atomic export;
- a stable JSON control protocol used by every version adapter.

The controller must not deserialize `Statistics.db` itself. That metadata is a
versioned Cassandra format and belongs in the matching worker.

### 7.2 Release-isolated workers

There is one thin tool artifact for each release line:

```text
sstable-tools-cassandra-3.11.jar
sstable-tools-cassandra-4.0.jar
sstable-tools-cassandra-4.1.jar
sstable-tools-cassandra-5.0.jar
```

Each artifact is built and tested against an exact patch release. It contains:

- a JDK-only bootstrap that can run before Cassandra classes are available;
- the release-specific supervisor/adapter and query guard; and
- shaded and relocated third-party dependencies that are not supplied by
  Cassandra.

The artifact does not package `cassandra-all`, Cassandra's transitive
dependencies, or a Java runtime. Cassandra dependencies use Maven `provided`
scope. The bootstrap resolves the installed Cassandra home, verifies its exact
version, and starts a child process equivalent to:

```shell
java -cp "/path/sstable-tools.jar:$CASSANDRA_HOME/conf:$CASSANDRA_HOME/lib/*" \
  com.axonops.sstable.worker.Main --workspace /path/to/workspace
```

The public entry point remains convenient:

```shell
java -jar sstable-tools-cassandra-4.1.jar \
  --cassandra-home /usr/share/cassandra \
  workspace start ./case
```

The bootstrap locates the Java executable compatible with that Cassandra
installation, constructs the child classpath without shell expansion, and
forwards signals and exit status. The worker child contains:

- a supervisor/adapter using that release's installed APIs for offline descriptor
  validation, schema/header comparison, import, verification, and inventory;
- a query guard selected through Cassandra's custom `QueryHandler` mechanism;
  and
- an isolated `CassandraDaemon` configured by the tool.

The worker starts the target release's real daemon code in its own JVM. The
daemon supplies native transport, query processing, commit log, memtable, read
path, flush, and SSTable writing. Offline adapter operations that directly open
SSTables run only while the daemon has not opened, or has closed, those files.

Process isolation is mandatory because:

- Cassandra internal APIs changed substantially from `CFMetaData` in 3.11 to
  `TableMetadata` and later APIs;
- Cassandra uses global static state and assumes one configured storage engine;
- release lines have different Java runtime requirements;
- identical class and package names make dependency shading an unreliable
  compatibility mechanism.

### 7.3 Installed Cassandra discovery

Runtime discovery follows an explicit order:

1. `--cassandra-home` and optional `--java-home` command-line arguments;
2. `CASSANDRA_HOME` and `JAVA_HOME` environment variables;
3. known package-layout metadata, only when it resolves to one unambiguous
   installation.

The bootstrap inventories the resolved Cassandra JARs, reads the release
version, and selects only the adapter matching both release line and tested patch
range. It refuses a classpath containing multiple Cassandra versions. A startup
self-test resolves the internal classes and method signatures used by the
adapter before source files are opened.

No runtime is downloaded, and no JAR is copied from the installation into the
tool artifact. The workspace records the canonical Cassandra path, release
version, Java version, and hashes of Cassandra JARs relevant to the adapter.
The installation's `cassandra.yaml` is not required, read, hashed, or recorded.
An optional support directory may provide distribution JVM module options or
client resources, but Cassandra is always started with the generated
workspace-owned configuration.

### 7.4 Sandbox node

The worker starts a real, isolated single-node Cassandra instance with:

- loopback-only authenticated native CQL and control addresses;
- Thrift RPC, internode messaging, and JMX connectors absent, with JVM attach
  disabled for the release worker;
- an automatically allocated native-protocol port;
- private data, saved-cache, hints, commit-log, log, and system directories;
- a single-node topology and replication factor one;
- auto-compaction disabled for the imported table;
- no external seeds, broadcast addresses, or remote management endpoint;
- file permissions restricted to the current user;
- a version-specific query guard allowing table reads plus `INSERT` and
  `UPDATE`, while rejecting DDL and the non-goals in section 5.

The query guard is important even on loopback. It prevents accidental
`TRUNCATE`, schema changes, counter mutations, and operations that would make a
delta export incomplete or misleading. It must inspect both direct and prepared
statements; string-prefix filtering is insufficient. All four target branches
support configuring a native-protocol `QueryHandler`; each version-specific
guard delegates accepted statements to that release's `QueryProcessor`.

Production RBAC is not copied into a workspace. Each start generates a random
256-bit native password in an owner-only `state/cqlshrc`; the matching worker
authenticator accepts only a fixed, non-superuser workspace identity from a
loopback client. A fixed in-memory role manager supplies login status without
querying `system_auth`. The password rotates on restart and the credential file
is deleted after graceful stop or dead-worker recovery. This identity gate does
not replace statement authorization: the version-specific query guard remains
the boundary that decides which CQL may execute.

Running on a Cassandra host does not mean running inside the production
Cassandra process. The worker uses a unique cluster name, loopback-only
addresses, non-conflicting ports, and workspace-owned config and storage paths.
It never reads the production process's commit log or mutable data directory.
Input must come from a snapshot, backup, or other stable component set.

### 7.5 Workspace layout

```text
workspace/
  manifest.json
  schema/
    schema.cql
    canonical-schema.cql
  source-inventory.json
  runtime/
    cassandra.yaml
    env
  data/                  # private copies/imported base plus generated deltas
  commitlog/
  logs/
  staging/
  exports/
  state/
    pid
    endpoint.json
    control.token
    cqlshrc
    baseline-inventory.json
```

`manifest.json` contains a format version, workspace UUID, state machine value,
source paths and hashes, schema hash, table ID, partitioner, target release and
format, exact tool/Cassandra/Java identity, creation time, and export history. It is
updated through write-to-temporary-file, fsync, and atomic rename.

Valid lifecycle states are:

```text
NEW -> VALIDATED -> IMPORTED -> RUNNING -> FLUSHED -> EXPORTED -> STOPPED
                             \-> FAILED_RECOVERABLE
```

Operations are idempotent by state and workspace UUID. A controller must never
infer success only from a process exit code; it also verifies the expected files
and worker health response.

## 8. SSTable format compatibility

The following matrix is derived from the release branches' `BigFormat` and
`BtiFormat` version implementations. The exact worker patch remains pinned and
its runtime `Version.isCompatible` result is authoritative.

| Target runtime | Accepted input | Default output |
|---|---|---|
| Cassandra 3.11 | Big `ma` through `me` | Big `me` |
| Cassandra 4.0 | Big `ma` through `nb` | Big `nb` |
| Cassandra 4.1 | Big `ma` through `nb` | Big `nb` |
| Cassandra 5.0 | Big `ma` through `oa`; BTI `da` | Big `oa` or BTI `da` |

The 5.0 worker runs in full Cassandra 5 storage-compatibility mode. A mode that
continues to emit Big `nb` is rejected for a 5.0 output workspace; users needing
`nb` output select the 4.0 or 4.1 target explicitly.

Notable variants are:

- Big `ma`: native row storage introduced for Cassandra 3.0;
- Big `mb`: commit-log lower bound metadata;
- Big `mc`: commit-log intervals;
- Big `md`: corrected min/max clustering metadata;
- Big `me`: originating host ID metadata;
- Big `na`: Cassandra 4.0 metadata checksums, pending/transient repair metadata,
  compression changes, and a new bloom-filter representation;
- Big `nb`: originating host ID in the Cassandra 4 generation;
- Big `oa`: Cassandra 5.0 min/max and key-range improvements;
- BTI `da`: the initial Cassandra 5.0 trie-indexed format.

The controller parses `{version}-{generation}-{format}-{component}` only to
select candidate workers. The chosen worker must then parse every descriptor and
validate it. A lexical range check alone is not sufficient.

Input sets may mix compatible Big versions because Cassandra's read path already
reconciles them. Big and BTI may be mixed only when the Cassandra 5.0 worker
confirms support. Write mode requires `--target`; an `ma` file alone cannot reveal
whether the desired output is 3.11, 4.x, or 5.0.

Every descriptor inventory includes, as applicable, `Data.db`, `Index.db`,
`Summary.db`, `Filter.db`, `CompressionInfo.db`, `Statistics.db`, digest/checksum,
`TOC.txt`, and BTI `Partitions.db`/`Rows.db` components. Temporary files are
rejected. Missing optional components are classified by the worker rather than
by a universal filename list.

Compression, checksum, partitioner, repaired-state, and serialization-header
variants are tested independently of the two-letter version. A worker fails
closed when a compressor or format feature is unavailable.

## 9. Schema contract

SSTable serialization headers contain enough type information for some reads,
but not a complete writable CQL schema and all associated semantics. Write mode
therefore requires a schema bundle containing:

- one `CREATE KEYSPACE` statement;
- any required `CREATE TYPE` statements in dependency order;
- exactly one `CREATE TABLE` statement, preferably including `WITH ID` from the
  source snapshot schema;
- no indexes, views, triggers, functions, or aggregates in the first release.

The worker canonicalizes the schema using the target Cassandra parser and
compares it with every input serialization header. It validates:

- partition-key and clustering-column count, order, and types;
- static and regular column names and types;
- clustering order;
- partitioner;
- frozen, collection, tuple, and UDT structure;
- dense/compact-table flags where the target runtime still supports reading
  them.

Table options that do not affect on-disk decoding may differ, but the difference
is recorded. A mismatch in an on-disk type or primary-key layout is fatal. The
worker records the source keyspace replication settings but creates a normalized
single-node, replication-factor-one keyspace in the sandbox. It preserves a
source table ID only when the target release supports doing so safely; otherwise
the sandbox table ID and source table ID are both retained in the manifest. A
user-provided table ID override is accepted only during workspace creation.

## 10. Import and session lifecycle

### 10.1 Create and validate

1. Resolve paths without following a source component through a writable
   workspace symlink.
2. Group all component sets and compute sizes and SHA-256 hashes.
3. Select the target worker from `--target` and descriptor candidates.
4. Have the worker deserialize metadata, verify checksums, and compare the schema.
5. Refuse a path that appears to be an active Cassandra data directory unless a
   snapshot or backup subdirectory was explicitly selected.

### 10.2 Import

The implementation uses Cassandra's release-specific import/streaming APIs,
not ad hoc component rewriting. Cassandra 4.x and 5.0 can use the SSTable import
path with copy semantics. The 3.11 adapter uses its supported refresh/streaming
path. If descriptor generations collide, the adapter allocates destination
descriptors through Cassandra APIs or streams the data; it never renames only a
subset of components.

Source data is copied or filesystem-reflinked into the private workspace. Hard
links are prohibited because compaction or cleanup in the sandbox could unlink
or alter metadata visible through the source link.

After import, the worker runs a full table scan and records the baseline
descriptor inventory. Auto-compaction is disabled before accepting mutations.

### 10.3 Connect with cqlsh

The target controller exposes both commands:

```shell
sstable-tools workspace start ./case
sstable-tools workspace cqlsh ./case
```

`start` prints a loopback endpoint, fixed username, and generated `cqlshrc` path
for an external client. `cqlsh` launches the client from the selected Cassandra
installation, which avoids unsupported client/server version combinations. The
implemented launcher requires the canonical Cassandra home and release recorded
by `start`, validates the live authenticated control endpoint
and owner-only credential, and fixes the client host, port, and `cqlshrc`. The
password never appears in process arguments. Interactive mode accepts no client
overrides; `--execute <CQL>` provides the constrained noninteractive form.

The printed configuration also remains usable with the installed client
directly:

```shell
${CASSANDRA_HOME}/bin/cqlsh --cqlshrc /workspace/state/cqlshrc HOST PORT
```

For DEB and RPM installations rooted at `/usr/share/cassandra`, the equivalent
stock client path is `/usr/bin/cqlsh`. The launcher recognizes both layouts and
does not fall back to an unrelated executable from `PATH`.

Example workflow:

```shell
sstable-tools workspace create ./case \
  --sstables /evidence/table-snapshot \
  --schema /evidence/schema.cql

sstable-tools workspace import ./case
sstable-tools workspace start ./case
sstable-tools workspace cqlsh ./case
sstable-tools workspace flush ./case
```

Inside Apache cqlsh:

```sql
SELECT * FROM evidence.users WHERE user_name = 'frodo';

INSERT INTO evidence.users (user_name, password, state)
VALUES ('sam', 'secret', 'CA')
USING TIMESTAMP 1784044800000000;

UPDATE evidence.users
USING TTL 3600
SET state = 'OR'
WHERE user_name = 'frodo';
```

The sandbox gives immediate read-your-writes behavior through its memtable.
`workspace flush` is the Cassandra 3.11 boundary that quiesces CQL and creates
new SSTables; `workspace export` performs release verification and publishes
those immutable results separately.

### 10.4 Flush and export

The Cassandra 3.11 checkpoint implements `workspace flush` separately from
export. Worker protocol v3 closes native transport, waits for all query-guard
requests, confirms auto-compaction is disabled, blockingly flushes the exact
workspace table, and atomically writes `state/flush-result.json` before the
worker endpoint becomes `FLUSHED`. The strict result records every table file's
path, size, and SHA-256. The controller verifies that the imported baseline is
unchanged, classifies every remaining file as delta, removes the CQL credential,
and can reconcile a `RUNNING` manifest when the worker/result already committed
`FLUSHED`.

```shell
sstable-tools workspace export ./case \
  --mode delta \
  --output ./case-output
```

The implemented Cassandra 3.11 export consumes the committed flush result and:

1. reverify the complete inventory and post-import delta classification;
2. run the target release's SSTable verification and a reconciliation scan;
3. copy complete component sets into a temporary export directory;
4. write an inventoried `.sstable-tools-export` ownership marker,
   `export-manifest.json`, `schema.cql`, checksums, runtime identity, and a
   source dependency list;
5. fsync and atomically rename the export directory into place.

Export modes are:

- **delta:** only new generations. The manifest declares the source hashes that
  must accompany them. This is fast and preserves cell-level mutations.
- **snapshot:** all imported base and new generations. This is self-contained
  but may contain many SSTables.
- **compact-snapshot:** an explicit, slower rewrite after a successful normal
  snapshot. It is never the default because compaction can purge tombstones and
  change repaired/level metadata according to the sandbox's current time and
  table settings.

Generated SSTables are unrepaired and use the target runtime's native latest
format. An export is not copied directly into a production data directory; it is
intended for later validation and import with Cassandra's supported tools.

Verification is worker-owned because only the matching release adapter may
interpret Cassandra descriptors and metadata. Worker protocol `VERIFY` is
accepted only after `FLUSHED`; it runs Cassandra extended verification,
requires the live-reader descriptors to match the flush inventory, checks each
generated reader's format and repaired metadata, scans a logical row count, and
durably binds the result to the flush hash. Controller-owned publication then
requires complete TOC component sets, an existing canonical destination
parent, owner-only staging, fsync, and an atomic non-replacing rename. A retry
can reconcile only a byte-identical deterministic export and never overwrites
an existing destination. A deterministic partial staging tree is removed only
after its exact ownership marker, allowed paths, non-symlink structure, and
owner-only permissions validate.

## 11. Mutation semantics

### 11.1 Supported

The initial write guard accepts non-conditional mutations against the one
workspace table:

- `INSERT` with literals or prepared values;
- `UPDATE` with a complete primary-key restriction accepted by Cassandra;
- `USING TIMESTAMP` and `USING TTL`;
- regular and static columns;
- scalar, collection, tuple, and UDT assignments supported by the target
  release;
- assigning `null` where Cassandra normally creates a tombstone.

`INSERT` and `UPDATE` are both upserts in Cassandra. They create newer cells in
the memtable and eventually a new SSTable; unchanged cells remain in the base
SSTables. Reads merge both sets and choose winners using Cassandra's normal
reconciliation rules.

The implemented Cassandra 3.11 checkpoint installs this guard through
`cassandra.custom_query_handler_class`. It parses both direct and prepared
requests with Cassandra before classification and uses a stable
`SSTABLE_TOOLS_POLICY` invalid-request error for rejections. Production RBAC is
not copied into the sandbox. The implemented per-start credential and fixed
role authenticate one loopback workspace identity; `AllowAllAuthorizer` is used
behind the query guard and does not expose production permissions.

The Cassandra 3.11 live profile exercises the complete supported shape set
against a pinned-writer latest-format source containing scalar, static,
collection, tuple, and frozen-UDT cells. Stock cqlsh reads the source, performs
literal `INSERT` and `UPDATE` statements with set/list/map operations and TTL,
and reads its writes. A prepared driver independently decodes the merged state.
After flush and delta export, a fresh workspace importing base plus delta
returns the same values and a still-live TTL, with source and delta hashes
unchanged.

The Cassandra 3.11 guard preserves the native request consistency value and
accepts only `ONE` or `LOCAL_ONE`. It rejects every other level rather than
silently reducing it to the sandbox's single replica. This workspace provides
local read/write and commit-log durability behavior; it cannot reproduce
distributed replica acknowledgement, read repair, or Paxos semantics.

### 11.2 Timestamp and TTL behavior

By default, Cassandra assigns the sandbox node's current microsecond timestamp.
An old source set can contain a future timestamp, in which case an apparently
successful update may not win. Workspace import reads the maximum timestamp
from every SSTable statistics component, records it in the manifest, and warns
at import, start, and status while it remains ahead of the controller clock.

The tool does not silently rewrite user timestamps. The user can supply `USING
TIMESTAMP`, or opt into `--timestamp-policy after-source`, which configures the
query guard's timestamp generator to advance above both wall clock and the
maximum source timestamp. The guard allocates a timestamp only when neither the
CQL statement nor the native-protocol request supplies one. It writes and
`fsync`s the new high-water mark before Cassandra executes the mutation, so a
crash may leave a gap but cannot cause reuse. The selected policy is recorded
in the manifest, while the owner-only high-water state is stored under
`state/timestamp.properties` and survives stop and crash recovery.

The Cassandra 3.11 checkpoint implements maximum detection, strict worker
protocol v3 handoff, future-clock warnings, and the durable allocator for both
direct and prepared mutations. Explicit `USING TIMESTAMP` and native-protocol
timestamps are preserved exactly. Cassandra 3.11's stock cqlsh Python driver
normally supplies a protocol timestamp, so the allocator correctly treats its
ordinary mutations as explicit client-timestamp requests. An operator using
stock cqlsh against a future-dated source must therefore use `USING TIMESTAMP`
above the reported maximum. Other clients can opt into allocation by omitting
both forms of timestamp.

The Cassandra 3.11 live profile generates a latest-format `me` SSTable with a
real cell timestamp one year ahead of the test clock using the installed
runtime's `CQLSSTableWriter`. Independent imports prove that a stock-cqlsh
update under `wall-clock` is accepted but loses reconciliation to the future
source cell, while a prepared client with client timestamp generation disabled
receives a durable `after-source` timestamp above the source maximum and wins.
Both paths recheck the original component inventory for byte immutability.

TTL expiry and tombstone visibility are evaluated using the sandbox clock. For
forensic reproducibility, a later phase may add a supported fixed-time query
mode; changing the process clock is not part of the first release.

### 11.3 Explicitly rejected

The guard returns a clear `InvalidRequest` for:

- `IF`, `IF NOT EXISTS`, and other lightweight transactions;
- counter increments/decrements or any counter-table mutation;
- `DELETE`, `TRUNCATE`, and `BATCH`;
- writes to system tables or any table except the workspace table;
- DDL after bootstrap;
- mutations relying on indexes, materialized views, triggers, or CDC.

Lightweight transactions on one isolated replica would not reproduce the source
cluster's Paxos state or distributed guarantee. They must not be presented as a
faithful offline edit.

## 12. Failure handling and safety

- **Crash before acknowledgement:** Cassandra commit-log rules determine whether
  the mutation exists in the workspace. Restart replays only the private commit
  log.
- **Crash after acknowledgement but before flush:** restart and replay, then
  export. The source set remains unchanged.
- **Crash after worker flush but before manifest update:** status, flush, or
  recovery verifies the durable full inventory and completes `RUNNING ->
  FLUSHED`; missing or changed result state fails closed.
- **Crash during export:** retry validates the deterministic ownership marker
  before removing a partial staging tree. If atomic publication already
  completed, retry adopts the final directory only when every recorded path,
  size, and hash matches; arbitrary staging or destination content is never
  deleted or overwritten.
- **Corrupt input:** validation fails before native transport is enabled.
- **Corrupt generated component:** export fails and retains the workspace for
  diagnosis.
- **Worker/controller version mismatch:** control-protocol negotiation fails
  before import.
- **Disk pressure:** creation checks estimated copy plus flush headroom. The
  worker also has a workspace quota and stops accepting writes before exhausting
  the filesystem.
- **Unexpected compaction:** baseline descriptors disappearing is an export
  error unless the user explicitly requested compact-snapshot mode.

The controller takes an exclusive workspace lock. `workspace flush` disables
native transport and inventories the exact table while the control endpoint
remains live. `stop` drains and stops the daemon. `destroy` refuses to run while
a worker is live and never traverses paths outside the canonical workspace
root. The implemented command requires the exact manifest UUID, accepts only
inactive pre-start states or `STOPPED`, rejects unexpected root entries, and
walks without following symlinks. Source and export paths are external and are
never candidates for deletion.

## 13. Control protocol

The controller and worker supervisor communicate over a private Unix-domain
socket where available, with a loopback TCP fallback. The protocol is versioned
JSON with length-prefixed messages. Required operations are:

```text
HELLO
VALIDATE_SOURCE
VALIDATE_SCHEMA
IMPORT
START_NATIVE_TRANSPORT
STATUS
QUIESCE
FLUSH
INVENTORY
VERIFY
STOP
```

Responses include a stable error code, human message, worker release, and
optional diagnostic fields. Java stack traces remain in workspace logs and are
not the controller API.

The Cassandra 3.11 checkpoint currently uses authenticated, line-oriented
requests over a private loopback TCP endpoint. Protocol v3 implements `STATUS`,
`FLUSH`, and `STOP`; endpoint and flush-result files provide the strict
versioned identity and recovery records. The framed JSON/Unix-socket transport
above remains the target before treating the control protocol as a public,
cross-release contract.

No source path supplied by a worker is trusted without canonicalization by the
controller. Authentication material and endpoint files use owner-only
permissions. Native transport refuses non-loopback binding.

## 14. Build and repository structure

The current single Maven module should become an aggregator:

```text
pom.xml
bootstrap/
workspace-core/
worker-api/
workers/cassandra-3.11/
workers/cassandra-4.0/
workers/cassandra-4.1/
workers/cassandra-5.0/
legacy-reader-3.11/
integration-tests/
fixtures/
verification/
scripts/
```

All release modules live on one main development branch. Cassandra release
lines are separated by Maven module/directory boundaries, not long-lived Git
branches. Ordinary short-lived branches are used for changes and tags identify
tool releases. This keeps workspace safety fixes and the shared contract suite
consistent across every adapter.

The initial adapter compile contracts are:

| Adapter | Cassandra compile dependency | Class-file target |
|---|---:|---:|
| 3.11 | 3.11.19 | Java 8 |
| 4.0 | 4.0.0 | Java 8 |
| 4.1 | 4.1.11 | Java 11 |
| 5.0 | 5.0.4-5.0.8 | Java 17 |

These pins prove build isolation; they are not the final installed patch support
ranges. Runtime discovery and linkage tests must define and enforce those ranges
before a release claims patch-level compatibility.

The 4.1 adapter explicitly implements both `QueryHandler` ABIs used within the
4.1 patch line and verifies the selected ABI during runtime preflight.

`bootstrap`, `workspace-core`, and `worker-api` contain no Cassandra types and
target Java 8. Each worker builds its supervisor/adapter and query guard against
`provided` Cassandra dependencies with its own compiler/toolchain configuration.
A packaging step combines the shared modules and exactly one release adapter
into each distributable JAR while excluding Cassandra classes and resources.
Third-party libraries are minimized and relocated to avoid conflicts with the
installation.

The bootstrap starts the matching worker as an external process and never loads
Cassandra classes itself. Build verification fails if a distributable contains
`org/apache/cassandra/**` or other forbidden server-owned packages.

The legacy shell can initially move unchanged into `legacy-reader-3.11`. A later
read-only adapter API may replace its reflection and Mockito usage, but that is
not on the critical path for safe mutations through Apache cqlsh.

## 15. Test strategy

### 15.1 Fixture matrix

Keep immutable, checksummed fixtures generated by exact Cassandra releases for:

- Big `ma`, `mb`, `mc`, `md`, `me`, `na`, `nb`, and `oa`;
- BTI `da`;
- compressed and uncompressed data;
- Murmur3 plus every other partitioner claimed as supported;
- skinny and wide partitions, static rows, range tombstones, row/cell
  tombstones, live and expired TTLs, future timestamps, collections, tuples,
  frozen/non-frozen UDTs, and compact legacy tables;
- repaired and pending-repair metadata as read-only validation cases;
- missing, truncated, checksum-failing, and schema-mismatched component sets.

`jb`, `ka`, `la`, and `lb` fixtures are included if the project advertises the
full 3.11 reader compatibility reported by `BigFormat`; otherwise the public
support claim must start at `ma`.

Each fixture records producer version, schema, insert script, partitioner,
format, compression, expected logical rows, and component hashes.

### 15.2 Contract tests

The same black-box suite runs against all workers:

1. validate and import every compatible fixture;
2. reject every incompatible fixture before startup;
3. query expected rows through the matching Apache cqlsh/native driver;
4. reject missing and incorrect credentials, and verify credentials rotate on
   restart and disappear on stop or dead-worker recovery;
5. execute `INSERT` and each supported `UPDATE` shape;
6. verify read-your-writes before flush;
7. flush and publish the generated SSTables next to the selected source set;
8. reopen the base plus generated SSTables through the direct tool and verify
   identical logical results and timestamps;
9. prove every source hash is unchanged;
10. verify that forbidden statements fail without producing delta files.

The contract deliberately excludes `sstableloader`, clean-node loading,
streaming, bulk-load, and cluster-import workflows. It never discovers a
Cassandra data root: each test supplies its exact `--sstables` set.

### 15.3 Failure tests

Kill the worker at each lifecycle boundary, including during import, after write
acknowledgement, during flush, and during export. Every state must either resume
idempotently or produce a diagnostic recovery command. Add disk-full, port
collision, stale PID, runtime mismatch, and malformed manifest tests.

The Cassandra 3.11 profile now hard-stops the export controller after release
verification, the first copy, all copies, the export-manifest fsync, the atomic
rename, and the workspace-manifest save. Every retry must publish or adopt the
same exact export and leave no staging directory. Import, write, and flush
boundaries remain separate lifecycle coverage work where not already listed in
the release findings.

### 15.4 Differential tests

For each target release, compare the base fixture with a direct reopen of that
base plus its generated sibling SSTables. Compare logical rows, cell timestamps,
TTLs, and tombstones. Component bytes are not expected to match because
generation, flush grouping, and metadata can legitimately differ.

## 16. Delivery plan

### Phase 0: Characterization and guardrails

- Add fixture provenance and source-hash tests for the current 3.11 reader.
- Define descriptor inventory, workspace manifest, state machine, and worker
  control protocol.
- Add a controller command that reports candidate/unsupported formats without
  opening data.

### Phase 1: Cassandra 3.11 end-to-end workspace

- Build the Java 8 bootstrap and a thin 3.11 JAR using installed Cassandra JARs.
- Implement Cassandra-home discovery, version checks, child classpath creation,
  and a compatibility self-test.
- Implement schema validation, safe import, loopback cqlsh, query guard, flush,
  delta export, restart, and source immutability checks.
- Support scalar and collection `INSERT`/`UPDATE`, timestamps, TTLs, and static
  columns.

This phase proves the architecture and is the first useful write release.

### Phase 2: Cassandra 4.0 and 4.1 workers

- Implement separate adapters despite their shared Big `nb` output format.
- Add `ma`-through-`nb` compatibility and cross-version upgrade tests.
- Test both matching cqlsh clients and the release-specific import paths.

### Phase 3: Cassandra 5.0 Big and BTI

- Add Big `oa`, BTI `da`, Java 17 runtime handling, and mixed Big/BTI reads.
- Export the user-selected `big` or `bti` format.
- Add Cassandra 5.0 types to schema and mutation tests; vector columns are
  supported only after explicit fixtures and round-trip tests exist.

### Phase 4: Operational hardening

- Add signed runtime metadata, quotas, structured audit events, compact-snapshot
  mode, and performance baselines for large source sets.
- Consider replacing the legacy local reader with versioned read-only workers.

## 17. Acceptance criteria

The feature is ready for a release line only when:

1. all advertised format fixtures import and query successfully;
2. a stock matching Apache cqlsh can execute supported `SELECT`, `INSERT`, and
   `UPDATE` statements;
3. base plus exported delta reopens to the same logical state seen before export;
4. the export imports into a clean node of the declared target release;
5. source hashes remain unchanged across success, process crash, failed import,
   and failed export tests;
6. incompatible formats and schema mismatches fail before writes are enabled;
7. forbidden mutations are rejected through both direct and prepared protocol
   paths;
8. missing and incorrect native credentials are rejected, credentials rotate
   across starts, and stopped workspaces retain no native credential; and
9. workspace recovery is documented and tested for every persisted state.

The Cassandra 3.11 checkpoint implements this recovery matrix. Pre-worker
states recover only after immutable inputs and any baseline revalidate. Active,
flushed, and exported states reconcile authenticated worker evidence and fall
back to `STOPPED` only when Linux process identity proves the exact worker is
gone. Recovering a failed `STOPPED` state likewise requires a valid stopped
endpoint and process-death proof.

## 18. Alternatives considered

### Extend the current `Query` class into a mutation engine

Rejected as the primary design. It would require reproducing Cassandra's
version-specific statement preparation, mutation construction, collection
semantics, timestamp/TTL handling, read-before-write cases, memtable overlay,
commit log, and SSTable writer behavior. The current reflection and mocked
statement restrictions make that especially risky.

### Use only `CQLSSTableWriter`

Useful for a constrained bulk-`INSERT` tool, but insufficient for this goal. Its
documented builder is based on one prepared `INSERT`; it does not by itself
provide arbitrary `UPDATE`, read-your-writes, reconciliation queries, crash
recovery, or an Apache cqlsh endpoint.

### Put four Cassandra dependencies in one shaded JAR

Rejected. Shading does not address global static state, native libraries,
service loading, resource names, reflective references, or incompatible Java
runtimes. External workers give a testable failure boundary.

### Rewrite all source data after every mutation

Rejected. It is unnecessarily expensive, changes compaction/tombstone behavior,
and increases the corruption blast radius. Cassandra's normal immutable delta
model is both safer and more faithful.

### Implement a partial native-protocol server

Rejected. Apache cqlsh issues system-schema and prepared/native-protocol queries
whose details differ by release. Running the real target Cassandra transport is
less code and provides the expected CQL behavior.

## 19. Risks and open decisions

- **Runtime footprint:** a sandbox node is heavier than the current local shell.
  This is the cost of full cqlsh and mutation correctness. The existing reader
  remains the fast inspection path.
- **Installed JAR compatibility:** Cassandra internal APIs are not a public
  binary-compatibility contract. Each artifact needs an explicit tested patch
  range and must fail closed when its startup linkage self-test detects drift.
- **Classpath conflicts:** Cassandra installations ship Guava, Netty, Jackson,
  logging, and other libraries. Tool-owned dependencies must be avoided or
  relocated, and the child classpath order must be deterministic.
- **Import behavior for legacy formats:** the 3.11 adapter needs a prototype to
  choose between refresh, streaming, and version-specific import APIs while
  preserving a reliable baseline inventory.
- **Old 3.11 formats:** source code reports compatibility back to `jb`, but the
  current repository only has `ma`, `mb`, and `mc` fixtures. Public support must
  follow tested fixtures, not the theoretical range.
- **Schema availability:** a writable schema cannot always be recovered from
  components. Requiring snapshot `schema.cql` is an intentional correctness
  boundary.
- **Time-sensitive data:** TTL and tombstone outcomes change with wall clock.
  The manifest must record all relevant times and `gc_grace_seconds`.
- **Output destination:** production import procedures vary by version and
  deployment. The tool should produce and verify artifacts, not directly alter a
  production data directory.
- **Licensing/distribution:** Cassandra binaries are not repackaged. Release
  artifacts contain notices for tool-owned dependencies and document that an
  existing compatible Cassandra installation is required.

## 20. References

- [Apache Cassandra storage engine and SSTable formats](https://cassandra.apache.org/doc/stable/cassandra/architecture/storage-engine.html)
- [Apache Cassandra cqlsh documentation](https://cassandra.apache.org/doc/stable/cassandra/managing/tools/cqlsh.html)
- [Apache Cassandra native protocol](https://cassandra.apache.org/doc/latest/cassandra/reference/native-protocol.html)
- [Apache Cassandra nodetool import](https://cassandra.apache.org/doc/stable/cassandra/managing/tools/nodetool/import.html)
- [Cassandra 3.11 BigFormat source](https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java)
- [Cassandra 4.0 BigFormat source](https://github.com/apache/cassandra/blob/cassandra-4.0/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java)
- [Cassandra 4.1 BigFormat source](https://github.com/apache/cassandra/blob/cassandra-4.1/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java)
- [Cassandra 5.0 BigFormat source](https://github.com/apache/cassandra/blob/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java)
- [Cassandra 5.0 BtiFormat source](https://github.com/apache/cassandra/blob/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java)
- [Cassandra 3.11 configurable native QueryHandler](https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/service/ClientState.java)
- [Cassandra 4.0 configurable native QueryHandler](https://github.com/apache/cassandra/blob/cassandra-4.0/src/java/org/apache/cassandra/service/ClientState.java)
- [Cassandra 4.1 configurable native QueryHandler](https://github.com/apache/cassandra/blob/cassandra-4.1/src/java/org/apache/cassandra/service/ClientState.java)
- [Cassandra 5.0 configurable native QueryHandler](https://github.com/apache/cassandra/blob/cassandra-5.0/src/java/org/apache/cassandra/service/ClientState.java)
