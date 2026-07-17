# Cassandra 3.11 Isolated Sandbox Findings

- **Status:** Implemented vertical prototype
- **Decision:** GO for the isolated-daemon architecture; NO-GO for production
  use until the blockers below are complete
- **Validated runtime:** Apache Cassandra 3.11.19 final distribution, JDK 8,
  native protocol v4
- **Last updated:** 2026-07-16

## Result

A thin project JAR can use an installed Cassandra 3.11.19 distribution as its
runtime, start Cassandra's real storage and native-protocol code in a separate
JVM, accept CQL through the distribution's unmodified `cqlsh`, survive forced
termination, replay only its private commit log, and drain cleanly. Cassandra
libraries remain `provided`; no Cassandra classes or transitive libraries are
packaged in the tool JAR.

This validates the central architecture proposed in
`cql-mutation-workspace-design.md`: Cassandra should own CQL mutation and
storage semantics, with one release-specific worker process per release line.
It now also validates and imports genuine Cassandra 3.11 Big SSTables before
native transport is enabled, publishes verified exports, and reopens an
original base plus generated delta in a fresh workspace.

## Process and classpath contract

The Cassandra-free bootstrap discovers the installation and constructs the
child classpath in this order:

1. the selected release-specific thin JAR;
2. the selected Cassandra configuration directory;
3. the installed distribution JARs in deterministic filename order.

The worker uses the Java home selected for Cassandra, adds the installed Jamm
JAR as `-javaagent`, removes inherited Java option variables, and redirects
stdout/stderr below `workspace/logs`. Tarball layouts are validated by the
integration test. Split Debian/RPM home and configuration layouts are supported
by discovery but still need real package fixtures.

## Isolation controls

The controller generates `runtime/cassandra.yaml` below the workspace and sets:

```text
cassandra.start_gossip=false
cassandra.join_ring=false
cassandra.load_ring_state=false
cassandra.start_rpc=false
cassandra.start_native_transport=true
cassandra.config=file:<workspace>/runtime/cassandra.yaml
cassandra.storagedir=<workspace>
cassandra.logdir=<workspace>/logs
java.io.tmpdir=<workspace>/runtime/tmp
```

The YAML binds native and RPC addresses to `127.0.0.1`, uses an allocated native
port, disables Thrift, CDC, hinted handoff, snapshots, and backups, and directs
data, commit log, hints, caches, CDC, logs, and temporary files below the
workspace. JMX connector properties are absent and rejected if injected. The
worker JVM also uses `-XX:+DisableAttachMechanism`, preventing ordinary
same-user JVM attach tools from dynamically starting a local management agent.
Cassandra still registers in-process MBeans, but no JMX connector exposes them.
The adapter validates every address and mutable path after
`DatabaseDescriptor.daemonInitialization()` and aborts on a symlink or path
escape.

Cassandra 3.11 returns early from `StorageService.initServer()` when gossip is
disabled. That prevents `MessagingService.listen()` and all seed contact, but
also omits the local token and gossip endpoint state required by CQL replica
selection and schema announcements. The adapter installs only local state:

- `Gossiper.maybeInitializeLocalState()` creates the in-memory local endpoint;
- `StorageService.setTokens()` persists the configured token to workspace
  system tables and updates local `TokenMetadata`;
- neither the gossip scheduler nor the internode listener is started.

Readiness and every health check require native transport to be running while
`Gossiper.isEnabled()` and `MessagingService.isListening()` remain false.

## Native CQL policy boundary

Sandbox startup pins the imported keyspace and table into controller-generated
JVM properties and requires Cassandra's
`cassandra.custom_query_handler_class` to name the 3.11
`WorkspaceQueryHandler`. The adapter refuses to start native transport when the
handler or target identity is absent. Import-only workers reject those
properties and never start native transport.

The handler asks Cassandra to parse and prepare each direct or prepared
statement before classifying it, then delegates only:

- `SELECT` against the one imported table;
- the `system.local`, `system.peers`, and `system_schema` reads required by
  stock cqlsh and its bundled driver;
- `USE`, which changes session state but grants no table access; and
- non-conditional, non-counter `INSERT` and `UPDATE` against the imported
  table.

It rejects every batch, `DELETE`, `TRUNCATE`, DDL, conditional mutation,
counter mutation, system-table write, non-workspace user-table access, and
unneeded system-table read with a stable `SSTABLE_TOOLS_POLICY` invalid-request
error. Rejection occurs before delegation to Cassandra's `QueryProcessor`.

The handler also inspects the native request's consistency level immediately
before delegation. It accepts only `ONE` and `LOCAL_ONE` for reads and writes.
It does not ignore or translate the client value. `ANY`, `TWO`, `THREE`,
`QUORUM`, `LOCAL_QUORUM`, `EACH_QUORUM`, `ALL`, `SERIAL`, and `LOCAL_SERIAL`
fail with a policy error. The sandbox keyspace is normalized to RF=1, so a
distributed acknowledgement level would otherwise be misleading rather than
evidence of source-cluster consistency.

During import, the matching Cassandra runtime deserializes the `STATS`
component of every source `Statistics.db` and returns the greatest
`StatsMetadata.maxTimestamp` through worker protocol v3. The controller records
it as `source.max-timestamp-micros` in the manifest and prints it during import,
start, and status. When that value remains ahead of the controller clock, the
tool warns that default wall-clock mutations may not win and tells the operator
to use an explicit `USING TIMESTAMP` above the recorded maximum.

The optional `--timestamp-policy after-source` policy persists a high-water
mark above both the source maximum and wall clock before each direct or prepared
mutation that omits both CQL and native-protocol timestamps. The selected
policy is immutable after first start, and the high-water survives stop, crash
recovery, and restart. Explicit timestamps remain unchanged. Stock Cassandra
3.11 cqlsh normally attaches a protocol timestamp, so its ordinary mutations
do not use the allocator; use explicit `USING TIMESTAMP` for a future-dated
source when operating through stock cqlsh.

The sandbox does not load or consult production Cassandra roles. Its custom
authenticator accepts one fixed `sstable_workspace` identity with a random
256-bit password generated for each worker start. The owner-only
`state/cqlshrc` carries that credential to stock cqlsh without placing the
password in command arguments. Authentication is limited to loopback clients,
and a fixed in-memory role manager grants login to only that non-superuser
identity. Cassandra's `AllowAllAuthorizer` remains configured: the parsed
statement guard, not production RBAC, is the authorization boundary.

`workspace cqlsh` validates that the selected Cassandra home, configuration,
and release match the runtime recorded by `start`, checks the authenticated
worker control endpoint and owner-only credential, and launches that
installation's stock client against the fixed loopback address. Interactive
mode allows no endpoint/authentication overrides; `--execute <CQL>` is the only
noninteractive client option exposed by the controller.

## Lifecycle and recovery

The child publishes a strict, atomic endpoint record containing workspace UUID,
PID, Cassandra release, loopback native/control endpoints, status, and times.
The control socket accepts `STATUS`, `FLUSH`, `VERIFY`, and `STOP` only with a
random 256-bit token stored as a mode-0600 workspace file. Graceful stop closes
native transport and drains Cassandra before publishing `STOPPED`. Start and
live status output also publish the native username and canonical `cqlshrc`
path. The native password rotates on restart, and graceful stop or dead-worker
recovery deletes its credential file.

After a failed health check, recovery first tries the authenticated control
endpoint. If it is unreachable, Linux `/proc/<pid>/cmdline` must prove that the
recorded worker command for the exact workspace UUID and path is gone. A live
matching process, unreadable identity, non-Linux host, or invalid PID blocks
recovery. A proven-dead worker recovers as `STOPPED` and may restart; Cassandra
then replays the private workspace commit log.

`workspace flush` closes native transport, waits for query-guard requests,
blockingly flushes the imported table, and atomically publishes an owner-only
full component inventory before endpoint state `FLUSHED`. The controller
verifies baseline inclusion and delta hashes before committing manifest state.
If the controller disappears after the worker commits, status, flush, or
recovery reconciles `RUNNING` to `FLUSHED`. A flushed worker exposes only the
authenticated control endpoint until stop; its CQL credential is deleted.

`workspace export` invokes authenticated `VERIFY` only from `FLUSHED`. The
worker reconciles the committed inventory with Cassandra's live readers, runs
extended SSTable verification, checks generated readers are latest-format and
unrepaired, counts logical rows, and atomically records owner-only evidence
bound to the flush hash. The controller publishes complete TOC component sets,
schema, and a deterministic export manifest through an fsynced atomic directory
rename. Delta mode excludes the baseline and declares its exact source hash
dependencies; snapshot mode includes all flushed descriptors. Existing output
is never overwritten. Each publication includes an inventoried
`.sstable-tools-export` ownership marker. Retry deletes a deterministic staging
directory only after validating that marker, rejecting symlinks and unexpected
entries, and requiring owner-only permissions throughout the partial tree.

## Automated evidence

The `cassandra-3.11-sandbox-it` Maven profile and GitHub Actions job:

1. start a complete production-like Cassandra daemon on loopback storage port
   7000 and native port 9042, with gossip and internode messaging enabled and
   every mutable path redirected to a private fixture root;
2. verify that production is queryable and has no peers, reject a complete
   source beneath its reported live storage root without creating a workspace,
   reject genuine `ma` fixture copies for schema, digest, required-index, and
   unsupported-format failures without retaining table data, exercise collision
   cleanup and retry, and then import it with the matching schema;
3. verify source hashes, full data digest, serialization header, extended
   Cassandra verification, one logical imported row, disabled auto-compaction,
   maximum source timestamp, and a baseline inventory while native transport
   remains disabled;
4. start the thin JAR against the same SHA-512-pinned 3.11.19 final tarball
   using JDK 8;
5. verify the worker's native and control endpoints are loopback-only,
   dynamically allocated, and distinct from the production ports, then prove
   that the selected JDK's `jcmd` cannot attach to the worker;
6. prove unauthenticated and incorrect-password cqlsh connections fail, then
   query through `workspace cqlsh --execute` and connect directly using the
   generated owner-only `cqlshrc` and the tarball's
   Python-2-only `cqlsh` under a SHA-256-pinned PyPy 2.7 runtime, disable Python
   bytecode writes into the Cassandra installation, and verify Cassandra
   3.11.19/native v4;
7. read the imported row, execute direct and prepared `INSERT`/`UPDATE`
   operations through native transport, prove direct and prepared paging with
   a page size of one, accept `ONE`/`LOCAL_ONE`, reject `QUORUM`/`ALL` before
   execution, preserve explicit CQL and protocol timestamps exactly, and
   advance the durable `after-source` high-water for timestamp-free clients;
8. prove direct `DELETE`, `TRUNCATE`, DDL, batch, conditional update, system
   write, and unneeded system read requests fail at the policy boundary, prove
   a prepared `DELETE` fails during prepare, and re-query rows and schema to
   show no forbidden change occurred;
9. send `SIGKILL` before flush, verify the production daemon remains queryable
   with no peers, detect worker failure, reconcile the PID, delete the stale
   native credential, and restart with a different password;
10. verify the inserted and updated values are restored by commit-log replay,
    restart without repeating the policy option, and prove the recorded policy
    and timestamp high-water are reused and advanced;
11. issue `workspace flush`, prove native CQL and its credential are removed,
    wait for all guarded requests, perform a blocking flush of only the target
    table with auto-compaction disabled, and verify the strict full inventory
    plus a non-empty post-baseline delta;
12. inject the controller-crash boundary where the worker/result are `FLUSHED`
    but the manifest remains `RUNNING`, then prove `workspace status` validates
    the inventory and completes the transition;
13. force the export controller to halt after verification, the first copied
    file, all payload files, the export manifest fsync, the publication rename,
    and the workspace-manifest save; after each halt, rerun the identical
    export, require an exact valid publication, and prove no staging directory
    remains;
14. run Cassandra extended verification and logical reconciliation, atomically
    publish a delta export, verify every recorded path, size, and SHA-256, prove
    no baseline component was copied, and replay the export command
    idempotently;
15. drain the exported worker to `STOPPED`, prove the native credential remains
    deleted, and verify the production daemon is still queryable before
    stopping the fixture;
16. import the original `ma` base plus generated latest-format `me` delta into
    a fresh workspace, re-query all logical values, write timestamps, and TTLs,
    and prove they match the pre-export state exactly;
17. use the installed distribution's stock `sstableloader` to stream that base
    and delta into the clean gossip/internode-enabled Cassandra fixture, then
    prove normal native reads return the same values and cell metadata; and
18. reject the repository's `mb` fixture because its explicit
   `LocalPartitioner` conflicts with the sandbox partitioner, successfully
   import its `mc` fixture, and verify source component hashes and all
   installation file metadata are unchanged.

Unit tests cover strict endpoint/import/flush/verification result parsing,
delta and snapshot publication, crash reconciliation and corruption refusal,
authenticated control, native credential parsing and loopback enforcement, the
fixed role manager, future-source timestamp warnings, private config generation,
schema capture/CQL splitting, child JVM arguments, lifecycle transitions,
baseline verification, stale PID handling, symlink/path confinement, and source
inventories.

## Remaining blockers

- Add equivalent live-source process evidence on non-Linux systems and document
  deployment behavior when Linux `/proc` hides processes owned by other users.
- Add a compatible future-dated SSTable fixture so CI proves reconciliation
  against a real source cell above wall clock, not only allocator boundaries
  derived from real statistics metadata.
- Add real Debian and RPM installation-layout fixtures.
- Port and revalidate the worker lifecycle against Cassandra 4.0, 4.1, and 5.0.

The Cassandra 3.11 vertical prototype covers the acceptance scope of issues #5
and #8 and the core import, schema/partitioner rejection, and
destination-collision paths from issue #6. Issue #6 remains open for compatible
`mb`, collection/tuple/UDT, compact/dense-table, and broader multi-SSTable
fixtures. Issues #7, #9, and #10 own remaining timestamp/query policy and the
other release adapters.
