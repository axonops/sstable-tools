# Cassandra 3.11 Isolated Sandbox Findings

- **Status:** Implemented vertical prototype
- **Decision:** GO for the isolated-daemon architecture; NO-GO for production
  use until the blockers below are complete
- **Validated runtime:** Apache Cassandra 3.11.19 final distribution, JDK 8,
  native protocol v4
- **Last updated:** 2026-07-15

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
native transport is enabled. Export is not implemented yet.

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

The sandbox still uses `AllowAllAuthenticator` and `AllowAllAuthorizer` and
does not load production Cassandra roles. The query guard, not production RBAC,
is the current authorization boundary. The endpoint is loopback-only, but
ephemeral per-workspace native-protocol credentials are still required before
this becomes an operator-ready write tool.

## Lifecycle and recovery

The child publishes a strict, atomic endpoint record containing workspace UUID,
PID, Cassandra release, loopback native/control endpoints, status, and times.
The control socket accepts `STATUS` and `STOP` only with a random 256-bit token
stored as a mode-0600 workspace file. Graceful stop closes native transport and
drains Cassandra before publishing `STOPPED`.

After a failed health check, recovery first tries the authenticated control
endpoint. If it is unreachable, Linux `/proc/<pid>/cmdline` must prove that the
recorded worker command for the exact workspace UUID and path is gone. A live
matching process, unreadable identity, non-Linux host, or invalid PID blocks
recovery. A proven-dead worker recovers as `STOPPED` and may restart; Cassandra
then replays the private workspace commit log.

## Automated evidence

The `cassandra-3.11-sandbox-it` Maven profile and GitHub Actions job:

1. start a complete production-like Cassandra daemon on loopback storage port
   7000 and native port 9042, with gossip and internode messaging enabled and
   every mutable path redirected to a private fixture root;
2. verify that production is queryable and has no peers, reject genuine `ma`
   fixture copies for schema, digest, required-index, and unsupported-format
   failures without retaining table data, exercise collision cleanup and retry,
   and then import it with the matching schema;
3. verify source hashes, full data digest, serialization header, extended
   Cassandra verification, one logical imported row, disabled auto-compaction,
   and a baseline inventory while native transport remains disabled;
4. start the thin JAR against the same SHA-512-pinned 3.11.19 final tarball
   using JDK 8;
5. verify the worker's native and control endpoints are loopback-only,
   dynamically allocated, and distinct from the production ports, then prove
   that the selected JDK's `jcmd` cannot attach to the worker;
6. connect using the tarball's Python-2-only `cqlsh` under a SHA-256-pinned
   PyPy 2.7 runtime, disable Python bytecode writes into the Cassandra
   installation, and verify Cassandra 3.11.19/native v4;
7. read the imported row, then execute direct and prepared `INSERT`/`UPDATE`
   operations through native transport;
8. prove direct `DELETE`, `TRUNCATE`, DDL, batch, conditional update, system
   write, and unneeded system read requests fail at the policy boundary, prove
   a prepared `DELETE` fails during prepare, and re-query rows and schema to
   show no forbidden change occurred;
9. send `SIGKILL` before flush, verify the production daemon remains queryable
   with no peers, detect worker failure, reconcile the PID, and restart;
10. verify the inserted and updated values are restored by commit-log replay;
11. drain the worker to `STOPPED` and verify the production daemon is still
   queryable before stopping the fixture; and
12. reject the repository's `mb` fixture because its explicit
   `LocalPartitioner` conflicts with the sandbox partitioner, successfully
   import its `mc` fixture, and verify source component hashes and all
   installation file metadata are unchanged.

Unit tests cover strict endpoint/import-result parsing and publication,
authenticated control, private config generation, schema capture/CQL splitting,
child JVM arguments, lifecycle transitions, baseline verification, stale PID
handling, symlink/path confinement, and source inventories.

## Remaining blockers

- Reject live Cassandra data directories and require stable snapshot/backup
  component sets.
- Add ephemeral per-workspace native-protocol credentials and timestamp-policy
  enforcement to the parsed-statement guard.
- Implement flush, baseline/delta classification, export, and post-export
  validation. The imported baseline is already recorded and reverified.
- Add real Debian and RPM installation-layout fixtures.
- Port and revalidate the worker lifecycle against Cassandra 4.0, 4.1, and 5.0.

The Cassandra 3.11 vertical prototype covers issue #5's acceptance scope and
the core import, schema/partitioner rejection, and destination-collision paths
from issue #6. Issue #6 remains open for compatible `mb`, collection/tuple/UDT,
compact/dense-table, and broader multi-SSTable fixtures. Issues #7 through #10
own remaining query-guard policy, flush/export, and the other release adapters.
