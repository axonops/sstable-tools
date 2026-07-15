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
workspace. JMX properties are absent. The adapter validates every address and
mutable path after `DatabaseDescriptor.daemonInitialization()` and aborts on a
symlink or path escape.

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
2. verify that production is queryable and has no peers, reject a genuine `ma`
   fixture against a mismatched schema without retaining table data, exercise
   collision cleanup and retry, and then import it with the matching schema;
3. verify source hashes, full data digest, serialization header, extended
   Cassandra verification, one logical imported row, disabled auto-compaction,
   and a baseline inventory while native transport remains disabled;
4. start the thin JAR against the same SHA-512-pinned 3.11.19 final tarball
   using JDK 8;
5. verify the worker's native and control endpoints are loopback-only,
   dynamically allocated, and distinct from the production ports;
6. connect using the tarball's Python-2-only `cqlsh` under a SHA-256-pinned
   PyPy 2.7 runtime, disable Python bytecode writes into the Cassandra
   installation, and verify Cassandra 3.11.19/native v4;
7. read the imported row, then execute `INSERT` and `UPDATE` through native
   transport;
8. send `SIGKILL` before flush, verify the production daemon remains queryable
   with no peers, detect worker failure, reconcile the PID, and restart;
9. verify the inserted and updated values are restored by commit-log replay;
10. drain the worker to `STOPPED` and verify the production daemon is still
   queryable before stopping the fixture; and
11. reject the repository's `mb` fixture because its explicit
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
- Install a parsed-statement query guard. The prototype currently permits DDL
  so its test can create a fixture table; the product must allow only the
  documented read, `INSERT`, and `UPDATE` subset.
- Implement flush, baseline/delta classification, export, and post-export
  validation. The imported baseline is already recorded and reverified.
- Add real Debian and RPM installation-layout fixtures.
- Port and revalidate the worker lifecycle against Cassandra 4.0, 4.1, and 5.0.

The Cassandra 3.11 vertical prototype covers issue #5's acceptance scope and
the core import, schema/partitioner rejection, and destination-collision paths
from issue #6. Issue #6 remains open for compatible `mb`, checksum,
missing-component, unsupported-format, collection/UDT, and compact-table
fixtures. Issues #7 through #10 own query guarding, flush/export, and the other
release adapters.
