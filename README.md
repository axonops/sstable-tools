# SSTable Tools

[![CI](https://github.com/axonops/sstable-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/axonops/sstable-tools/actions/workflows/ci.yml)

SSTable Tools queries and mutates explicitly selected, stopped Cassandra
SSTables with the matching installed Cassandra release and its stock `cqlsh`.
It is under active development and is not yet an operator-ready production tool.

## Direct CQLSH

The primary interface is one command. Supply one or more comma-separated
`Data.db` or `TOC.txt` paths from one table directory, a schema bundle, and the
matching Cassandra installation. The tool opens stock `cqlsh`; on a clean exit
it writes verified new SSTable component sets beside the supplied source files.

```shell
java -jar workers/cassandra-5.0/target/sstable-tools-cassandra-5.0-0.1.0-SNAPSHOT.jar \
  --cassandra-home /opt/apache-cassandra-5.0.8 \
  --sstables /archive/acme/users-<table-id>/nb-42-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh
```

For multiple SSTables, use one comma-separated value (or repeat `--sstables`):

```shell
--sstables /archive/acme/users-<table-id>/nb-42-big-Data.db,/archive/acme/users-<table-id>/nb-43-big-Data.db
```

Use `--execute` for an automated statement and `--tmp-dir` to choose the parent
of the private temporary workspace. The default is `/tmp/sstable-tools/`; it is
removed after a successful operation.

```shell
java -jar workers/cassandra-4.1/target/sstable-tools-cassandra-4.1-0.1.0-SNAPSHOT.jar \
  --tmp-dir /var/tmp/sstable-tools \
  --cassandra-home /opt/apache-cassandra-4.1.3 \
  --sstables /archive/acme/users-<table-id>/na-17-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh --execute "INSERT INTO acme.users (id, name) VALUES (1, 'Ada');"
```

The command does not scan data roots, keyspaces, or unrelated tables. All
selected SSTables must be in the same table directory. Cassandra 3.11, 4.0, and
4.1 use the next numeric generation. Cassandra 5.0 uses the configured
sequence or UUID SSTable identifier mode from `cassandra.yaml`.

On Cassandra 5.0, `--output-format bti` selects BTI `da` delta output. The
default is Big `oa`; BTI is rejected by the 3.11, 4.0, and 4.1 JARs.

The former `workspace ...` commands remain available as an advanced diagnostic
and recovery interface; they are not the normal workflow.

> **DANGER:** Do not import, mutate, compact, or export files belonging to a
> running Cassandra process. The source must be an external completed copy.


## Building

This project uses [Apache Maven](https://maven.apache.org/) and is organized as a
single multi-module build. To compile and test every shared and version-specific
module, execute:

```shell
mvn test
```

Build the Cassandra 3.11, 4.0, 4.1, and 5.0 thin-JAR artifacts with:

```shell
mvn package
scripts/verify-thin-jars
```

Version-specific workspace artifacts are written below
`workers/cassandra-<line>/target/`.
The shared workspace manifest commands are available in all four thin JARs.
They provide schema/header validation, copy-based SSTable import, isolated
daemon start, guarded table flush, status, stop, recovery, confined workspace
destroy, release verification, and atomic delta/snapshot export. The native
endpoint is loopback-only and accepts only the workspace's guarded CQL surface.
Cross-platform live-source detection and broader fixture coverage remain under
development, so this is not yet an operator-ready write workflow.

> **DANGER:** Never use SSTable import, mutation, compaction, or export against
> files owned by a running production Cassandra process. Stop the owning
> Cassandra process, or use a completed snapshot or backup copied outside every
> live Cassandra data directory. The isolated Cassandra worker started by this
> tool is expected and writes only beneath its private workspace.

On Linux, `workspace create`, `import`, `start`, `cqlsh`, `flush`, and `export`
also scan visible `/proc` entries for an active Cassandra daemon. A source below
its reported `cassandra.storagedir`, or containing a file the daemon has open or
memory-mapped, is rejected. Creation performs this check before writing the
workspace directory. Restricted `/proc` visibility and non-Linux platforms
cannot provide the same process evidence, so this guard does not replace the
warning or the requirement to use an external completed copy.

The following is a Cassandra 3.11 example. Use the JAR, Cassandra home, and
Java runtime matching the target release line; Cassandra 4.0, 4.1, and 5.0
require `--timestamp-policy after-source` when starting a writable sandbox.

```shell
java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  workspace create ./case \
  --sstables /evidence/snapshot/table-directory/ma-1-big-Data.db \
  --schema /evidence/schema.cql

java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  --java-home /usr/lib/jvm/java-8-openjdk \
  workspace import ./case

java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  --java-home /usr/lib/jvm/java-8-openjdk \
  workspace start ./case --timestamp-policy after-source
```

`wall-clock` is the default timestamp policy. `after-source` durably assigns a
timestamp greater than both the imported source maximum and the controller
clock when a mutation supplies neither `USING TIMESTAMP` nor a native-protocol
timestamp. The policy is fixed by the first start and reused when later starts
omit the option. `workspace status` reports the selected policy and, for
`after-source`, its durable high-water mark.

`workspace start` and a live `workspace status` print `worker.native`,
`worker.username`, and `worker.cqlshrc`. Pass the generated owner-only
configuration to the installation's unmodified `cqlsh`; the password does not
appear in the process arguments:

```shell
/opt/apache-cassandra-3.11.19/bin/cqlsh \
  --cqlshrc /absolute/path/from/worker.cqlshrc \
  127.0.0.1 PORT_FROM_worker.native
```

The controller can launch that exact client and fixed workspace endpoint
without placing the password in its arguments:

```shell
java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  --java-home /usr/lib/jvm/java-8-openjdk \
  workspace cqlsh ./case

java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  --java-home /usr/lib/jvm/java-8-openjdk \
  workspace cqlsh ./case --execute "SELECT * FROM blog.users;"
```

The launcher requires the same canonical Cassandra home, configuration, and
release recorded when the worker started. It accepts only `--execute`; endpoint,
authentication, and client configuration overrides are intentionally not
passed through.

Cassandra 3.11's stock cqlsh Python driver normally attaches a protocol
timestamp to mutations. The guard preserves that timestamp, so `after-source`
does not replace ordinary stock-cqlsh timestamps. When the imported maximum is
in the future, use an explicit `USING TIMESTAMP` greater than
`source.maxTimestampMicros`. Direct and prepared clients that omit both forms
of timestamp use the durable `after-source` allocator.

After completing mutations, quiesce native transport and flush the exact
workspace table:

```shell
java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  workspace flush ./case
```

Flush closes all CQL connections, waits for guarded requests, disables
auto-compaction, performs a blocking table flush, and atomically records the
complete checksummed table inventory in `state/flush-result.json`. It removes
the native credential and reports the full and post-import delta file counts.
The worker remains available only through its authenticated control endpoint
until `workspace stop`. Publish either the generated delta or a self-contained
snapshot to a new destination directory:

```shell
java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  workspace export ./case --mode delta --output ./case-delta
```

Export first asks the quiesced release worker to run Cassandra's extended
SSTable verification and a logical row-count scan. It records owner-only,
flush-bound evidence in `state/verification-result.properties`, validates
complete TOC-declared descriptor sets, copies and fsyncs into a private sibling
directory, and publishes with an atomic directory rename. The output contains
the inventoried ownership marker `.sstable-tools-export`,
`export-manifest.json`, `schema.cql`, and `sstables/`. Delta manifests list the
exact source component hashes required alongside the generated SSTables;
snapshot mode includes the entire flushed table. Existing output is never
overwritten and is adopted after a controller crash only when every path, size,
and SHA-256 matches the deterministic publication. A retry removes a partial
staging directory only when its owner-only marker has the exact expected
workspace, flush, mode, and export identity.

After stopping a worker, obtain the exact UUID from `workspace status` and
repeat it as the destructive confirmation:

```shell
java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  workspace stop ./case

java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  workspace destroy ./case \
  --confirm-workspace-id UUID_FROM_workspace.status
```

Destroy is accepted only for `NEW`, `VALIDATED`, `IMPORTED`, or `STOPPED`.
It takes the exclusive workspace lock, rejects a mismatched UUID, refuses
active or unrecovered states and a still-running recorded worker, and will not
delete a root containing unexpected top-level entries. The confined walk never
follows symlinks. Source SSTables, schema inputs, and published export
directories are outside the workspace and are not part of deletion.

The schema bundle and SSTable sources must remain outside the workspace and
unchanged. The importer recognizes Cassandra 3.11 row-storage Big `ma` through
the installed patch's latest compatible version (`me` in 3.11.19), with a
complete digest-bearing component set and matching schema and partitioner.
Real fixture coverage includes `ma` and `mc`; generated `me` delta SSTables are
re-imported with the `ma` base in a fresh workspace. The repository's `mb`
fixture declares `LocalPartitioner`, so the integration test proves that it is
rejected against the sandbox's `Murmur3Partitioner`; compatible `mb` and `md`
fixtures remain required before claiming full fixture coverage.

`workspace recover` covers every persisted lifecycle state. `NEW`, `VALIDATED`,
and `IMPORTED` recover only after immutable input and baseline checks. Failed
active/flushed/exported states reconcile the authenticated endpoint and exact
recorded PID; a dead worker can fall back only to `STOPPED` when Linux `/proc`
proves the matching process is gone. Failed `STOPPED` recovery requires the
same process evidence and removes any stale native credential.

The Cassandra 3.11 worker uses a custom native-protocol query handler. It
allows cqlsh metadata reads, `SELECT` against the imported table, and
non-conditional `INSERT`/`UPDATE` against that table. It rejects `DELETE`,
`TRUNCATE`, DDL, batches, LWT, counters, other user tables, system writes, and
unneeded system reads before Cassandra executes them. Production roles are not
loaded. A workspace authenticator and fixed in-memory role manager accept only
the `sstable_workspace` identity with a random 256-bit password from
`state/cqlshrc`, and only from loopback. The password rotates on every start;
the file is removed after a graceful stop or dead-worker recovery. Cassandra's
`AllowAllAuthorizer` remains configured because the parsed-statement guard, not
production RBAC, is the authorization boundary.

Native requests must use consistency `ONE` or `LOCAL_ONE`. The guard preserves
the requested value and rejects every other consistency level; it never
silently downgrades `QUORUM`, `ALL`, or another distributed level. Because the
sandbox keyspace is RF=1, it provides no evidence about source-cluster replica
acknowledgement, repair, or Paxos behavior.

Import deserializes each source `Statistics.db` with the installed Cassandra
runtime and records the greatest `StatsMetadata.maxTimestamp` as
`source.maxTimestampMicros`. Import, start, and status print that value and the
current controller time. If the source maximum is still in the future, they
print a warning that default wall-clock writes may not win and identify the
minimum explicit `USING TIMESTAMP` boundary. The optional `after-source` policy
durably allocates above that bound only for clients that omit both CQL and
native-protocol timestamps; explicit timestamps are never rewritten.

The [thin JAR dependency record](docs/packaging-dependencies.md) documents the
provided/packaged boundary and the build checks that enforce it. GitHub Actions
runs the complete reactor on Java 17 and tests each adapter on its declared Java
runtime.

The CI release bundle contains the four thin JARs, SHA-256 checksums, a
compatibility manifest, SPDX SBOM, and third-party notices. See the
[Cassandra node deployment guide](docs/node-deployment.md) for installation,
preflight, operational storage, and rollback guidance.

### Runtime discovery and preflight

Each thin JAR can inspect a Cassandra installation without loading Cassandra
classes into the bootstrap process. For a tarball installation:

```shell
java -jar workers/cassandra-3.11/target/sstable-tools-cassandra-3.11-*.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  --java-home /usr/lib/jvm/java-8-openjdk \
  runtime inspect
```

For Debian/RPM-style layouts, pass the separate configuration directory when
it cannot be discovered unambiguously. Cassandra 3.11's RPM package places it
under `default.conf`:

```shell
java -jar workers/cassandra-4.1/target/sstable-tools-cassandra-4.1-*.jar \
  --cassandra-home /usr/share/cassandra \
  --cassandra-conf /etc/cassandra/default.conf \
  --java-home /usr/lib/jvm/java-11-openjdk \
  runtime preflight
```

`CASSANDRA_HOME`, `CASSANDRA_CONF`, and `JAVA_HOME` are used when the matching
command-line option is absent. `runtime inspect` prints canonical runtime paths,
versions, the deterministic child classpath, and SHA-256 identities.
`runtime preflight` starts a separate worker JVM with that classpath and checks
the Cassandra APIs required by the adapter.

The real Cassandra 3.11 sandbox test is opt-in and requires an unpacked final
3.11.19 distribution plus JDK 8:

```shell
mvn clean verify -pl workers/cassandra-3.11 -am \
  -Pcassandra-3.11-sandbox-it \
  -Dcassandra311.home=/opt/apache-cassandra-3.11.19 \
  -Dcassandra311.java.home=/usr/lib/jvm/java-8-openjdk
```

It first starts a complete production-like daemon on normal ports 7000 and
9042. The pinned Cassandra runtime's `CQLSSTableWriter` then generates a real
latest-format `me` source whose cell timestamp is one year ahead of wall clock.
The profile imports independent copies under both policies, proves an ordinary
stock-cqlsh update loses under `wall-clock`, proves a timestamp-free prepared
update wins under `after-source`, and verifies the source remains unchanged and
the production daemon remains queryable. A second writer-generated `me` source
contains scalar, collection, tuple, frozen UDT, static, and regular cells. Stock
cqlsh reads that source, executes rich `INSERT` and `UPDATE` statements with a
live TTL, and reads the merged rows; a prepared driver independently validates
every value and TTL. The profile flushes and exports that delta, reopens base
plus delta in a fresh workspace, and proves the same logical state and source
immutability. It then rejects a complete source
placed beneath that daemon's live storage root before creating a workspace,
rejects schema, partitioner, digest,
required-component, unsupported-format, and destination-collision failures
without retaining user data, then imports matching `ma` and `mc` fixtures. It
starts the thin JAR worker from the
same installed distribution on private loopback endpoints, reads the imported
row, records the maximum source timestamp from real Statistics metadata,
proves missing and incorrect credentials are rejected, proves the native port
refuses the host's non-loopback address, rejects the direct/prepared forbidden
matrix and flushes exactly zero delta files before any allowed write, then
restarts with a rotated credential and runs `INSERT` and
`UPDATE` with the generated `cqlshrc` and the distribution's `cqlsh`, verifies
forbidden direct and prepared statements are rejected without data/schema
changes, exercises direct and prepared paging, accepts `ONE`/`LOCAL_ONE`,
rejects `QUORUM`/`ALL`, verifies exact explicit CQL/native timestamps, and
exercises durable `after-source` allocation from clients with client timestamp
generation disabled. It then forces worker termination, verifies the
production daemon remains isolated and queryable, reconciles the recorded
worker PID, removes the stale credential, restarts with a rotated credential,
verifies workspace commit-log replay and timestamp high-water advancement, and
then quiesces CQL, flushes and inventories generated table files, exercises
manifest reconciliation after an interrupted controller transition, runs
Cassandra's extended verifier, atomically publishes a delta, and drains cleanly
without retaining the credential file. Finally it imports the original `ma`
base plus the generated latest-format `me` delta into a fresh workspace and
proves all values, write timestamps, and TTLs are identical. The distribution's
stock `sstableloader` then streams the same base and delta into the
gossip/internode-enabled clean Cassandra fixture, where normal native reads
prove the same cell metadata again.
GitHub Actions is configured to run the same profile against a SHA-512-pinned 3.11.19 archive,
supplies a SHA-256-pinned PyPy 2.7 runtime required by that release's `cqlsh`
launcher, and preflights SHA-256-pinned official 3.11.19 Debian and RPM
packages after extraction only. The Debian fixture uses `/usr/share/cassandra`
with `/etc/cassandra`; the RPM fixture uses `/usr/share/cassandra` with
`/etc/cassandra/default.conf`. No package service script is called.

Each CI job publishes its executed Surefire/Failsafe suites, exact test counts,
and failures/errors/skips to both the job log and the GitHub Actions run
summary. The live Cassandra 3.11 job includes the cqlsh mutation, recovery,
flush, export, reopen, and clean-node-import integration suite.

The `stopped-cqlsh-source` matrix additionally starts a stock Cassandra Docker
image for every supported release line. Its stock `cqlsh` creates a table,
executes `INSERT` and `UPDATE`, reads the mutated row, and flushes it. The job
then stops the source node before copying its SSTables and asks the matching
tool JAR to create and integrity-check a workspace from that completed copy.
This proves that each artifact accepts real, version-matched SSTable component
sets without ever pointing the tool at a live source node. The 4.0, 4.1, and
5.0 integration jobs extend this stopped-source sequence through workspace
import, an isolated loopback sandbox, the distribution's stock `cqlsh`
`INSERT`, `UPDATE`, and `SELECT`, then flush and delta export.

Compatibility remains deliberately conservative until the remaining release
lines have equivalent installed-package fixtures in CI:

| Artifact | Tested Cassandra patch | Java runtime | CI-proven workspace path |
|---|---:|---:|---|
| `cassandra-3.11` | 3.11.19 | 8 | Import, stock `cqlsh` `INSERT`/`UPDATE`/`SELECT`, flush, export |
| `cassandra-4.0` | 4.0.17 | 8-11 | Import, stock `cqlsh` `INSERT`/`UPDATE`/`SELECT`, flush, export |
| `cassandra-4.1` | 4.1.3 | 11 | Import, stock `cqlsh` `INSERT`/`UPDATE`/`SELECT`, flush, export |
| `cassandra-5.0` | 5.0.4 | 17 | Import, stock `cqlsh` `INSERT`/`UPDATE`/`SELECT`, flush, export |

The implemented capability boundary is deliberately narrow: 3.11, 4.0, 4.1,
and 5.0 support import plus the guarded writable sandbox. Every import uses a
selected `Data.db` or `TOC.txt`, never a broad Cassandra data-root scan.

## Design documents

* [CQL mutation workspaces for SSTables](docs/cql-mutation-workspace-design.md) -
  Proposed design for safe `INSERT` and `UPDATE` support with Apache cqlsh
  across Cassandra 3.11, 4.0, 4.1, and 5.0 formats.
* [Workspace manifest and lifecycle contract](docs/workspace-manifest.md) -
  Implemented source inventory, atomic manifest, locking, path confinement,
  lifecycle, recovery, and command behavior.
* [Cassandra 3.11 sandbox findings](docs/cassandra-3.11-sandbox-findings.md) -
  Implemented vertical prototype, isolation controls, test evidence, and
  remaining go-live blockers.
* [Real Cassandra CI coverage](docs/ci-real-cassandra-testing.md) -
  Stock-cqlsh source generation coverage for every release line and the
  current boundary for full write-workflow acceptance.
