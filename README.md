# SSTable Tools

[![CI](https://github.com/axonops/sstable-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/axonops/sstable-tools/actions/workflows/ci.yml)

SSTable Tools queries and mutates explicitly selected, stopped Cassandra
SSTables with the matching installed Cassandra release and its stock `cqlsh`.
It is under active development and is not yet an operator-ready production tool.

## Launcher and Automatic Adapter Selection

Use `sstable-tools` from the DEB/RPM, or `./sstable-tools` from the standalone
archive. Point it at Cassandra's `lib` directory with either
`--cassandra-lib-dir` or `CASSANDRA_LIB_DIR`:

```shell
tar -xzf sstable-tools-1.2.3.tar.gz
cd sstable-tools-1.2.3

./sstable-tools \
  --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --version
```

The launcher finds exactly one `apache-cassandra-<version>.jar` or
`cassandra-all-<version>.jar`, maps versions `3.11.x`, `4.0.x`, `4.1.x`, and
`5.0.x` to the matching adapter, derives the Cassandra home from the parent
directory, and forwards the remaining arguments. Multiple or unsupported core
JARs fail instead of guessing.

The selected adapter launcher chooses Java in this order:
`SSTABLE_TOOLS_JAVA`, `JAVA_HOME/bin/java`, then `java` from `PATH`. It always
enables headless mode and UTF-8. Add whitespace-delimited launcher-JVM options
with `SSTABLE_TOOLS_JAVA_OPTS`:

```shell
CASSANDRA_LIB_DIR=/opt/apache-cassandra-5.0.8/lib \
SSTABLE_TOOLS_JAVA_OPTS="-Xms256m -Xmx2g" \
  sstable-tools --version
```

The explicit `sstable-tools-cassandra-3.11`, `-4.0`, `-4.1`, and `-5.0`
commands remain available for diagnosis, but normal use should not need them.
The examples below use the installed `sstable-tools` command; prefix it with
`./` when running from an unpacked release archive.

## Direct CQLSH

The primary interface is one command. Supply either explicit `Data.db` or
`TOC.txt` paths with `--sstables`, or one table directory with `--output-dir`,
plus a schema bundle and the matching Cassandra installation. The two input
modes are mutually exclusive. The tool opens stock `cqlsh`; on a clean exit
after a mutation it publishes verified new SSTable component sets into the
selected table directory.

```shell
sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --sstables /archive/acme/users-7ad54392bcdd35a684174e047860b377/nb-42-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh
```

The schema bundle must describe the exact keyspace, table, primary key, column
types, table ID, and any UDTs used by the selected SSTables.

> **Partitioner limitation:** SSTable Tools currently supports only
> `org.apache.cassandra.dht.Murmur3Partitioner`. SSTables produced with
> `RandomPartitioner`, `ByteOrderedPartitioner`, `LocalPartitioner`, or another
> partitioner are not supported.

### Run a read-only query

Use `--execute` to run one statement without opening an interactive shell. A
read-only session validates and queries the selected data but publishes no new
SSTables:

```shell
sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --sstables /archive/acme/users-7ad54392bcdd35a684174e047860b377/nb-42-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh --execute "SELECT id, name FROM acme.users WHERE id = 1;"
```

### Select multiple SSTables

All selected descriptors must belong to the same table directory. Repeat
`--sstables` or provide a comma-separated list:

```shell
sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --sstables /archive/acme/users-7ad54392bcdd35a684174e047860b377/nb-42-big-Data.db \
  --sstables /archive/acme/users-7ad54392bcdd35a684174e047860b377/nb-43-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh --execute "SELECT id, name FROM acme.users;"
```

The direct command accepts `SELECT` plus non-conditional `INSERT` and `UPDATE`
for the selected table. It does not scan data roots, keyspaces, or unrelated
tables. All selected SSTables must be in one table directory.

### Create the first SSTable in an output directory

An `INSERT` does not need an existing SSTable. Use `--output-dir` instead of
`--sstables` when the destination table directory is the source of truth:

```shell
mkdir -p /archive/acme/users-7ad54392bcdd35a684174e047860b377
NOW_MICROS=$(date +%s%6N)
INSERT_CQL="INSERT INTO acme.users (id, name) VALUES (1, 'Ada') USING TIMESTAMP $NOW_MICROS;"

sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --output-dir /archive/acme/users-7ad54392bcdd35a684174e047860b377 \
  --schema /archive/acme-users.cql \
  --output-format bti \
  cqlsh --execute "$INSERT_CQL"
```

The directory must already exist and must not be a symlink. If it is empty,
direct CQLSH requires `--execute` with an `INSERT` statement. Cassandra 5.0
uses UUID-style identifiers for the first SSTable; older release lines use a
numeric identifier.

If the directory already contains complete SSTables, the tool inventories and
imports all of them as the baseline. Numeric output is allocated at one above
the highest numeric identifier in the directory. UUID-style output keeps the
fresh Cassandra-generated identifier; UUIDs are not numerically incremented.
The complete directory is checked again immediately before publication, and
publication fails if it changed after the baseline was imported.

### Insert or update a row

For automation, generate the CQL separately so the microsecond timestamp is
obvious:

```shell
NOW_MICROS=$(date +%s%6N)
WRITE_CQL="UPDATE acme.users USING TIMESTAMP $NOW_MICROS SET name = 'Grace' WHERE id = 1;"

sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --tmp-dir /var/tmp/sstable-tools \
  --output-dir /archive/acme/users-7ad54392bcdd35a684174e047860b377 \
  --schema /archive/acme-users.cql \
  cqlsh --execute "$WRITE_CQL"
```

`--tmp-dir` changes the parent of the private temporary workspace. The default
is `/tmp/sstable-tools/`. A successful invocation removes its private child;
a failed invocation retains it and prints the path for diagnosis.

### Write timestamps

`INSERT` and `UPDATE` statements must include an explicit `USING TIMESTAMP`.
The value is a Unix timestamp in **microseconds** and must be greater than the
maximum timestamp in the selected source SSTables. Cassandra's CQL `now()`
function returns a `timeuuid` and cannot be used for this clause.

If the local clock is ahead of every source timestamp, generate a suitable
current timestamp with `date +%s%6N`:

```sql
INSERT INTO acme.users (id, name)
VALUES (1, 'Ada')
USING TIMESTAMP 1785170000000000;

UPDATE acme.users
USING TIMESTAMP 1785170000000001
SET name = 'Grace'
WHERE id = 1;
```

The Cassandra 4.0, 4.1, and 5.0 guards reject a missing timestamp, or an
explicit timestamp that is not greater than the imported source maximum. If a
source contains future-dated cells, do not use the current clock value; choose
a microsecond value above the reported source maximum instead.

### Query or update a system table

Stopped SSTables from Cassandra's built-in `system` keyspace are supported.
The schema bundle identifies exactly one fully qualified built-in table; the
matching Cassandra runtime supplies its actual metadata.

```shell
NOW_MICROS=$(date +%s%6N)
SYSTEM_CQL="UPDATE system.local USING TIMESTAMP $NOW_MICROS SET cluster_name = 'recovered-cluster' WHERE key = 'local';"

sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --output-dir /archive/system/local-7ad54392bcdd35a684174e047860b377 \
  --schema /archive/system.local.cql \
  --output-format bti \
  cqlsh --execute "$SYSTEM_CQL"
```

This inventories the complete stopped table directory, changes only the
isolated copy, and publishes verified sibling SSTables into the same directory.
It does not change a running node or Cassandra installation.

### Write Cassandra 5.0 BTI output

On Cassandra 5.0, `--output-format bti` selects BTI `da` output. The default is
Big `oa`; BTI is rejected by the 3.11, 4.0, and 4.1 adapters.

```shell
NOW_MICROS=$(date +%s%6N)
INSERT_CQL="INSERT INTO acme.users (id, name) VALUES (2, 'Ada') USING TIMESTAMP $NOW_MICROS;"

sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --sstables /archive/acme/users-7ad54392bcdd35a684174e047860b377/nb-42-big-Data.db \
  --schema /archive/acme-users.cql \
  --output-format bti \
  cqlsh --execute "$INSERT_CQL"
```

Original components are never modified. Every adapter allocates the next
available numeric SSTable identifier when the selected baseline uses numeric
identifiers. For Cassandra 5.0, selecting a descriptor with Cassandra's
28-character UUID/ULID-style identifier enables
`uuid_sstable_identifiers_enabled` in the private sandbox and publishes the
Cassandra-generated UUID-style delta unchanged. A mixed numeric and UUID-style
selection uses UUID-style output. With `--sstables`, inference uses only the
explicitly selected descriptors. With `--output-dir`, it uses every complete
SSTable in that directory. Publication does not read the installation's
`cassandra.yaml`.

After a successful direct mutation, later commands for that table should select
both the original descriptors and every published sibling descriptor that
should participate in reconciliation. Using the same `--output-dir` does this
automatically.

> **DANGER:** Do not mutate, compact, import, or export files belonging to a
> running Cassandra process. The source must be an external completed copy,
> snapshot, or backup. The isolated worker is expected; it uses only private
> storage and loopback networking.

## Command-Line Reference

The general forms are:

```text
sstable-tools --cassandra-lib-dir <path> [tool options] <command>
CASSANDRA_LIB_DIR=<path> sstable-tools [tool options] <command>
```

`--cassandra-lib-dir` is a launcher option and must immediately follow
`sstable-tools`. Forwarded tool options may appear before or after the command.

| Option | Value and scope |
|---|---|
| `--cassandra-lib-dir <path>` | Cassandra `lib` directory used to detect the release and select an adapter. Equivalent to `CASSANDRA_LIB_DIR`. |
| `--cassandra-home <path>` | Cassandra installation used by direct `cqlsh`, runtime commands, and workspace `import`, `start`, or `cqlsh`. |
| `--java-home <path>` | Compatible Java installation for the selected Cassandra worker. |
| `--sstables <path>` | Explicit `Data.db` or `TOC.txt` source. Repeat it or use comma-separated paths. Valid only with direct `cqlsh` and `workspace create`. |
| `--output-dir <path>` | Existing non-symlink table directory used as the complete baseline and publication destination. Valid only with direct `cqlsh` and mutually exclusive with `--sstables`. |
| `--schema <path>` | UTF-8 CQL schema bundle. Required by direct `cqlsh`; recorded by `workspace create`. |
| `--timestamp-policy <policy>` | `wall-clock` or `after-source`. Valid with direct `cqlsh` and `workspace start`; the choice is persisted for a managed workspace. |
| `--output-format <format>` | `big` (default) or `bti`. Valid with direct `cqlsh` and `workspace create`; `bti` requires Cassandra 5.0. |
| `--tmp-dir <path>` | Parent for private direct-CQLSH workspaces. Default: `/tmp/sstable-tools`. |
| `--execute <cql>` | Run one CQL statement and exit. Valid with direct `cqlsh` and `workspace cqlsh`. |
| `--mode <mode>` | `delta` or `snapshot`; required by `workspace export`. |
| `--output <path>` | Atomic publication destination; required by `workspace export`. |
| `--confirm-workspace-id <uuid>` | Exact manifest UUID required by `workspace destroy`. |
| `--version` | Print the tool, adapter, and compile-target versions. |
| `--help`, `-h` | Print command and option help. |

`after-source` maintains a durable timestamp high-water above the imported
source maximum for timestamp-free requests that support it. It does not remove
the explicit `USING TIMESTAMP` requirement enforced by the Cassandra 4.0, 4.1,
and 5.0 query guards.

### Launcher environment

| Variable | Purpose |
|---|---|
| `CASSANDRA_LIB_DIR` | Cassandra `lib` directory containing one supported core JAR; selects the adapter automatically. |
| `CASSANDRA_HOME` | Fallback Cassandra home; the universal launcher scans `$CASSANDRA_HOME/lib`. |
| `SSTABLE_TOOLS_JAVA` | Exact Java executable used by the launcher. |
| `JAVA_HOME` | Supplies `$JAVA_HOME/bin/java` when `SSTABLE_TOOLS_JAVA` is unset. |
| `SSTABLE_TOOLS_JAVA_OPTS` | Additional whitespace-delimited launcher-JVM options, such as `-Xms256m -Xmx2g`. |
| `SSTABLE_TOOLS_HOME` | Override the installed JAR directory; normally `/usr/share/sstable-tools`. |
| `SSTABLE_TOOLS_JAR` | Override the exact adapter JAR path for development or diagnosis. |

The isolated Cassandra worker is started separately with `-Xms512m`,
`-Xmx512m`, attach disabled, and the release-specific Java module options it
requires. `--java-home` selects that worker's compatible Java installation;
`SSTABLE_TOOLS_JAVA_OPTS` tunes only the small launcher JVM.

### Commands

| Command | Purpose |
|---|---|
| `cqlsh` | Query explicit SSTables, or inventory and publish verified mutations into `--output-dir`. |
| `runtime inspect` | Print resolved Cassandra paths, versions, classpath, and JAR hashes. |
| `runtime preflight` | Start a separate worker JVM and verify required Cassandra API linkage. |
| `workspace create <path>` | Inventory sources and create a validated managed workspace. |
| `workspace import <path>` | Validate and copy selected SSTables into the workspace. |
| `workspace start <path>` | Start the isolated Cassandra sandbox. |
| `workspace cqlsh <path>` | Launch the selected installation's stock `cqlsh`. |
| `workspace status <path>` | Verify sources and display persisted lifecycle state. |
| `workspace flush <path>` | Quiesce CQL and flush the workspace table. |
| `workspace export <path>` | Publish a verified `delta` or self-contained `snapshot`. |
| `workspace stop <path>` | Drain and stop the workspace sandbox. |
| `workspace recover <path>` | Recover a failed workspace to its last stable state. |
| `workspace destroy <path>` | Permanently delete an inactive workspace after UUID confirmation. |

## Runtime Preflight

Each thin JAR uses the Cassandra installation supplied by `--cassandra-home`.
It does not package Cassandra server classes or attach to the installed
Cassandra process. The installation's `cassandra.yaml` is neither required nor
read. The tool generates a private sandbox configuration beneath its workspace.
Optional distribution JVM options and client resources are discovered
automatically from the selected installation; `--java-home` selects the worker
JVM.

```shell
sstable-tools --cassandra-lib-dir /usr/share/cassandra/lib \
  --java-home /usr/lib/jvm/java-11-openjdk \
  runtime preflight
```

`runtime inspect` prints the discovered runtime paths, Cassandra version, child
classpath, and JAR identities. `runtime preflight` starts a separate worker JVM
and verifies the Cassandra APIs required by that adapter before opening an
SSTable.

Inspect runtime discovery without starting the worker:

```shell
sstable-tools --cassandra-lib-dir /opt/apache-cassandra-5.0.8/lib \
  --java-home /usr/lib/jvm/java-17-openjdk \
  runtime inspect
```

## Managed Workspace Example

The direct `cqlsh` command is preferred. For diagnostic or recovery workflows
that need explicit lifecycle control:

```shell
TOOL=sstable-tools
CASE=/var/tmp/sstable-tools-case
export CASSANDRA_LIB_DIR=/opt/apache-cassandra-5.0.8/lib

"$TOOL" \
  --sstables /archive/acme/users-7ad54392bcdd35a684174e047860b377/nb-42-big-Data.db \
  --schema /archive/acme-users.cql \
  --output-format big \
  workspace create "$CASE"

"$TOOL" workspace import "$CASE"

"$TOOL" \
  --timestamp-policy after-source \
  workspace start "$CASE"

"$TOOL" \
  workspace cqlsh "$CASE" \
  --execute "SELECT id, name FROM acme.users WHERE id = 1;"

"$TOOL" workspace flush "$CASE"
"$TOOL" workspace export "$CASE" \
  --mode delta \
  --output /archive/acme-users-delta
"$TOOL" workspace stop "$CASE"
```

Use `--mode snapshot` instead when the export must contain both the imported
base and the new SSTables.

## Compatibility

All release adapters currently require
`org.apache.cassandra.dht.Murmur3Partitioner`.

The CI matrix creates stopped SSTables with stock Cassandra `cqlsh`, then runs
the matching thin JAR and stock `cqlsh` direct workflow. It verifies `INSERT`,
`UPDATE`, `SELECT`, flush, sibling publication, direct reopen, and unchanged
source component hashes.

| Artifact | Tested Cassandra patch | Java runtime | Direct output |
|---|---:|---:|---|
| `cassandra-3.11` | 3.11.19 | 8 | Big `me` |
| `cassandra-4.0` | 4.0.17 | 8-11 | Big `nb` |
| `cassandra-4.1` | 4.1.3 | 11 | Big `nb` |
| `cassandra-5.0` | 5.0.4-5.0.8 | 17 | Big `oa`, BTI `da` |

The direct workflow intentionally has no `sstableloader`, streaming, clean-node
import, or broad filesystem discovery.

## Building

This is one Maven multi-module build. Compile and run the shared and
version-specific unit tests with:

```shell
mvn test
```

Build the four thin JARs and verify that they do not embed Cassandra runtime
classes:

```shell
mvn package
scripts/verify-thin-jars
```

Artifacts are written to `workers/cassandra-<line>/target/`.

## Release Process

The Git tag is the source of truth for a published version. A stable tag such
as `v1.2.3` builds JARs and packages with embedded version `1.2.3`; there is no
separate release-version file. Untagged development builds use the default
`0.1.0-SNAPSHOT` Maven revision.

### Build a release candidate locally

Release packaging requires Java 17, Go 1.26.4 or newer, nFPM, `dpkg-deb`,
`rpm`, `rpm2cpio`, and `cpio`. CI pins Go `1.26.4` and nFPM `v2.47.0`;
install the same nFPM version with:

```shell
go install github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.47.0
```

Build and verify a candidate with the exact version that will be tagged:

```shell
RELEASE_VERSION=0.1.0

mvn clean verify -Drevision="$RELEASE_VERSION"
scripts/package-release \
  --output target/release \
  --version "$RELEASE_VERSION"
scripts/package-linux-packages \
  --release-dir "target/release/sstable-tools-$RELEASE_VERSION"
scripts/verify-linux-packages \
  "target/release/sstable-tools-$RELEASE_VERSION"
scripts/verify-release-bundle \
  "target/release/sstable-tools-$RELEASE_VERSION"
```

This creates the auto-detecting `sstable-tools` launcher, four explicit
adapter launchers with adjacent raw JARs, an architecture-independent DEB,
and a noarch RPM under
`target/release/sstable-tools-$RELEASE_VERSION/`. It also creates the
executable-mode-preserving standalone archive
`target/release/sstable-tools-$RELEASE_VERSION.tar.gz`. Both Linux packages
install the following commands:

```text
sstable-tools
sstable-tools-cassandra-3.11
sstable-tools-cassandra-4.0
sstable-tools-cassandra-4.1
sstable-tools-cassandra-5.0
```

Use `--sign` on `scripts/package-linux-packages` to create a detached
`SHA256SUMS.asc` signature after the DEB and RPM have been included in
`SHA256SUMS`. For a packaging-only rebuild of the same upstream version, use
`--package-release 2` (then increment it for later rebuilds).

### Publish a tagged release

After the candidate is verified, the release commit is pushed, and CI is
passing, create and push an annotated stable-version tag:

```shell
RELEASE_VERSION=0.1.0
git tag -a "v$RELEASE_VERSION" -m "SSTable Tools $RELEASE_VERSION"
git push origin "v$RELEASE_VERSION"
```

The [Release workflow](.github/workflows/release.yml) validates the tag, builds
and tests every adapter with that exact Maven revision, rejects mismatched
embedded JAR versions, creates and extracts both Linux packages, verifies their
payloads and checksums, runs the release security gate, and publishes the
resulting directory as a workflow artifact and GitHub Release.

The security gate scans the built release with ClamAV, analyzes Java source with
CodeQL `security-extended`, checks source secrets and configuration with Trivy,
records the full Maven build/compatibility graph, and scans the published SPDX
SBOM for known dependency vulnerabilities. The full Maven graph is report-only
because it includes Cassandra compatibility and provided dependencies that are
not shipped. Malware, scanner errors, missing reports, or HIGH/CRITICAL CodeQL,
source-security, or published-SBOM findings block publication. The workflow
always retains diagnostic reports when the scan step runs; successful GitHub
Releases include
`sstable-tools-<version>-security-reports.tar.gz` with the Markdown summary,
tool metadata, ClamAV logs, CodeQL SARIF, Trivy JSON, and Maven dependency trees.

If the `RELEASE_GPG_PRIVATE_KEY` repository secret is configured, the workflow
imports it and signs the final checksum file. The workflow can also be started
manually with an explicit stable version for release testing or recovery.

See the [release guide](docs/releasing.md) for the complete artifact list and
package-revision details.

## Design Documents

- [Direct CQLSH design](docs/direct-cqlsh-design.md)
- [System keyspace direct-CQLSH design](docs/system-keyspace-direct-cqlsh-design.md)
- [Cassandra node deployment guide](docs/node-deployment.md)
- [Release guide](docs/releasing.md)
- [Real Cassandra CI coverage](docs/ci-real-cassandra-testing.md)
- [Thin JAR packaging dependencies](docs/packaging-dependencies.md)
- [Workspace manifest and lifecycle contract](docs/workspace-manifest.md)

The `workspace ...` commands remain an advanced diagnostic and recovery
interface. They are not the normal operator workflow.
