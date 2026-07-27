# SSTable Tools

[![CI](https://github.com/axonops/sstable-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/axonops/sstable-tools/actions/workflows/ci.yml)

SSTable Tools queries and mutates explicitly selected, stopped Cassandra
SSTables with the matching installed Cassandra release and its stock `cqlsh`.
It is under active development and is not yet an operator-ready production tool.

## Direct CQLSH

The primary interface is one command. Supply one or more comma-separated
`Data.db` or `TOC.txt` paths from one table directory, a schema bundle, and the
matching Cassandra installation. The tool opens stock `cqlsh`; on a clean exit
after a mutation it writes verified new SSTable component sets beside the
supplied source files.

```shell
java -jar workers/cassandra-5.0/target/sstable-tools-cassandra-5.0-0.1.0-SNAPSHOT.jar \
  --cassandra-home /opt/apache-cassandra-5.0.8 \
  --sstables /archive/acme/users-<table-id>/nb-42-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh
```

For multiple SSTables, use one comma-separated value or repeat `--sstables`:

```shell
--sstables /archive/acme/users-<table-id>/nb-42-big-Data.db,/archive/acme/users-<table-id>/nb-43-big-Data.db
```

For automation, use `--execute`. `--tmp-dir` changes the parent directory for
the private temporary workspace; the default is `/tmp/sstable-tools/`, and a
successful invocation removes its private child directory.

```shell
java -jar workers/cassandra-4.1/target/sstable-tools-cassandra-4.1-0.1.0-SNAPSHOT.jar \
  --tmp-dir /var/tmp/sstable-tools \
  --cassandra-home /opt/apache-cassandra-4.1.3 \
  --sstables /archive/acme/users-<table-id>/na-17-big-Data.db \
  --schema /archive/acme-users.cql \
  cqlsh --execute "INSERT INTO acme.users (id, name) VALUES (1, 'Ada');"
```

The direct command accepts `SELECT` plus non-conditional `INSERT` and `UPDATE`
for the selected table. It does not scan data roots, keyspaces, or unrelated
tables. All selected SSTables must be in one table directory.

Original components are never modified. Cassandra 3.11, 4.0, and 4.1 publish
the next numeric generation. Cassandra 5.0 uses the sequence or UUID SSTable
identifier mode configured by its `cassandra.yaml`.

After a successful direct mutation, later commands for that table should select
both the original files and the published sibling SSTables. This is the default
when `--output-version` is omitted. Supplying `--output-version` deliberately
restricts a command to that version and format family, which is useful for
Cassandra 5.0 Big-to-BTI transitions.

On Cassandra 5.0, `--output-format bti` selects BTI `da` delta output. The
default is Big `oa`; BTI is rejected by the 3.11, 4.0, and 4.1 JARs.

> **DANGER:** Do not mutate, compact, import, or export files belonging to a
> running Cassandra process. The source must be an external completed copy,
> snapshot, or backup. The isolated worker is expected; it uses only private
> storage and loopback networking.

## Runtime Preflight

Each thin JAR uses the Cassandra installation supplied by `--cassandra-home`.
It does not package Cassandra server classes or attach to the installed
Cassandra process. `--cassandra-conf` and `--java-home` are available when the
installation layout cannot be inferred.

```shell
java -jar workers/cassandra-4.1/target/sstable-tools-cassandra-4.1-*.jar \
  --cassandra-home /usr/share/cassandra \
  --cassandra-conf /etc/cassandra/default.conf \
  --java-home /usr/lib/jvm/java-11-openjdk \
  runtime preflight
```

`runtime inspect` prints the discovered runtime paths, Cassandra version, child
classpath, and JAR identities. `runtime preflight` starts a separate worker JVM
and verifies the Cassandra APIs required by that adapter before opening an
SSTable.

## Compatibility

The CI matrix creates stopped SSTables with stock Cassandra `cqlsh`, then runs
the matching thin JAR and stock `cqlsh` direct workflow. It verifies `INSERT`,
`UPDATE`, `SELECT`, flush, sibling publication, direct reopen, and unchanged
source component hashes.

| Artifact | Tested Cassandra patch | Java runtime | Direct output |
|---|---:|---:|---|
| `cassandra-3.11` | 3.11.19 | 8 | Big `me` |
| `cassandra-4.0` | 4.0.17 | 8-11 | Big `nb` |
| `cassandra-4.1` | 4.1.3 | 11 | Big `nb` |
| `cassandra-5.0` | 5.0.4 | 17 | Big `oa`, BTI `da` |

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

## Design Documents

- [Direct CQLSH design](docs/direct-cqlsh-design.md)
- [Cassandra node deployment guide](docs/node-deployment.md)
- [Real Cassandra CI coverage](docs/ci-real-cassandra-testing.md)
- [Thin JAR packaging dependencies](docs/packaging-dependencies.md)
- [Workspace manifest and lifecycle contract](docs/workspace-manifest.md)

The `workspace ...` commands remain an advanced diagnostic and recovery
interface. They are not the normal operator workflow.
