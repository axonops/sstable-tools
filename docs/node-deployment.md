# Cassandra Node Deployment

SSTable Tools is distributed with one auto-detecting launcher, four explicit
adapter launchers with adjacent thin JARs, and a single DEB or RPM containing
all four adapters. Point `sstable-tools` at the installed Cassandra `lib`
directory. It reads the version from `apache-cassandra-<version>.jar` or
`cassandra-all-<version>.jar` and selects the matching adapter. Cassandra
classes are loaded only in a private worker process using that installation.

| Cassandra | Java | Artifact |
|---|---:|---|
| 3.11.19 | 8 | `sstable-tools-cassandra-3.11-<version>.jar` |
| 4.0.17 | 8-11 | `sstable-tools-cassandra-4.0-<version>.jar` |
| 4.1.3 | 11 | `sstable-tools-cassandra-4.1-<version>.jar` |
| 5.0.4 | 17 | `sstable-tools-cassandra-5.0-<version>.jar` |

## Install And Verify

Download a release directory, verify its `SHA256SUMS`, and retain the
compatibility manifest and SPDX SBOM with the artifact.

Official tagged releases also contain `SHA256SUMS.asc` when the release signing
key is configured. Verify that signature against the published project signing
key before trusting the checksums.

```shell
tar -xzf sstable-tools-1.2.3.tar.gz
cd sstable-tools-1.2.3
sha256sum --check SHA256SUMS
./sstable-tools \
  --cassandra-lib-dir /opt/apache-cassandra-5.0.4/lib \
  --java-home /usr/lib/jvm/java-17-openjdk \
  runtime preflight
```

Alternatively, install the package for the host distribution:

```shell
sudo apt install ./sstable-tools_1.2.3-1_all.deb
# or
sudo dnf install ./sstable-tools-1.2.3-1.noarch.rpm

CASSANDRA_LIB_DIR=/opt/apache-cassandra-5.0.4/lib sstable-tools --version
```

The package installs the universal and explicit adapter launchers in `/usr/bin`,
adapter JARs in
`/usr/share/sstable-tools`, and release metadata in
`/usr/share/doc/sstable-tools`. It requires a Java 17-or-newer launcher runtime.
For Cassandra 3.11, 4.0, or 4.1, continue to pass `--java-home` when the
Cassandra worker requires an older matching JVM. `SSTABLE_TOOLS_JAVA` can
override the Java executable used by the package launcher itself.

Set `CASSANDRA_LIB_DIR` once for repeated invocations, or pass
`--cassandra-lib-dir` immediately after `sstable-tools`. The launcher derives
the Cassandra home from the directory's parent. You can instead pass
`--cassandra-home`; in that case the launcher scans its `lib` child. Provide
no Cassandra configuration argument: optional JVM module options and client
resources are discovered from standard tarball, Debian, or RPM installation
layouts. The installation's `cassandra.yaml` is neither required nor read; the
tool generates a private sandbox configuration. It does not run a package
service script or modify Cassandra's installation.

## Operational Boundary

Stop the source Cassandra process before any import, mutation, compaction, or
export. Use a completed snapshot or backup copied outside every live Cassandra
data directory. Place the tool's private temporary directory and any advanced
workspace on local storage with capacity for staged component copies, commitlog
segments, and generated deltas.

The private worker binds only loopback endpoints. It does not join gossip or
start a source-node JMX service. The bundled workflow uses the matching
installation's stock `cqlsh` against that private endpoint.

For the normal direct workflow, give explicit files or an `--output-dir`, a
schema bundle, and an optional private workspace parent:

```shell
sstable-tools \
  --cassandra-lib-dir /opt/apache-cassandra-5.0.4/lib \
  --tmp-dir /var/tmp/sstable-tools \
  --sstables /backup/ks/table-<id>/nb-1-big-Data.db \
  --schema /backup/table.cql \
  cqlsh
```

On a clean `cqlsh` exit, verified deltas are published beside the supplied
SSTables. A failed operation retains its private workspace for diagnosis and
publishes no delta. Do not replace or delete original source components during
tool upgrade or rollback; JAR replacement is independent of Cassandra.

For a first `INSERT` with no existing SSTables, create the table directory and
use `--output-dir <directory>` instead of `--sstables`. If the directory later
contains SSTables, the tool inventories all of them, derives numeric or
UUID-style naming, and publishes the next collision-free delta there.
