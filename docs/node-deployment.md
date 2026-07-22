# Cassandra Node Deployment

SSTable Tools is distributed as four thin JARs. Choose the JAR matching the
installed Cassandra release line and keep it outside the Cassandra installation
and every live data directory. The JAR loads Cassandra classes only in a private
worker process using the selected installed distribution.

| Cassandra | Java | Artifact |
|---|---:|---|
| 3.11.19 | 8 | `sstable-tools-cassandra-3.11-<version>.jar` |
| 4.0.17 | 8-11 | `sstable-tools-cassandra-4.0-<version>.jar` |
| 4.1.3 | 11 | `sstable-tools-cassandra-4.1-<version>.jar` |
| 5.0.4 | 17 | `sstable-tools-cassandra-5.0-<version>.jar` |

## Install And Verify

Download a release directory, verify its `SHA256SUMS`, and retain the
compatibility manifest and SPDX SBOM with the artifact.

```shell
sha256sum --check SHA256SUMS
java -jar sstable-tools-cassandra-5.0-<version>.jar \
  --cassandra-home /opt/apache-cassandra-5.0.4 \
  --java-home /usr/lib/jvm/java-17-openjdk \
  runtime preflight
```

For packaged layouts, point `--cassandra-home` to the Cassandra share directory
and provide `--cassandra-conf` when configuration discovery is ambiguous. The
tool supports tarball, Debian-style, and RPM-style layouts; it does not run a
package service script or modify Cassandra's installation.

## Operational Boundary

Stop the source Cassandra process before any import, mutation, compaction, or
export. Use a completed snapshot or backup copied outside every live Cassandra
data directory. Place the tool's private temporary directory and any advanced
workspace on local storage with capacity for staged component copies, commitlog
segments, and generated deltas.

The private worker binds only loopback endpoints. It does not join gossip or
start a source-node JMX service. The bundled workflow uses the matching
installation's stock `cqlsh` against that private endpoint.

For the normal direct workflow, give explicit files, a schema bundle, and an
optional private workspace parent:

```shell
java -jar sstable-tools-cassandra-5.0-<version>.jar \
  --cassandra-home /opt/apache-cassandra-5.0.4 \
  --tmp-dir /var/tmp/sstable-tools \
  --sstables /backup/ks/table-<id>/nb-1-big-Data.db \
  --schema /backup/table.cql \
  cqlsh
```

On a clean `cqlsh` exit, verified deltas are published beside the supplied
SSTables. A failed operation retains its private workspace for diagnosis and
publishes no delta. Do not replace or delete original source components during
tool upgrade or rollback; JAR replacement is independent of Cassandra.
