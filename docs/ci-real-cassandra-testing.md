# Real Cassandra CI coverage

## Goal

CI must prove that SSTables come from the matching Cassandra release, not from
hand-written fixture files. A source Cassandra process is always stopped before
its SSTables are copied into a workspace or passed to SSTable Tools.

## Source lifecycle matrix

The `stopped-cqlsh-source` GitHub Actions matrix runs against Cassandra 3.11.19,
4.0.18, 4.1.11, and 5.0.8. Each matrix entry performs this sequence:

1. Start the matching official Cassandra Docker image.
2. Use that image's stock `cqlsh` to create a UDT and `ci_source.events` with
   scalar, set, map, tuple, and frozen-UDT columns.
3. Execute stock-CQL `INSERT`, `UPDATE`, `DELETE`, TTL, and `SELECT`
   operations. The assertions cover live cells, a deleted cell, an expiring
   row, collections, a tuple, and a UDT.
4. Run `nodetool flush` for the table.
5. Stop the container and assert it is no longer running.
6. Copy only the completed table components from the stopped container.
7. Run the matching thin JAR's `workspace create` and `workspace status` against
   an explicitly selected `Data.db` component, asserting the recorded source
   integrity.
8. Record SHA-256 hashes for every copied component and recheck them after the
   workspace operation, including import where that adapter supports it.

Each successful source job also publishes a `fixture-provenance.json` artifact.
It records the exact producer patch and image, partitioner, detected SSTable
descriptor format, compression selection, schema bundle, source CQL mutation
sequence, expected rows, and SHA-256 for every copied component. The
compatibility-report job downloads
all four artifacts and refuses to publish its matrix unless it has one complete
record for each supported release line.

The shell implementation is `scripts/ci-real-cassandra-source`. It preserves
the source components and logs under the job temporary directory and uploads
them when a matrix entry fails.

## Write-workflow acceptance

The Cassandra 3.11 Failsafe integration test adds a stronger end-to-end case:
it starts a temporary 3.11 node, uses the distribution's `cqlsh` to create,
insert, update, select, and flush a source table, then stops that source node.
Only after shutdown does it copy the table into a workspace. SSTable Tools then
executes `create`, `import`, `start`, stock-cqlsh `SELECT`/`INSERT`/`UPDATE`,
`flush`, delta `export`, and `stop`.

The 4.0.18, 4.1.11, 5.0.4, and 5.0.8 jobs execute the same stopped-source import
sequence and then start guarded isolated sandboxes. They invoke the installed
distribution's stock `cqlsh` for `INSERT`, `UPDATE`, and `SELECT`, flush,
export the delta, and stop. The sandbox is loopback-only, disables gossip and
JMX, and only permits writes to the imported workspace table with an explicit
timestamp greater than the source SSTable maximum.

The 4.0.18 and 4.1.11 jobs also run `scripts/ci-legacy-ma-cqlsh`. It copies the
immutable checked-in Cassandra 3.11 Big `ma` user fixture, reads it with the
target adapter, performs stock-`cqlsh` `INSERT` and `UPDATE` through the direct
interface, verifies the original component hashes, and reopens the combined
`ma` plus published `nb` delta set for a final `SELECT`. This separates tested
older-format migration behavior from the current 3.11.19 producer format
(`me`).

The Cassandra 5.0 jobs configure the source node and selected installation
identically; it does not exercise format conversion. The generic direct
workflow runs as `CASSANDRA_4` plus Big and verifies `nb` input and `nb`
output. Cassandra 5.0.8 additionally imports an uncompressed Big table.

The same job runs the real two-`data_file_directories` `system.local` scenario
five times: `CASSANDRA_4/big` produces `nb/big`, `UPGRADING/big` and
`NONE/big` produce `oa/big`, and `UPGRADING/bti` and `NONE/bti` produce
`da/bti`. Each run places source and generated SSTables in two actual Cassandra
data roots, restarts the stock 5.0.8 node with the same YAML settings, and
verifies the combined row through stock `cqlsh`. The BTI path also removes
`Partitions.db` and `Rows.db` in separate negative cases, and repeats the
multi-directory read-only import three times to exercise the ordering barrier.

Real SSTable producer/import coverage includes 3.11.0 and 3.11.19, 4.0.0 and
4.0.18, 4.1.0 and 4.1.11, and 5.0.4 and 5.0.8. The 4.0.0 and 4.1.0 jobs use
downloaded Apache distributions, run runtime preflight, create a real table
with the matching official image, and import its stopped SSTable. Cassandra
3.11.0 is an older-format producer imported by the supported 3.11.19 runtime;
5.0.4 is both the minimum supported runtime and a real producer.

A separate `patch-line-linkage` matrix resolves Cassandra's real Maven runtime
for every declared patch from 4.0.0 through 4.0.18, 4.1.0 through 4.1.11, and
5.0.4 through 5.0.8, then launches the thin adapter and requires an exact
runtime-preflight version response. Cassandra 4.0 is checked on both Java 8
and Java 11. This linkage matrix complements, rather than replaces, the
full-node endpoint scenarios.

The release-package job installs the generated dependency-free DEB and RPM in
clean Java 17 containers and invokes the installed universal launcher. Release
publication reuses a successful full CI run for the exact commit when one
exists; otherwise it dispatches `ci.yml` for the release ref and waits for it
to pass before building or publishing artifacts.
