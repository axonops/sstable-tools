# Real Cassandra CI coverage

## Goal

CI must prove that SSTables come from the matching Cassandra release, not from
hand-written fixture files. A source Cassandra process is always stopped before
its SSTables are copied into a workspace or passed to SSTable Tools.

## Source lifecycle matrix

The `stopped-cqlsh-source` GitHub Actions matrix runs against Cassandra 3.11.19,
4.0.17, 4.1.3, and 5.0.4. Each matrix entry performs this sequence:

1. Start the matching official Cassandra Docker image.
2. Use that image's stock `cqlsh` to create `ci_source.events`.
3. Execute stock-CQL `INSERT`, `UPDATE`, and `SELECT`, asserting the changed
   values appear in `cqlsh` output.
4. Run `nodetool flush` for the table.
5. Stop the container and assert it is no longer running.
6. Copy only the completed table components from the stopped container.
7. Run the matching thin JAR's `workspace create` and `workspace status` against
   an explicitly selected `Data.db` component, asserting the recorded source
   integrity.
8. Record SHA-256 hashes for every copied component and recheck them after the
   workspace operation, including import where that adapter supports it.

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

The 4.0.17, 4.1.3, and 5.0.4 jobs execute the same stopped-source import
sequence and then start guarded isolated sandboxes. They invoke the installed
distribution's stock `cqlsh` for `INSERT`, `UPDATE`, and `SELECT`, flush,
export the delta, and stop. The sandbox is loopback-only, disables gossip and
JMX, and only permits writes to the imported workspace table with an explicit
timestamp greater than the source SSTable maximum.

The 4.0.17 and 4.1.3 jobs then run `scripts/ci-clean-node-import`. The script
combines the immutable stopped-source components with the exported delta,
bulk-loads them using the matching distribution's `sstableloader` into a fresh
matching Docker node, and verifies the source and sandbox rows with that
node's stock `cqlsh`. Post-export clean-node readback remains pending for 5.0.

The 4.0.17 and 4.1.3 jobs also run `scripts/ci-legacy-ma-cqlsh`. It copies the
immutable checked-in Cassandra 3.11 Big `ma` user fixture, reads it with the
target adapter, performs stock-`cqlsh` `INSERT` and `UPDATE` through the direct
interface, verifies the original component hashes, and reopens the combined
`ma` plus published `nb` delta set for a final `SELECT`. This separates tested
older-format migration behavior from the current 3.11.19 producer format
(`me`).
