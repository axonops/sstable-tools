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

The 4.0.17 and 4.1.3 jobs execute the same stopped-source import sequence and
then start guarded isolated sandboxes. They invoke the installed distribution's
stock `cqlsh` for `INSERT`, `UPDATE`, and `SELECT`, flush, export the delta, and
stop. The sandbox is loopback-only, disables gossip and JMX, and only permits
writes to the imported workspace table with an explicit timestamp greater than
the source SSTable maximum.

Cassandra 5.0.4 has a separate stopped-source import job. It proves stock
`cqlsh` source generation, source shutdown, selected-SSTable capture, schema
validation, offline import, and source-integrity recheck. Native CQL sandbox
support and post-export reimport/readback remain pending for that release.
