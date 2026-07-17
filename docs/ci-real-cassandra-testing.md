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
   that copied component directory, asserting the recorded source integrity.

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

This is intentionally not enabled for 4.0, 4.1, or 5.0 yet. Their adapters
currently expose version detection and linkage verification only; they do not
implement `ImportRuntimeAdapter` or `SandboxRuntimeAdapter`. Enabling a write
workflow test for them before that implementation exists would make CI report a
capability that the released JARs do not have.

When each adapter gains import and sandbox support, its matrix entry must be
promoted to the same stopped-source acceptance sequence, including a post-export
reimport and stock-cqlsh readback.
