# Cassandra 5.0 BTI output design

## Scope

The Cassandra 5.0 worker must import either Big or BTI SSTables. A writable
workspace must also have one immutable output format: Big `oa` or BTI `da`.
This document describes the implemented output-selection contract.

## User contract

Add `--output-format big|bti` to `workspace create` and direct `cqlsh`.

- `bti` is accepted only by the Cassandra 5.0 adapter; `big` remains available
  to every supported adapter.
- `big` is the default for backward-compatible existing workflows.
- `bti` selects Cassandra's configured BTI writer and publishes `da-*-bti`
  component sets.
- It is not accepted by 3.11, 4.0, or 4.1 adapters.
- A later `workspace start`, `import`, `flush`, or `export` cannot override
  the recorded value.

The selected output format is independent of the input set. A 5.0 workspace
may import compatible Big and BTI sets only when Cassandra validates the
combination. Generated deltas always use the selected writer format.

## Persistent state

The value is stored as the immutable `sstable.format` output-identity entry
during creation before any source is staged. This preserves the v1 manifest
format and lets existing manifests resolve to `big` when first used.

The controller passes the recorded value, not a caller-supplied later value,
to every worker launch. Flush verification rejects a delta from another
format family.

## Worker configuration

`Cassandra311SandboxConfig` emits this 5.0-only YAML section:

```yaml
sstable:
  selected_format: bti
```

For Big it emits `selected_format: big`. Cassandra 5.0's `DatabaseDescriptor`
registers both formats, so selecting BTI does not prevent reading compatible
Big input. The runtime verifies `DatabaseDescriptor.getSelectedSSTableFormat()`
matches the manifest before importing or starting native transport.

The private Cassandra 5.0 sandbox also sets
`storage_compatibility_mode: NONE`. Cassandra 5.0 defaults to `CASSANDRA_4`,
which rejects the BTI writer. This setting applies only to the tool's isolated
workspace, never the stopped source node or its configuration.

Import remains format-preserving for staged source descriptors. Flush output
is validated against the selected format: Big must be `oa-big`; BTI must be
`da-bti`, with `Data.db`, `Partitions.db`, `Rows.db`, `Statistics.db`,
`Digest.crc32`, and `TOC.txt` present.

## Failure behavior

Invalid output formats, a request on a non-5.0 adapter, a mismatch between
the manifest and generated YAML, or a generated delta in another format fail
before publication. A Cassandra storage-compatibility mode that prevents the
selected writer is rejected during runtime preflight, before import changes
the workspace.

## Test plan

1. Unit-test CLI parsing, immutable output identity, and runtime configuration
   generation.
2. Run direct stock Cassandra 5.0 `cqlsh` with `--output-format bti` against
   a stopped Big fixture, then verify published `da-*-bti` output.
3. For each fixture, run direct stock `cqlsh` `SELECT`, timestamped `INSERT`,
   timestamped `UPDATE`, source-hash verification, delta publication, and a
   fresh reopen of source plus delta.
4. Exercise mixed Big/BTI input only after Cassandra's reader accepts it;
   otherwise assert the failure is actionable and source files remain intact.
5. Reject `nb`, future `ob`/`db`, missing BTI index components, and a
   manifest/YAML output-format mismatch before write publication.
