# Cassandra 5.0 BTI output design

## Scope

The Cassandra 5.0 worker must import either Big or BTI SSTables. A writable
workspace must also have one immutable output format and storage compatibility
mode: Big `nb` in `CASSANDRA_4`, Big `oa` in `UPGRADING`/`NONE`, or BTI `da`
in `UPGRADING`/`NONE`. This document describes the implemented selection
contract.

## User contract

Add `--output-format big|bti` to `workspace create` and direct `cqlsh`.

- `bti` is accepted only by the Cassandra 5.0 adapter; `big` remains available
  to every supported adapter.
- `big` is the default for backward-compatible existing workflows.
- `bti` selects Cassandra's configured BTI writer and publishes `da-*-bti`
  component sets.
- `bti` is rejected when the resolved storage compatibility mode is
  `CASSANDRA_4`.
- It is not accepted by 3.11, 4.0, or 4.1 adapters.
- A later `workspace start`, `import`, `flush`, or `export` cannot override
  the recorded value.

The selected output format is independent of the input set. A 5.0 workspace
may import compatible Big and BTI sets only when Cassandra validates the
combination. Generated deltas always use the selected writer format.

## Persistent state

The format is stored as the immutable `sstable.format` output-identity entry
during creation before any source is staged. At import, the selected Cassandra
5.0 runtime resolves and records `cassandra.storage-compatibility-mode`.
Existing imported manifests without that entry retain the old `NONE` behavior.

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

The private Cassandra 5.0 sandbox sets `storage_compatibility_mode` to the
resolved target value. Resolution checks `CASSANDRA_CONF`, the selected
runtime's `conf/cassandra.yaml`, and `/etc/cassandra/cassandra.yaml` for a
packaged `/usr/share/cassandra` runtime. An omitted YAML setting means
`CASSANDRA_4`, matching Cassandra 5.0's default. When no configuration is
available, `oa`/`da` input selects native mode; `nb`, older input, or an empty
inventory selects the safe `CASSANDRA_4` default. The target configuration is
read only for this setting; the worker still runs from a generated,
loopback-only configuration inside its private workspace.

The same generated sandbox configuration infers
`uuid_sstable_identifiers_enabled` from the explicitly selected SSTable
descriptors. All-numeric input keeps numeric output. Any selected
28-character Cassandra UUID/ULID-style identifier, including a mixed
numeric/UUID selection, enables UUID-style output.

Import remains format-preserving for staged source descriptors. Flush output
is validated against the selected format and mode: Big must be `nb-big` in
`CASSANDRA_4` or `oa-big` in `UPGRADING`/`NONE`; BTI must be `da-bti`, with
`Data.db`, `Partitions.db`, `Rows.db`, `Statistics.db`, `Digest.crc32`, and
`TOC.txt` present.

## Failure behavior

Invalid output formats, a request on a non-5.0 adapter, a mismatch between
the manifest and generated YAML, or a generated delta in another format fail
before publication. A Cassandra storage-compatibility mode that prevents the
selected writer is rejected during runtime preflight, before import changes
the workspace. Cassandra 5.0 vector types are rejected explicitly because
they do not yet have direct-cqlsh round-trip coverage.

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
5. Run the real multi-directory `system.local` mutation and restart scenario
   in both `CASSANDRA_4` (`nb`) and `NONE` (`oa`) modes.
6. Reject BTI in `CASSANDRA_4`, future `ob`/`db`, missing BTI index components,
   and a manifest/YAML output-format mismatch before write publication.
