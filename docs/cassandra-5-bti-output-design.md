# Cassandra 5.0 SSTable output selection design

## Scope

The Cassandra 5.0 worker must import either Big or BTI SSTables. A writable
workspace must also have one immutable output format and storage compatibility
mode: Big `nb` in `CASSANDRA_4`, Big `oa` in `UPGRADING`/`NONE`, or BTI `da`
in `UPGRADING`/`NONE`. This document describes the implemented selection
contract.

## User contract

The selected Cassandra installation is authoritative. SSTable Tools reads
`storage_compatibility_mode` and `sstable.selected_format` from its
`cassandra.yaml` and does not expose a format-conversion workflow.

- `CASSANDRA_4` plus `big` publishes Big `nb`.
- `UPGRADING` or `NONE` plus `big` publishes Big `oa`.
- `UPGRADING` or `NONE` plus `bti` publishes BTI `da`.
- `CASSANDRA_4` plus `bti` is rejected as an invalid Cassandra configuration.
- `--output-format big|bti`, when supplied, is a consistency assertion. It
  cannot override `cassandra.yaml`, and a mismatch fails before import.
- A later `workspace start`, `import`, `flush`, or `export` cannot override the
  resolved value.

The input set may contain SSTables Cassandra can read, but generated deltas
always use the target installation's configured mode and writer format.

## Persistent state

For new workspaces the format remains unresolved during Cassandra-free
creation. At import, the selected Cassandra 5.0 runtime resolves and records
the immutable `sstable.format` and
`cassandra.storage-compatibility-mode` output-identity entries.
An explicit `--output-format` is recorded earlier only as an assertion and
must match the resolved installation setting.
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

The private Cassandra 5.0 sandbox sets both `storage_compatibility_mode` and
`sstable.selected_format` to the resolved target values. Resolution checks
`CASSANDRA_CONF`, the selected runtime's `conf/cassandra.yaml`, and
`/etc/cassandra/cassandra.yaml` for a packaged `/usr/share/cassandra` runtime.
An omitted mode means `CASSANDRA_4`; an omitted format means `big`. When no
configuration is available, the inventory supplies a conservative fallback;
mixed Big/BTI input is rejected as ambiguous. The worker still runs from a
generated, loopback-only configuration inside its private workspace.

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

Invalid output formats, an explicit `--output-format` that disagrees with
`cassandra.yaml`, a mismatch between the manifest and generated YAML, or a
generated delta in another format fail before publication. A Cassandra
storage-compatibility mode that prevents the selected writer is rejected
during runtime preflight, before import changes the workspace. Cassandra 5.0
vector types are rejected explicitly because they do not yet have
direct-cqlsh round-trip coverage.

## Test plan

1. Unit-test CLI parsing, immutable output identity, and runtime configuration
   generation.
2. Run direct stock Cassandra 5.0 `cqlsh` against fixtures created with the
   same target YAML settings and verify that input and output remain `nb/big`,
   `oa/big`, or `da/bti` as configured.
3. For each fixture, run direct stock `cqlsh` `SELECT`, timestamped `INSERT`,
   timestamped `UPDATE`, source-hash verification, delta publication, and a
   fresh reopen of source plus delta.
4. Run the real multi-directory `system.local` mutation and restart scenario
   for `CASSANDRA_4/big`, `UPGRADING/big`, `NONE/big`, and `NONE/bti`.
5. Reject BTI in `CASSANDRA_4`, future `ob`/`db`, missing BTI index components,
   and an explicit format/YAML mismatch before write publication.
