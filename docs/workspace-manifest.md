# Workspace manifest and lifecycle contract

## Purpose

A mutation workspace is an isolated, single-controller directory used to import
immutable SSTable inputs, run a version-matched Cassandra worker, and export new
SSTables. The source component files are evidence: the controller never writes
to them and verifies their recorded identities before every implemented
workspace operation.

This document defines the shared, Cassandra-free lifecycle contract. Release
workers may add validation and files inside the owned directories, but they must
not bypass the manifest repository or invent release-specific state files that
replace this contract.

## Current command surface

The shared bootstrap currently implements:

```shell
java -jar sstable-tools-cassandra-3.11-<version>.jar \
  workspace create ./case \
  --sstables /evidence/snapshots/before-change \
  --schema /evidence/schema.cql

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  workspace import ./case

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  --cassandra-home /opt/apache-cassandra-3.11.19 \
  workspace start ./case --timestamp-policy after-source

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  workspace status ./case

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  workspace flush ./case

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  workspace stop ./case

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  workspace recover ./case

java -jar sstable-tools-cassandra-3.11-<version>.jar \
  workspace destroy ./case \
  --confirm-workspace-id UUID_FROM_workspace.status
```

`--sstables` is repeatable. Every value is a directory containing complete
TOC-declared component sets. `workspace create` records the inventory and moves
the manifest from `NEW` to `VALIDATED`. Repeating create with the same canonical
directories and byte identities is idempotent. Repeating it with different
inputs fails.

`workspace import` is currently implemented by the Cassandra 3.11 adapter. It
parses the schema with installed Cassandra classes, validates every source set,
copies and loads it with native transport disabled, and records the table and
baseline identities before committing `IMPORTED`. `workspace status`, `start`,
`flush`, `stop`, and `recover` reverify source, schema, and imported baseline
hashes.
New delta files are allowed; changing or removing a baseline file fails closed.

`workspace destroy` is deliberately different from data operations. It does
not touch or require readable source files and never deletes recorded export
destinations. It requires the exact manifest UUID and accepts only `NEW`,
`VALIDATED`, `IMPORTED`, or `STOPPED`. Under the exclusive lock it rejects an
unexpected root entry and uses a non-following walk confined to the canonical
workspace. A recorded worker must be proven stopped before removal begins.

The bootstrap does not discover or load Cassandra for create, status, flush,
stop, or recover. Import and start launch a release-specific child JVM against
the selected installation. Cassandra 3.11 start and live status output include
the loopback native endpoint, fixed username, and owner-only `state/cqlshrc`
path needed by the installation's stock cqlsh. `workspace cqlsh` validates the
recorded runtime and live endpoint, then launches that stock client with the
fixed configuration; it does not load Cassandra classes into the controller.

## Owned layout

Creation establishes this private layout with owner-only permissions where the
filesystem supports POSIX permissions:

```text
workspace/
  manifest.json
  .workspace.lock
  schema/
  runtime/
  data/
  commitlog/
  logs/
  staging/
  exports/
  state/
```

All controller-resolved paths must remain beneath the canonical workspace root.
Absolute paths, `.` or `..` segments, and existing symlink crossings are
rejected. Source and schema paths are the only paths outside the workspace;
they are recorded as canonical absolute paths and are read-only inputs. A
source directory may not contain, equal, or be contained by the workspace root,
and the schema bundle may not be stored below the workspace.

## Persistence protocol

Only a controller holding the OS file lock on `.workspace.lock` may initialize
or replace `manifest.json`. The text inside the lock file is diagnostic and is
not proof of ownership. A stale text record does not block recovery after the OS
releases the lock.

A manifest update uses this sequence:

1. encode and validate the complete successor manifest in memory;
2. create a unique temporary file in the workspace root;
3. write all bytes and `fsync` the temporary file;
4. atomically replace `manifest.json` on the same filesystem;
5. `fsync` the workspace directory;
6. discard any uncommitted temporary file.

The controller fails when the filesystem cannot provide atomic replacement. It
does not fall back to a non-atomic move. A temporary file left by process death
is never interpreted as committed state.

The repository rejects a manifest larger than 16 MiB, a changed workspace UUID,
a timestamp moving backwards, and a successor state that is not allowed by the
state machine.

## Manifest format version 1

The root JSON object contains exactly these fields. Unknown or missing fields
are rejected so that a newer writer is never partially interpreted by an older
controller.

| Field | Type | Meaning |
|---|---|---|
| `formatVersion` | integer | Schema version, currently `1` |
| `workspaceId` | UUID string | Immutable identity for this workspace |
| `state` | enum string | Current persisted lifecycle state |
| `lastStableState` | enum string or null | Recovery target for a failed state |
| `failureMessage` | string or null | Operator-facing failure context |
| `createdAt` | ISO-8601 instant | Initial manifest creation time |
| `updatedAt` | ISO-8601 instant | Last committed transition time |
| `sourceInventory` | object | Canonical source descriptors and components |
| `schemaIdentity` | string map | Bundle hash/source, imported table identity, and source timestamp bound |
| `runtimeIdentity` | string map | Tool, Java, and Cassandra installation identity |
| `outputIdentity` | string map | Sandbox/import contract and network identity |
| `baselineInventory` | file array | Checksummed imported descriptor baseline |
| `exports` | export array | Successfully verified export history |

Each `sourceInventory.sets` item records:

| Field | Meaning |
|---|---|
| `descriptor` | Descriptor prefix, for example `mc-1-big` |
| `formatVersion` | SSTable version token, for example `mc` |
| `format` | Format token, for example `big` or `bti` |
| `directory` | Canonical absolute source directory |
| `components` | Sorted TOC-declared component identities |

Each component and workspace-owned file identity contains a path or component
name, its non-negative byte size, and a lowercase SHA-256 digest. A component
path must be the direct child `<descriptor>-<name>` of its recorded directory.
Duplicate descriptors, components, export UUIDs, and file paths are rejected.

## Source inventory rules

Inventory accepts complete descriptor sets, not standalone `Data.db` files.
Every descriptor must have a `TOC.txt`, and the TOC must declare at least
`TOC.txt`, `Data.db`, and `Statistics.db`. Every declared component must be a
regular file. Temporary components, symlinked components, missing files,
duplicate TOC entries, and unsafe component names fail creation.

The inventory is sorted deterministically and stores file size plus SHA-256.
Verification checks type, size, and digest. A mismatch stops create, status, and
recover with workspace exit code `6`. Cassandra snapshot or backup directories
are the normal source. Before create, import, start, cqlsh, flush, and export on
Linux, the controller scans visible `/proc` command lines, file descriptors, and
memory maps. It rejects a source below an active Cassandra daemon's reported
`cassandra.storagedir` or containing a file that daemon has open or mapped.
Create performs the check before creating workspace artifacts. This detection
cannot classify processes hidden by `/proc` permissions and is unavailable on
non-Linux systems, so the required input remains a completed copy outside all
live Cassandra data directories.

The release worker reads the maximum timestamp from each validated statistics
component during import. Worker protocol v3 returns the overall maximum, which
is persisted under the `schemaIdentity` map key
`source.max-timestamp-micros`. Import, start, and status compare it with the
current controller clock and warn while the source remains in the future.

The first start records `wall-clock` or `after-source` under
`schemaIdentity["timestamp.policy"]`; later starts reuse that choice when the
option is omitted and reject a conflicting explicit option. `wall-clock` keeps
Cassandra's normal assignment. `after-source` initializes the owner-only
`state/timestamp.properties` high-water above the imported maximum. Before a
timestamp-free mutation executes, the release worker durably advances it to
`max(previous + 1, wall clock)` using write, file `fsync`, atomic replacement,
and directory `fsync`. The file is retained across graceful stop and proven-dead
recovery. Import removes stale timestamp state because a new source maximum
must establish the initial bound.

An explicit CQL `USING TIMESTAMP` or native-protocol timestamp bypasses the
allocator and is preserved exactly. In particular, stock Cassandra 3.11 cqlsh
normally sends a protocol timestamp. `workspace status` reports the policy and
the current durable high-water when present.

The Cassandra 3.11 live profile exercises this manifest contract with a real
latest-format source cell dated one year in the future. It proves an ordinary
stock-cqlsh update loses under `wall-clock`, while a timestamp-free prepared
update under `after-source` wins with a write time above the persisted source
maximum.

`workspace flush` is terminal for the current native session. Worker protocol
v3 closes native transport, waits for all requests already admitted by the
query guard, verifies auto-compaction is disabled, and performs a blocking
flush of only the imported table. The worker then atomically writes the strict,
owner-only `state/flush-result.json` record before publishing endpoint state
`FLUSHED`. The record contains workspace/release/table identity, flush time, and
the complete sorted table inventory with size and SHA-256 for every component.
The controller requires the imported baseline to remain an exact subset and
reports the remaining files as the delta. It removes `state/cqlshrc` after a
successful or reconciled flush.

`workspace export <path> --mode delta|snapshot --output <destination>` first
uses authenticated worker protocol `VERIFY` while the worker remains
`FLUSHED`. The Cassandra 3.11 worker requires its live descriptor set to equal
the committed flush, runs extended verification, checks generated SSTables are
latest-format and unrepaired, performs a logical row-count scan, and atomically
writes owner-only `state/verification-result.properties`. The evidence is bound
to the SHA-256 of the complete flush result.

The controller accepts only complete TOC-declared descriptor component sets.
It publishes the inventoried `.sstable-tools-export` ownership marker,
`export-manifest.json`, `schema.cql`, and `sstables/` through a mode-0700
temporary sibling, fsyncs files and directories, and requires an atomic
non-replacing directory rename. `delta` contains only descriptors added after
the immutable baseline and records every required source component; `snapshot`
contains the complete flushed inventory. A deterministic export ID and
byte-stable manifest allow a retry to adopt an already-published directory only
when its exact path, size, and SHA-256 inventory matches. The workspace manifest
records that inventory and canonical output path before entering `EXPORTED`;
publication never writes into or overlaps the workspace or source directories.

## Lifecycle and recovery

The legal forward transitions are:

```text
NEW -> VALIDATED -> IMPORTED -> RUNNING -> FLUSHED -> EXPORTED -> STOPPED

any non-failed state -> FAILED_RECOVERABLE -> reconciled safe state
```

Additional operational transitions allow `IMPORTED`, `FLUSHED`, `EXPORTED`, or
`STOPPED` to start or resume `RUNNING`, and allow active/imported states to move
to `STOPPED`. Any non-failed state can enter `FAILED_RECOVERABLE` with a message.
The failed manifest retains its exact prior state in `lastStableState`.

| Persisted state | Recovery action |
|---|---|
| `NEW` | Retry create, or destroy the incomplete workspace |
| `VALIDATED` | Resume source validation and import, or destroy the inactive workspace |
| `IMPORTED` | Start the version-matched worker, or destroy the inactive workspace |
| `RUNNING` | Check worker health; recover only when its recorded process is stale |
| `FLUSHED` | Export the flushed delta, restart, or stop |
| `EXPORTED` | Verify the export, then restart or stop |
| `STOPPED` | Restart or destroy the workspace |
| `FAILED_RECOVERABLE` | Inspect the failure, then recover to `lastStableState` |

Recovery of `NEW`, `VALIDATED`, or `IMPORTED` restores the prior state only
after source, schema, and any imported baseline validate; none of those states
has a persistent sandbox worker. The Cassandra 3.11 controller reconciles a
failed `RUNNING`, `FLUSHED`, or `EXPORTED` state against its authenticated
control endpoint and recorded PID. A responsive worker restores its prior
state. An unreachable worker recovers as `STOPPED` only when Linux `/proc`
proves the exact workspace worker command is gone; missing or ambiguous process
identity fails closed. A failed `STOPPED` state requires a valid `STOPPED`
endpoint and the same process-death proof before recovery. A worker exit code
alone is never sufficient evidence that a transition completed.

If the worker and flush record are already `FLUSHED` while the manifest still
says `RUNNING`, `workspace status`, `flush`, or `recover` verifies the complete
inventory and finishes the manifest transition. This is the controller-crash
boundary after the worker committed the result. A missing, changed, mismatched,
or unsafe flush record fails closed. Starting a stopped workspace deletes the
old record before reopening native transport; no old flush inventory is reused
for a later mutation session.

If publication completes but the controller exits before recording it, the
manifest remains recoverable from `FLUSHED`; rerunning the same export command
recomputes its deterministic identity and validates the existing directory.
If the controller exits earlier, retry removes the deterministic staging tree
only when its marker has the exact expected workspace, flush, mode, and export
identity; all entries are owner-only, non-symlink, and on the expected path
allowlist. Otherwise recovery fails closed without deleting the tree.
An `EXPORTED` workspace and a failed workspace whose last stable state was
`EXPORTED` must validate the flush-bound verification evidence and every
recorded export hash before status or recovery succeeds.

Destroy is not a lifecycle transition because it removes the manifest itself.
It refuses `RUNNING`, `FLUSHED`, `EXPORTED`, and `FAILED_RECOVERABLE`; those
states must first complete `stop` or `recover`. Repeating the manifest UUID on
the command line prevents a mistaken path from authorizing deletion. The lock
file is removed last, after all other confined entries, and the parent
directory is fsynced.

Each Cassandra 3.11 start replaces `state/cqlshrc` with a new random 256-bit
password for the fixed `sstable_workspace` identity. Graceful stop and
proven-dead worker recovery delete that file. Recovery retains it only when the
authenticated control channel proves the original worker remains live.
Timestamp high-water state is separate from this ephemeral credential and is
retained so allocated values cannot be reused after restart.

## Failure and security properties

- A source identity mismatch blocks further mutation work.
- The workspace lock prevents concurrent controllers in one or multiple JVMs.
- Workspace-owned paths cannot traverse outside the canonical root.
- The manifest parser is strict about types, timestamps, UUIDs, hashes, paths,
  fields, and supported format versions.
- A crash before atomic replacement preserves the previous complete manifest.
- A crash after replacement exposes the complete successor manifest.
- The source inventory never authorizes writes to the source paths.
- Native CQL requires a per-start credential and a loopback client; production
  Cassandra RBAC is neither imported nor consulted.

This is a local integrity and crash-consistency boundary. It is not a signature
scheme and does not defend against an attacker who can replace both source data
and the workspace manifest. Signed provenance is a separate hardening phase.
