## Pezhai Engine Architecture

This document is the architecture source of truth for the `Pezhai` storage
engine library in `crates/pezhai`.

### Purpose

`Pezhai` owns the local storage semantics for `kalanjiyam`.
It exposes a direct engine API for in-process callers and a separate server
runtime that will later embed the same engine core.

### Module layout

The engine crate is split into the following modules:

- `iyakkam.rs`: public engine shell and shared operation-shape helpers
- `nilaimai.rs`: mutable engine state, sequence numbers, and snapshot tracking
- `pathivu.rs`: WAL and checksum helpers
- `idam.rs`: durable store path layout helpers
- `pani.rs`: maintenance-planning boundary for flush, compaction, checkpoint,
  and garbage collection
- `sevai.rs`: transport-agnostic asynchronous server runtime

The public Rust surface is exported from the crate root:

- `PezhaiEngine`
- `SnapshotHandle`
- `Bound`
- `ScanRange`
- `ScanCursor`
- `ScanRow`
- `GetResponse`
- `SyncResponse`
- `StatsResponse`
- `LevelStats`
- `LogicalShardStats`
- `Error`

### Runtime model

`PezhaiEngine` is a lightweight handle around shared engine state.
It does not create or own a private async runtime.
Callers use the surrounding Tokio runtime for the async data-plane methods.

The public lifecycle contract is:

- `open(config_path)` is synchronous
- `close(self)` is synchronous
- `create_snapshot`, `release_snapshot`, `put`, `delete`, `get`, `scan`,
  `sync`, and `stats` are asynchronous

### Current milestone-3 state

Milestone 3 upgrades the direct engine from a WAL-backed memtable engine to one
shared data plane with retained manifest generations and published `.kjm` data
files.

The current engine implementation keeps:

- one current shared-data manifest plus retained historical manifests keyed by
  `data_generation`
- one active memtable reference that stays mutable until freeze
- one ordered frozen-memtable queue, oldest first, with monotonic
  `frozen_memtable_id`
- published shared-data files grouped by level
- one snapshot table that pins `snapshot_seqno` plus `data_generation`
- one whole-keyspace logical-shard stats view whose `live_size_bytes` value is
  tracked in owner-local state

The retained-manifest rule is:

- writes mutate only the current active memtable object
- freeze creates the next generation by swapping in a fresh empty active
  memtable and appending the old active object to the frozen queue
- flush publish creates the next generation by removing the oldest frozen
  source and adding one L0 file
- compaction publish creates the next generation by removing one exact input
  file set and adding one level-1 output file

Historical manifests are kept in memory for the life of the open engine
instance so snapshot reads can continue using the exact pinned source set even
after later freezes, flushes, or compactions are accepted.

### Boundaries between small admission work and heavier work

The public engine methods are designed around a small critical section over
owner-local state. Milestone 3 keeps that boundary:

- validate arguments before doing heavier work
- resolve snapshot handles and manifest generations before suspension
- capture immutable read or maintenance plans inside the owner critical section
- perform WAL I/O, data-file reads, scans, flush builds, and compaction builds
  only after that section is released

The resulting split is:

- `put` and `delete` validate input and hand the mutation to the worker thread
- `get` may answer inline only when the pinned active memtable already contains
  the visible result; otherwise it captures a point-read plan and runs the
  blocking file work outside the owner lock
- `scan` captures one owned scan plan immediately and defers row materialization
  until `ScanCursor::next()`

### Worker and publication model

The engine still uses one internal worker thread for sequential WAL ownership.
That worker now performs three distinct responsibilities:

- append `Put` and `Delete` records in seqno order
- advance the explicit durable frontier for `sync()`
- perform flush and compaction publication after ordinary writes have installed
  any needed in-memory freeze

Flush and compaction both follow the same publication boundary:

1. build one temporary output file from immutable captured sources
2. fsync that temporary file
3. rename it to the canonical `data/<file_id_20d>.kjm` path
4. fsync the `data/` directory
5. append the matching `FlushPublish` or `CompactPublish` WAL record
6. install the next in-memory manifest generation

If the canonical file exists but the publish record was never accepted, the
file stays orphaned and the engine keeps it invisible because read routing
consults only the current or retained manifests.

### Durable file responsibilities

The durable-format modules now have distinct responsibilities:

- `pathivu.rs` owns `CURRENT`, WAL filenames, WAL publish payload encoding,
  `.kjm` file encode/decode, replay, and CRC32C validation
- `idam.rs` owns canonical and temporary path derivation for `CURRENT`, `wal/`,
  `meta/`, and `data/`
- `pani.rs` owns the execution helpers that turn immutable read and maintenance
  plans into blocking file work

Milestone 3 keeps the shared-data file format intentionally simple:

- every published data file uses one root data-leaf block stored at block id `1`
- the file still uses the common `.kjm` header and footer plus the data-leaf
  slot layout from the spec
- file validation checks the header, footer, block CRC, internal-key order, and
  the summary fields mirrored into the WAL payload

### Read routing

Reads route through the pinned manifest generation rather than the latest live
state.

Point reads probe sources in this order:

1. the pinned active memtable
2. frozen memtables in descending `frozen_memtable_id`
3. matching L0 files in descending `file_id`
4. at most one matching file per higher level

Scans capture the pinned manifest generation, collect overlapping sources, and
merge internal records by full internal-key order before suppressing shadowed
versions and tombstones for the pinned `snapshot_seqno`.

### Recovery model

Recovery now rebuilds the current shared-data plane from two sources only:

- the latest accepted manifest publications named by replayed `FlushPublish`
  and `CompactPublish` records
- the remaining uncovered `Put` and `Delete` records that become the recovered
  active memtable

No frozen memtable survives restart. Orphan data files are ignored because
replay validates and installs only files explicitly named by durable publish
records.
