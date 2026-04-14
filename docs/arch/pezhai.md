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
- `ScanPageLimits`
- `ScanPageResponse`
- `GetResponse`
- `SyncResponse`
- `StatsResponse`
- `LevelStats`
- `LogicalShardStats`
- `WriteDecision`
- `DurabilityWait`
- `GetDecision`
- `DeferredGet`
- `PagedScan`
- `ScanPageDecision`
- `DeferredScanPage`
- `Error`

The shared engine types listed above are canonical only at `pezhai::*`.
`pezhai::sevai` uses those types but does not re-export duplicate public paths.

### Runtime model

`PezhaiEngine` is a lightweight handle around shared engine state.
It does not create or own a private async runtime.
Callers use the surrounding Tokio runtime for the async data-plane methods.

The public lifecycle contract is:

- `open(config_path)` is synchronous
- `close(self)` is synchronous
- `create_snapshot`, `release_snapshot`, `put`, `delete`, `get`, `scan`,
  `sync`, and `stats` are asynchronous
- `prepare_put`, `prepare_delete`, `prepare_latest_get`, and `start_paged_scan`
  are synchronous admission helpers that return engine-owned decisions or
  handles for later async execution

### Current milestone-4 state

Milestone 4 keeps the shared-data manifest model from milestone 3 and adds the
durable metadata plane needed for `CURRENT`, metadata checkpoints, logical
shards, WAL truncation, and file GC.

The current engine implementation keeps:

- one current shared-data manifest plus retained historical manifests keyed by
  `data_generation`
- one current latest-only logical-shard map stored separately from the
  snapshot-visible shared-data generations
- one `durable_current` record that mirrors the checkpoint named by `CURRENT`
  and keeps its file references alive for GC
- one snapshot table that still pins only `snapshot_seqno` plus
  `data_generation`
- one checkpoint-generation allocator and one short checkpoint-capture pause
  that guards exact metadata capture

The retained-state rule is now:

- writes mutate only the current active memtable object
- freeze creates the next generation by swapping in a fresh empty active
  memtable and appending the old active object to the frozen queue
- flush publish creates the next generation by removing the oldest frozen
  source and adding one L0 file
- compaction publish creates the next generation by removing one exact input
  file set and adding one replacement output set
- logical split and merge do not create a new `data_generation`; they replace
  only the current logical-shard map
- historical manifests are pruned once they are no longer the current
  generation, no active snapshot still pins them, and the durable checkpoint
  referenced by `CURRENT` no longer needs their file set

### Boundaries between small admission work and heavier work

The public engine methods are designed around a small critical section over
owner-local state. Milestone 4 keeps that boundary:

- validate arguments before doing heavier work
- resolve snapshot handles and manifest generations before suspension
- capture immutable read or maintenance plans inside the owner critical section
- perform WAL I/O, data-file reads, scans, exact logical-byte recomputation,
  checkpoint file writes, and GC deletion only after that section is released

The resulting split is:

- `put` and `delete` validate input and hand the mutation to the worker thread
- `get` may answer inline only when the pinned active memtable already contains
  the visible result; otherwise it captures a point-read plan and runs the
  blocking file work outside the owner lock
- `scan` captures one owned scan plan immediately and defers row materialization
  until `ScanCursor::next()`
- `prepare_put` and `prepare_delete` append one WAL mutation without forcing
  immediate fsync and return one engine-owned durability waiter when `per_write`
  acknowledgement still needs the durable frontier
- `prepare_latest_get` returns either one inline `GetResponse` or one
  `DeferredGet` that performs blocking file reads later
- `start_paged_scan` returns one `PagedScan` handle whose `prepare_next_page()`
  method yields either one inline `ScanPageResponse` or one `DeferredScanPage`

### Worker and publication model

The engine still uses one internal worker thread for sequential WAL ownership.
That worker now performs four distinct responsibilities:

- append `Put` and `Delete` records in seqno order
- publish flush and compaction results after ordinary writes have installed any
  needed in-memory freeze
- publish `LogicalShardInstall` after exact latest-state recomputation chooses
  one split or merge candidate
- install metadata checkpoints, update `CURRENT`, truncate covered closed WAL
  segments, and best-effort delete GC-eligible data files

Explicit durability now has two public entry points:

- direct `sync()` asks the worker for one immutable sync plan, executes the
  fsync outside the worker, then records the covered durable frontier
- `DurabilityWait::wait()` executes the same kind of immutable sync plan for one
  prepared write and then re-enters the worker only for the follow-on
  maintenance tick

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

Logical-shard installs follow a simpler boundary:

1. capture the current logical-shard source entries plus one latest-state scan
   plan
2. recompute exact live bytes outside the owner lock
3. choose at most one split or merge candidate for that maintenance pass
4. append one `LogicalShardInstall` WAL record
5. replace the current logical-shard source entry set atomically in memory

Checkpoint install follows the spec's capture ordering:

1. freeze the active memtable if it still contains committed records
2. flush until the current generation has zero frozen memtables and an empty
   active memtable
3. enter a short checkpoint-capture pause and snapshot the current published
   file manifest, current logical-shard map, and allocator state
4. write one temporary metadata checkpoint file under `meta/`, fsync it, rename
   it to its canonical filename, and fsync `meta/`
5. write one temporary `CURRENT`, fsync it, rename it to `CURRENT`, and fsync
   the store root
6. only after `CURRENT` is durable, best-effort delete any closed WAL segment
   fully covered by `checkpoint_max_seqno`

### Durable file responsibilities

The durable-format modules now have distinct responsibilities:

- `pathivu.rs` owns `CURRENT`, WAL filenames, WAL record payload encoding,
  metadata-checkpoint `.kjm` encode/decode, replay, WAL truncation helpers, and
  CRC32C validation
- `idam.rs` owns canonical and temporary path derivation for `CURRENT`, `wal/`,
  `meta/`, and `data/`
- `pani.rs` owns the execution helpers that turn immutable read and maintenance
  plans into blocking file work, including exact logical-byte recomputation and
  best-effort GC deletion

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

Recovery now rebuilds the current engine state from up to three sources:

- `CURRENT`, when present, names the authoritative durable checkpoint frontier
- the referenced metadata checkpoint seeds the published-file manifest, current
  logical-shard map, `next_seqno`, and `next_file_id`
- WAL replay after `checkpoint_max_seqno` reapplies later mutations,
  shared-data publishes, and logical-shard installs

No frozen memtable survives restart. Recovery materializes exactly one current
generation with one recovered active memtable and zero frozen memtables.
Metadata checkpoints not referenced by `CURRENT` remain orphan and invisible.
Data files remain visible only when the recovered current manifest or the
durable checkpoint referenced by `CURRENT` still names them.

### GC Rules

GC considers a data file deletable only when all of the following are false:

- the current shared-data manifest still names the file
- any snapshot-pinned historical manifest still names the file
- the durable checkpoint referenced by `CURRENT` still names the file

File deletion is best-effort and ordered by stable filename order. Failed
deletes do not roll back an accepted publish or checkpoint. Old logical-shard
maps are not retained after install because snapshots never pin logical-shard
history.
