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

### Current milestone-2 state

Milestone 2 upgrades the direct engine from an in-memory foundation to a
WAL-backed memtable engine with replay-based recovery.

The current engine implementation keeps:

- an ordered in-memory map keyed by user key
- per-key version lists ordered by descending sequence number
- a snapshot table that pins `snapshot_seqno` plus `data_generation`
- a background WAL worker that performs sequential segment appends and explicit
  durability syncs
- replay logic that reconstructs the active memtable from surviving `Put` and
  `Delete` records
- a whole-keyspace logical-shard stats view

`CURRENT` parsing is present now so later checkpoint installs can reuse the
same durable pointer format, but milestone 2 still rejects non-empty checkpoint
references because metadata checkpoints are not implemented until later work.

### Boundaries between small admission work and heavier work

The public engine methods are designed around a small critical section over
owner-local state.
Milestone 2 uses that critical section only for validation, snapshot handling,
seqno reservation, and immutable plan capture before handing WAL I/O to the
background worker.

Later milestones must preserve the same boundary:

- validate arguments before doing heavier work
- capture immutable read or maintenance plans inside the owner critical section
- perform blocking or file-backed work only after that section is released

### Durable path and checksum foundations

The durable-format foundation is present even before the WAL and data files are
fully implemented:

- `pathivu.rs` owns `CURRENT`, WAL filenames, segment encoding, replay, and
  CRC32C validation
- `idam.rs` owns canonical path derivation for `CURRENT`, `wal/`, `meta/`, and
  `data/`
- `pani.rs` still marks the boundary where future maintenance planning will
  live

These modules exist now so later durable milestones can extend them without
changing the public API or the crate layout again.
