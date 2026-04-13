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

### Current milestone-1 state

Milestone 1 establishes the public engine surface and the module boundaries
without claiming durable storage behavior yet.

The current engine implementation keeps:

- an ordered in-memory map keyed by user key
- per-key version lists ordered by descending sequence number
- a snapshot table that pins `snapshot_seqno` plus `data_generation`
- a whole-keyspace logical-shard stats view

This gives the direct API a stable contract for validation, snapshots, scans,
and stats while the durable WAL, file, checkpoint, and maintenance machinery is
landed in later milestones.

### Boundaries between small admission work and heavier work

The public engine methods are designed around a small critical section over
owner-local state.
Milestone 1 keeps all work inside that in-memory critical section because no
durable I/O exists yet.

Later milestones must preserve the same boundary:

- validate arguments before doing heavier work
- capture immutable read or maintenance plans inside the owner critical section
- perform blocking or file-backed work only after that section is released

### Durable path and checksum foundations

The durable-format foundation is present even before the WAL and data files are
fully implemented:

- `pathivu.rs` owns the CRC32C helper required by the spec
- `idam.rs` owns canonical path derivation for `wal/`, `meta/`, and `data/`
- `pani.rs` marks the boundary where future maintenance planning will live

These modules exist now so later durable milestones can extend them without
changing the public API or the crate layout again.
