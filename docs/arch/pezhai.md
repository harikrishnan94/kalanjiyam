# Pezhai Architecture

This document describes the intended layering boundary for the `pezhai` storage subsystem.

`PezhaiEngine` is the async storage-engine layer. It owns storage semantics, recovery, snapshot
rules, WAL publication rules, immutable plan capture, and maintenance semantics. It is transport
agnostic, and its contract does not depend on any particular host thread model or runtime
topology. That keeps the engine usable directly in tests and embeddings without depending on
server-hosted transport or orchestration concerns.

`PezhaiServer` is one concrete host for that engine layer. It owns thread spawning, runtime
orchestration, worker execution, request admission, cancellation, ordering, scan-session
management, and transport-facing behavior. The server is where broader runtime policy is attached
to the engine.

This architecture document intentionally describes the intended layering even where implementation
details are still converging. The normative source of truth for externally visible semantics,
ordering, and durable-format details remains `docs/specs/pezhai/storage-engine.md` and
`docs/specs/pezhai/server.md`.

## Summary

`pezhai` is one shared LSM-style data plane plus one latest-only logical-shard map.

- The shared data plane owns the active memtable, frozen memtables, published files, manifest
  history, snapshots, and recovery state.
- Logical shards are metadata only. They do not partition the read path, memtable ownership, file
  ownership, flush ownership, or compaction ownership.
- Reads are pinned by `(snapshot_seqno, data_generation)`.
- `Put`, `Delete`, and explicit `Sync` all route through the same owner-managed WAL waiter table
  when durability must wait for the sync thread.
- Flush, compaction, checkpoint, and GC preparation now run on workers and return structured
  results for owner-thread validation and publication.
- Immutable captured plans separate live mutable state from delegated work.

The layering boundary is:

- `PezhaiEngine` owns storage behavior and the async engine API.
- A host owns execution strategy.
- `PezhaiServer` is the repository's concrete threaded host.

In practice, the engine captures immutable read or maintenance plans, and the host decides whether
to execute those plans inline or delegate them. The server uses owner-loop execution, a Tokio
runtime, background workers, and a dedicated WAL service as one concrete strategy. Those choices
belong to the host layer, not to the engine contract.

## Component Map

### `src/pezhai/engine.rs`

`engine` owns the storage-engine state machine and the async engine-facing surface.

- It defines `PezhaiEngine` as the async API used by direct embeddings and tests.
- It owns `EngineState`.
- It owns operation planning for reads, writes, sync waits, and maintenance work.
- It owns snapshot allocation and release.
- It owns manifest history, logical-shard publication, and stale-result rejection rules.
- It owns WAL append publication, durable-frontier publication, rollover rules, and waiter
  semantics.
- It owns checkpoint capture rules, recovery order, and maintenance publication semantics.

The engine is described in host-agnostic terms: it decides storage semantics and captures
immutable work inputs, but it is not defined by any specific thread or worker topology.

### `src/sevai/mod.rs`, `src/sevai/owner.rs`, `src/sevai/worker.rs`

`sevai` owns the in-process server host.

- `mod.rs` defines `PezhaiServer`, the public request and response types, and the bounded mailbox
  handles used by callers.
- `owner.rs` owns the owner-loop state machine, request routing, per-client response ordering,
  scan-session tables, WAL waiters, maintenance retries, and shutdown cleanup.
- `worker.rs` owns the stateless worker-pool queue used for delegated immutable work.

`sevai` does not redefine storage semantics. It hosts the engine and adds transport-facing and
runtime-facing behavior around it.

### `src/pezhai/config.rs`

`config` parses and validates `store/config.toml`.

- It owns the operator-facing config surface.
- It implements the normative `[engine]`, `[wal]`, and `[lsm]` tables.
- It also defines the current implementation-specific `[maintenance]` table used for host policy,
  scheduling, retries, and queue sizing.
- It resolves `config_path` into the canonical `store_dir` used by engine-supporting code.

`config` supports engine open, recovery, and host policy wiring. It does not define a thread model
by itself.

### `src/pezhai/codec.rs`

`codec` owns low-level durable encoding helpers and canonical path helpers.

- It encodes and decodes `CURRENT`.
- It encodes and decodes WAL segment headers, footers, and records.
- It encodes shared durable payloads used by WAL records and `.kjm` metadata blocks.
- It defines canonical relative filenames and `resolve_store_path`.
- It provides checksum and alignment helpers shared by WAL and `.kjm` handling.

`codec` owns byte-level translation only. It does not decide publication, replay, or execution
strategy.

### `src/pezhai/durable.rs`

`durable` owns `.kjm` file handling.

- It writes and validates shared data files.
- It writes and validates metadata checkpoint files.
- It provides crash-safe checkpoint file and `CURRENT` install helpers.
- It loads checkpoint contents back into `CheckpointState`.
- It exposes both fully parsed data files and lazy point-read or forward-scan
  helpers so the engine can probe one file or merge scan sources without
  materializing whole files in memory.

`durable` owns artifact mechanics. The engine decides when those mechanics are used.

### `src/pezhai/types.rs`

`types` owns the shared in-memory structures and validation helpers used by the engine.

Important types include:

- `ActiveMemtable` and `FrozenMemtable`
- `FileMeta`
- `DataManifestSnapshot` and `DataManifestIndex`
- `SnapshotHandle`
- `LogicalShardEntry`
- public response types such as `GetResponse`, `ScanRow`, and `StatsResponse`

This module also owns range validation, key validation, internal-record ordering, and logical
shard validation. The current memtable representation maintains both point-lookup visibility state
and ordered scan iteration state so writes do not need to re-sort the full memtable on each
insert. The visibility index stores per-key version positions back into the append-order record
buffer so steady-state writes pay ordered `BTreeMap` maintenance without duplicating full
payload-bearing records in the index itself.

## Durable State

At the store-root level, the implementation expects the spec-defined layout:

- `store/config.toml`
- `store/CURRENT`
- `store/wal/*.log`
- `store/meta/*.kjm`
- `store/data/*.kjm`

The module split around that durable state is:

- `config` owns semantic config parsing and validation.
- `codec` owns durable encoders, decoders, checksums, and canonical filenames.
- `durable` owns `.kjm` read and write mechanics plus checkpoint installation helpers.
- `engine` owns recovery order, CURRENT loading, checkpoint selection, WAL discovery, WAL replay,
  active WAL adoption or creation, and recovered-state materialization.

The engine therefore owns storage recovery semantics and publication rules. A host may decide when
and where open happens, but the host does not redefine the durable format or recovery behavior.

## In-Memory Architecture

### Engine State

`EngineState` owns the mutable storage state for one open engine instance.

Conceptually, it tracks:

- seqno allocation and committed or durable frontiers
- file, snapshot, checkpoint, and frozen-memtable allocators
- the current active memtable
- the current manifest plus retained manifest history
- the latest logical-shard map
- active snapshots
- durable-root tracking such as the current checkpoint pointer
- WAL writer state, segment rollover metadata, and unsynced-byte accounting

The current implementation keeps active snapshots in a monotonic slot table
indexed by snapshot id, which gives constant-time create, validate, and release
operations without renumbering older handles.

The engine's mutable state is the source of truth for storage visibility, publication, and replay.
The architecture is defined around that state machine, not around one required threading model.

### Server State

`OwnerRuntime` adds host-only runtime state around one hosted engine instance.

It owns:

- lifecycle state such as `Booting`, `Ready`, `Stopping`, and `Stopped`
- admission queues and per-client ordering tables
- cancelled-token and disconnected-client tracking
- scan-session state, including pinned snapshots, paging cursors, and fetch FIFO queues
- pending external requests, WAL waiters, and in-flight sync-batch tracking
- maintenance retry state and delayed retry scheduling
- worker dispatch queues and task-id allocation

None of this state changes storage semantics. It exists to host the engine behind an async server
boundary.

## Ownership and Threading Boundary

Threading is not part of the `PezhaiEngine` contract.

- The engine owns storage semantics, immutable plan capture, publication rules, and recovery
  semantics.
- A host owns execution topology.
- `PezhaiServer` chooses and owns the concrete threaded topology used by the server runtime.

That means the engine should be describable without binding its identity to owner-thread
mechanics. The important engine boundary is that live mutable state stays under engine control,
while delegated work operates on immutable captured inputs and must be revalidated before
publication.

`PezhaiServer` is the concrete place where the current repository attaches:

- an owner loop
- a Tokio current-thread runtime
- background worker threads
- a dedicated WAL service thread
- request admission and backpressure
- cancellation wiring
- scan-session management
- transport-facing request and reply behavior

Those are server-hosting decisions. They are not what makes `PezhaiEngine` the engine.

### Owner Loop Topology

The implemented server hosts one dedicated Tokio current-thread runtime on its own thread. That
runtime runs one owner loop task which owns the `PezhaiEngine` instance as ordinary state, so
mutable engine state, per-client ordering, queue counters, scan sessions, WAL waiters, and
admission limits all live behind one owner-thread boundary.

Callers interact with the owner thread through bounded async mailboxes:

- one external request queue
- one control queue for cancel, disconnect, and shutdown
- one worker-result queue
- one WAL-sync-result queue

Each owner turnaround asks the engine whether a request can finish immediately, must wait for WAL
sync, or should dispatch one immutable plan such as `GetPlan`, `ScanPlan`, or one maintenance
task. The owner tracks one unified WAL waiter table keyed by external request. Waiting
`PerWrite` `Put`, `Delete`, and explicit `Sync` requests all register there and all complete from
the same durable-frontier publication path.

Immutable read work and maintenance preparation run on background threads that never mutate shared
state directly. Workers execute captured plans and send results back to the owner, and the owner
remains the only publisher of visible state by calling helpers such as `finish_get`,
`finish_scan_page`, `publish_flush_task_result`, `publish_compact_task_result`,
`publish_checkpoint_task_result`, `publish_gc_task_result`, and `publish_wal_sync_result`.

Integration tests now validate server behavior only through the public lifecycle and request APIs.
The production `sevai` modules no longer embed a hidden test-control surface or deterministic
runtime blocking hooks.

## Operation Model

The engine presents one shared operation workflow for storage behavior.

For each operation, the engine captures exactly one of:

- an immediate completion
- an immutable point-read plan
- an immutable range-scan plan
- one engine-owned WAL durability wait
- one immutable maintenance task plan

The host may execute or delegate those plans, then route completions back through the engine for
validation and publication. `PezhaiServer` uses threads and workers as one concrete execution
strategy for that workflow.

## Read Path

Reads stay host agnostic at the architecture level.

- The engine validates the request against the current or requested snapshot view.
- The engine captures immutable read plans pinned by `snapshot_seqno` and `data_generation`.
- The host may execute that plan inline or delegate it.
- Completion returns to the engine boundary before any final reply is emitted.

Logical shards remain metadata only in this read model. They influence metadata and maintenance
policy, but they do not partition point reads, scans, memtables, or published files.

For the concrete server host:

- the server adds latest-only external read semantics
- the server owns paged scan sessions and per-scan fetch ordering
- the server may dispatch captured read plans to worker threads

## Write Path and WAL Publication

Writes are also framed in engine terms first.

- The engine validates the mutation and allocates visibility order.
- The engine appends the mutation through WAL publication rules.
- The engine applies the mutation to the shared active memtable.
- The engine determines whether completion is immediate or waits on one durability frontier.
- The engine may capture follow-on maintenance work from the resulting state transition.

The host decides how to wait for or observe those outcomes. `PezhaiServer` adds admission,
cancellation, per-client ordering, and response emission around the same engine-owned write rules.
The server does not keep a separate write path for `PerWrite` durability. It uses the same
engine-owned write decision for direct writes and server-hosted writes, then routes waiter-backed
durability through the dedicated WAL sync thread.

The dedicated WAL service is part of the hosted execution strategy, but durable-frontier semantics
remain engine owned. Seqno allocation, append visibility, waiter completion, and stale-result
rejection are storage rules, not transport rules.

## Maintenance Model

Maintenance is defined by immutable captured work plus engine-side revalidation.

- The engine decides when a flush, compaction, checkpoint, or GC task is needed.
- The engine captures the immutable inputs needed to perform that task safely outside live mutable
  state.
- The host may execute or delegate the captured task.
- Worker execution prepares durable artifacts or deletion results, but it does not publish them.
- The engine validates the completion against current state before publication.

This keeps live mutable state separate from I/O-heavy or CPU-heavy work without changing the
engine's ownership of semantics.

Logical shard split and merge remain metadata-only engine operations in the current implementation.
They publish `LogicalShardInstall` WAL records directly through engine-controlled validation rather
than through one separate worker-dispatched logical-maintenance task kind.

`PezhaiServer` uses worker threads, retry policy, queue limits, idle polling, and shutdown-aware
coordination as one concrete host strategy for those maintenance tasks. Only one task per
maintenance kind is active at a time, and the owner dispatch order is flush, compaction,
checkpoint, then GC. Checkpoint capture is driven by a dedicated
`maintenance.checkpoint_interval_secs` deadline, while GC keeps its separate
`maintenance.gc_interval_secs` deadline. The owner also contributes in-flight worker pins to GC
capture so the engine can decide safe deletion against both engine-known references and host-only
references.

## Recovery Pipeline

Open-time recovery is engine owned.

The engine is responsible for:

1. parsing `config.toml`
2. creating required directories
3. loading and validating `CURRENT`
4. loading the referenced checkpoint state
5. discovering and replaying WAL records after the durable frontier
6. materializing the recovered active memtable, manifest state, and logical-shard map
7. opening or creating the active WAL
8. establishing the runtime state needed for later publication and maintenance

A host supplies lifecycle wiring around that process. `PezhaiServer` performs the concrete server
startup sequence and does not accept requests until engine open succeeds.

## Config Surface

The normative config surface remains the spec-defined `[engine]`, `[wal]`, and `[lsm]` tables.
`engine.page_size_bytes` is now part of that durable surface because new `.kjm` artifacts encode
their chosen page size in the common header.

The current implementation also uses an implementation-specific `[maintenance]` table for
scheduling, retries, queue sizing, scan-session limits, generic background-worker sizing, and GC
polling cadence. It now includes both `maintenance.gc_interval_secs` and
`maintenance.checkpoint_interval_secs`. Those settings tune host policy and operational behavior,
but they do not redefine the engine's durable format or externally visible storage semantics.

The durable reader path now takes page size from each `.kjm` header instead of assuming one global
constant for the whole store. That keeps reopen-time `engine.page_size_bytes` changes local to new
artifacts, while older files remain readable through their own encoded header value. Format-major
v1 still caps supported page sizes at 32 KiB because several block-local offsets remain `u16`.

## Design Notes

### Logical Shards Stay Metadata Only

The implementation keeps one shared read and write data plane for the whole keyspace. Logical
shards track the latest metadata map and `live_size_bytes`, but memtables, files, snapshots,
flushes, and compactions are not partitioned by shard.

### Immutable Plans Protect the Live State Machine

Immutable point-read, range-scan, and maintenance plans are the mechanism that separates live
mutable engine state from delegated work. This boundary is what lets a host use workers without
introducing concurrent mutation algorithms into the storage state machine.

### The Server Attaches Runtime and Transport Concerns

`PezhaiServer` is where the repository attaches request routing, client ordering, cancellation,
scan sessions, worker pools, owner-loop execution, and runtime shutdown behavior. Those concerns
surround the engine; they do not define the engine.

## Relationship to the Specs

The specs remain authoritative for semantics and durable-format details.

- See `docs/specs/pezhai/storage-engine.md` for operation semantics, invariants, crash behavior,
  and durable bytes.
- See `docs/specs/pezhai/server.md` for server routing, ordering, cancellation, and backpressure
  rules.

This document summarizes implementation layering only. It does not replace the specs, and it does
not restate their normative detail.
