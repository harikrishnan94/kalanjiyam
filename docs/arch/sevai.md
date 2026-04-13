# Pezhai Server Architecture

This document records the milestone-5 runtime architecture for `pezhai::sevai`
and the `pezhai-sevai` TCP/protobuf binary. The server now embeds the
persistent engine directly inside the owner actor, reuses the engine helpers
from `iyakkam`, `pathivu`, and `pani`, and adds durable worker, scan, and
maintenance orchestration that the milestone-5 spec requires.

### Package responsibilities

- `crates/pezhai/src/sevai.rs` owns the public server handle, owner actor
  task, lifecycle state machine, cancellation index, reader stats, WAL sync,
  maintenance state, and the transport-agnostic request and response types.
- `crates/pezhai-sevai/src/adapter.rs` owns configuration loading, framing,
  protobuf encoding/decoding, cancel-token synthesis, disconnect detection,
  and mapping wire requests to `ExternalRequest` plus the matching response.
- `crates/pezhai-sevai/src/main.rs` boots the server, starts the adapter, and
  waits for shutdown signals. The adapter never mutates owner-held state; every
  request crosses the owner boundary through `PezhaiServer::call`.

### Lifecycle and owner state

The owner actor implements the Booting -> Ready -> Stopping -> Stopped state
machine defined by the spec. Booting opens the engine, WAL, store layout, and
the owner-owned WAL append state, and only after that initialization completes
does the actor mark itself Ready and accept RPCs.

Owner-held mutable state includes:

- Embedded `EngineState` plus the persisted `StoreLayout` references.
- `OwnerWalAppendState` with seqno allocator, active WAL segment cursor, active
  memtable owner, bytes-on-disk cursor, and durable frontier coverage data.
- Lifecycle flags plus a write-admission stop flag that keeps reads alive while
  blocking new writes after fatal WAL or sync failures.
- Per-client ordering tables that guarantee replies exit in `request_id`
  sequence and scan-specific FIFOs that keep each `scan_id` ordered.
- The cancellation index keyed by `(client_id, effective_cancel_token)`.
- Scan session table with queued fetch FIFOs, active fetch task id, in-flight
  scan-page counter, expiry deadlines, and pinned snapshots.
- Durability waiter registry keyed by target durable seqno plus cancel token.
- Bounded worker queue counters and maintenance retry/backoff state.
- Maintenance in-flight indicators so only one checkpoint, one logical task, one
  GC task, and the configured number of flush/compaction tasks run at once.

### Communication topology

Requests enter through a bounded `external_tx` channel sized by
`server_limits.max_pending_requests`. `PezhaiServer::call` performs
non-blocking admission: it synthesizes a cancel token when the wire request
lacks one, enforces per-client ordering and worker limits, and returns `Busy`
before mutating visible state when the queue is full. Closed or canceled
channels keep returning `Err(ServerUnavailable)`.

All other control traffic — cancel requests, shutdown, worker completions, WAL
sync completions, maintenance results, retry timers, and scan expiry ticks —
arrives on an unbounded `control_tx`. The owner loop always biases
`tokio::select!` toward control traffic so cancel and shutdown work even when
the external queue is saturated.

### Worker pool

A bounded `mpsc::channel(server_limits.max_worker_tasks)` feeds
`server_limits.worker_parallelism` Tokio worker tasks. Workers execute point
reads, scan pages, and maintenance work by invoking the shared blocking helpers
in `pani`. They remain stateless and call `spawn_blocking` only for pure
filesystem work such as data file reads, checkpoint builds, and GC deletes.
The owner actor orchestrates admission, cancellation, backpressure, and stats
publication.

### WAL sync actor

The WAL sync actor is a separate Tokio task with its own bounded channel for
immutable `WalSyncPlan`s. Each plan references bytes that are already appended,
the active segment identity (using its first seqno as the segment id), the
target durable seqno, and an indicator of whether the current durable frontier
already covers the writes. The actor coalesces plans for the same segment,
flushes when `wal.group_commit_bytes` is reached or when the oldest pending plan
exceeds `wal.group_commit_max_delay_ms`, and then reports success or failure
back to the owner.

Success updates the durable frontier; failure sets the owner write-stop flag,
fails impacted waiters with retryable `IO`, and stops new writes until the
owner restarts the pipeline. The WAL sync actor never mutates seqno allocation,
append cursor ownership, or waiter tables; it merely fsyncs bytes that the
owner already owns.

### Scan sessions and pagination

Scan helpers reuse the engine-shared snapshot capture and plan-building code in
`iyakkam`. `ScanStart` validation enforces `max_scan_sessions`, builds a
pinned snapshot, allocates a `scan_id`, and creates a `ScanSession` that tracks
the requested range, pagination limits, resume key, snapshot handle, queued
fetches, expiry timestamp, and the data generation that was pinned.

`ScanFetchNext` either answers inline when the active memtable can satisfy the
page or captures one `ScanPagePlan` and dispatches a worker. The owner allows
at most `server_limits.max_in_flight_scan_tasks` concurrent page workers; the
remaining fetches stay queued inside the session FIFO even across connections,
and attempts to exceed `server_limits.max_scan_fetch_queue_per_session`
return `Busy` before admission. On each non-EOF page, the owner updates
`resume_after_key` before starting the next queued fetch.

EOF tears down the session, releases the snapshot, and fails any queued
follow-ups with `InvalidArgument`. Expiry runs on a 1-second sweep tick: expired
sessions drop snapshots, fail in-flight and queued fetches with `Cancelled
(retryable)`, and clear the queues. Shutdown uses the same cleanup path so
long-lived snapshots never survive owner termination.

### Cancellation index and disconnect handling

Every admitted request carries an effective cancel token: either the one supplied
on the wire or a hidden token generated by the adapter when none is present.
The cancellation index maps queued external requests, queued scan fetches,
running read tasks, and durability waiters by `(client_id, cancel_token)`.

`PezhaiServer::cancel` scrubs the queues, marks active tasks as canceled so that
their results are dropped, and lets worker completions realize the cancellation
without publishing stale responses. While `server.call()` is running, the TCP
adapter watches for EOF or response-write failures and invokes
`cancel(client_id, effective_token)` so that only that connection's queued work
and waiters unwind without touching shared scan sessions.

### Write-stop guardrails

Durability waiters register with the owner when sync mode demands it. Each
waiter is keyed by the target durable seqno plus the cancel token. After a WAL
append or WAL sync failure, the WAL sync actor signals failure. The owner sets a
write-stop flag, blocks new writes and WAL sync plans, and fails every impacted
waiter with retryable `IO`. Reads continue to work so long as their snapshots
remain pinned. Recovery requires restarting the WAL sync pipeline so the owner
can resume admitting writes.

### Maintenance coordination

Maintenance reuses the shared `iyakkam` and `pani` helpers for flush,
compaction, checkpoint installation, logical shard maintenance, and GC. The
owner validates worker results, publishes them, and keeps caps on concurrency:
one checkpoint, one logical task, one GC, and configurable flush/compaction
limits. Worker tasks send results back over the control channel so the owner can
accept, retry, or drop them.

Maintenance `IO` failures retry internally with `50ms`, `200ms`, and `1s`
backoffs; after the third retry the stale attempt is dropped so the next sweep
can replan from current state. `Stale` remains an internal-only reason never
exposed to the protocol.

### Stats and read-only observations

`Stats` stays owner-local and reads the embedded engine state via the shared
helpers so no file I/O occurs. Active-memtable `Get`s return inline, while
misses capture a point-read plan that a worker executes. Level and logical
shard stats follow the same snapshots already maintained inside the engine.

### Adapter and transport

`pezhai-sevai` keeps the transport stack thin. The adapter frames requests,
maps them to `ExternalRequest`, waits for the logical response, and writes one
frame before reading the next request. It is responsible for cancel-token
synthesis and calling `PezhaiServer::cancel` when the socket closes with a
call outstanding. The binary still parses `--config`, starts the server before
serving, runs the TCP adapter, and waits for shutdown.
