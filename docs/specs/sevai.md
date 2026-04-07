# Pezhai Server Specification

This document is the normative specification for the asynchronous `Pezhai` server.
It defines the in-process server architecture and the transport-agnostic logical RPC contracts that
surround the storage-engine behavior defined in `docs/specs/pezhai.md`.

Normative keywords:

- `MUST`: required for correctness or compatibility
- `MUST NOT`: forbidden
- `SHOULD`: strongly recommended unless there is a clear reason not to
- `MAY`: optional

All text in this repository is ASCII-only by rule.

Source of truth:

- `docs/specs/pezhai.md` remains authoritative for storage-engine state, snapshots,
  WAL,
  durable bytes, and engine error classes
- this document is authoritative for server-thread ownership, asynchronous execution, logical RPC
  method shapes, request ordering, cancellation, and backpressure
- `docs/specs/tcp-rpc.md` is authoritative for the concrete proto3-over-TCP framing and
  binding used by this repository
- pseudocode blocks labeled `Normative pseudocode` are authoritative for routing and state
  transitions
- prose examples and diagrams remain explanatory unless a subsection explicitly says
  `Behavioral clarification`

Reading guide:

- Sections 1 through 4 define the server scope, invariants, and architecture
- Sections 5 and 6 define the external and internal logical RPC contracts
- Sections 7 through 10 define routing, scan sessions, maintenance execution, and error handling
- Section 11 defines acceptance criteria and required tests

Naming rules:

- RPC method names and server-internal message names in this document use clear English
- storage-engine naming remains governed by `docs/specs/pezhai.md`

## 1. Problem Restatement

### 1.1 What is being built

Build an asynchronous in-process server around `Pezhai`. [BEHAVIORAL]

The server owns one open `Pezhai` engine instance at a time. [BEHAVIORAL] It exposes a
transport-agnostic external logical RPC surface for client-visible operations and a separate
transport-agnostic internal logical RPC surface between the main thread and background worker
threads. [BEHAVIORAL] Server open and close are tied to process lifetime rather than to external
RPC. [BEHAVIORAL]

The server uses one main thread as the sole owner of mutable shared engine state. [BEHAVIORAL]
Background workers are stateless across jobs and may keep only per-task scratch state while a task
is running. [BEHAVIORAL]
The main thread MUST invoke the engine's shared internal operation model for storage-engine
operations instead of maintaining a separate server-only execution path for `put`, `delete`,
`get`, `scan`, `sync`, `stats`, or internal snapshot commands. [BEHAVIORAL]
The main thread MUST host the engine's mutable storage parts directly as ordinary owner-thread
state, including at minimum `EngineState`, `WalSyncService`, and any pending WAL-reply
bookkeeping, rather than routing through a second private synchronous core abstraction.
  [BEHAVIORAL]

### 1.2 Why it exists

The server exists to make the `Pezhai` engine usable through an asynchronous RPC boundary without
introducing concurrent mutation algorithms around memtables, manifests, snapshots, WAL state, or
logical-shard metadata.

The core design goals are:

- one-thread ownership of all shared mutable state [BEHAVIORAL]
- external read and write RPCs that preserve the storage-engine semantics exactly [BEHAVIORAL]
- delegation of arbitrarily complex or blocking work to background workers [BEHAVIORAL]
- stateless workers that prepare results but never publish them directly [BEHAVIORAL]
- latest-only external reads while still using internal snapshots when a paged scan needs a stable
  view across multiple RPCs [BEHAVIORAL]
- bounded queues, cancellation, and explicit backpressure under load [BEHAVIORAL]

### 1.3 What success looks like

Success means:

- the main thread ownership boundary is unambiguous [BEHAVIORAL]
- external RPC method shapes are defined without binding to TCP, HTTP, gRPC, or any byte-level
  transport [BEHAVIORAL]
- internal RPC message types are defined exactly enough that independent implementations route work
  the same way [BEHAVIORAL]
- `put`, `delete`, `get`, `scan`, `sync`, and `stats` preserve the engine semantics already
  defined in `docs/specs/pezhai.md` [BEHAVIORAL]
- server startup reads one configuration file path from the command line and uses that path to open
  the engine before serving requests [BEHAVIORAL]
- the Rust binding exposes startup, request submission, cancellation controls, shutdown, and
  wait-for-stop as asynchronous handle methods [BEHAVIORAL]
- external `get` and external `scan` are latest-only [BEHAVIORAL]
- internal `create_snapshot` and `release_snapshot` exist and are used by paged scans even though
  they are not externally exposed [BEHAVIORAL]
- the main thread routes storage work through the engine-owned operation workflow instead of
  duplicating storage semantics in server-only helpers [BEHAVIORAL]
- worker failures on behalf of one user request are surfaced externally as retryable server errors
  when appropriate, while recoverable background-maintenance failures are retried internally
  without exposing admin RPCs externally [BEHAVIORAL]

Worked example:

```text
Server process starts with --config /srv/pezhai/config.toml:
  1. bootstrap code passes config_path to the main thread
  2. main thread opens one engine instance before serving external RPCs
  3. external client issues scan_start(["ant", "yak"))
  4. main thread validates the range
  5. main thread creates one internal snapshot handle
  6. main thread allocates one scan session
  7. client A later calls scan_fetch_next(scan_id)
  8. client B may also call scan_fetch_next(scan_id)
  9. main thread queues same-scan fetch requests and processes them one by one
  10. worker reads frozen memtables and files for the pinned snapshot
  11. main thread publishes the returned page into the session state
  12. when EOF is reached, main thread releases the internal snapshot
```

ASCII diagram:

```text
external RPCs
      |
      v
+--------------------+
| main thread        |
| - owns all state   |
| - validates        |
| - fast in-memory   |
| - publishes        |
+--------------------+
      |        ^
      |        |
      v        |
+--------------------+
| wal sync thread    |
| - fsync batching   |
| - owner-requested  |
| - no publish       |
+--------------------+
      |
      v
+--------------------+
| bgworker pool      |
| - read frozen/file |
| - scan pages       |
| - flush            |
| - compact          |
| - checkpoint/gc    |
+--------------------+
```

Normative pseudocode:

```text
function conceptual_server_rule(request):
    main_thread_admits(request)
    if request.needs_blocking_or_complex_work:
        dispatch_to_bgworker(request)
        main_thread_validates_and_publishes(worker_result)
    else:
        main_thread_executes(request)
```

## 2. Scope

### 2.1 In scope

- one open `Pezhai` engine instance per server process [BEHAVIORAL]
- one main thread that owns all mutable shared state [BEHAVIORAL]
- one or more stateless background worker threads [BEHAVIORAL]
- process startup that receives one configuration-file path as a command-line argument and opens the
  engine before serving requests [BEHAVIORAL]
- transport-agnostic external logical RPC for `put`, `delete`, `get`, `scan`, `sync`, and `stats`
  [BEHAVIORAL]
- transport-agnostic internal logical RPC for snapshot control, reads, scans, WAL sync,
  flush, compaction, checkpoint, and garbage collection [BEHAVIORAL]
- paged scan sessions using `scan_start` plus `scan_fetch_next` [BEHAVIORAL]
- request ordering, cancellation, and backpressure [BEHAVIORAL]
- retry rules for user-request failures versus automatic maintenance failures [BEHAVIORAL]

### 2.2 Out of scope

- byte-level network framing details in this document [BEHAVIORAL]
- concrete transport wire-format details in this document; see
  `docs/specs/tcp-rpc.md` for the repository's proto3-over-TCP binding [BEHAVIORAL]
- separate worker processes, remote workers, or cross-process RPC [BEHAVIORAL]
- external `Open` or `Close` RPC methods [BEHAVIORAL]
- exposing compaction, flush, checkpoint, garbage collection, logical split, or logical merge as
  external RPCs [BEHAVIORAL]
- authentication and authorization [BEHAVIORAL]
- multi-engine multiplexing in one v1 server instance [BEHAVIORAL]

### 2.3 Non-goals

- allowing workers to mutate manifests, memtables, logical-shard maps, snapshots, or WAL ownership
  directly [BEHAVIORAL]
- exposing external explicit snapshot handles in v1 [BEHAVIORAL]
- making the external scan API a streaming transport contract [BEHAVIORAL]

## 3. Core Invariants

### 3.1 Main-thread ownership

The main thread is the sole owner of:

- engine open and ready state [BEHAVIORAL]
- `engine_instance_id` and open-instance lifecycle [BEHAVIORAL]
- current metadata map and current logical-shard map [BEHAVIORAL]
- active memtable and frozen-memtable registry [BEHAVIORAL]
- current data manifest generation and publish decisions [BEHAVIORAL]
- WAL append cursor and durable-frontier bookkeeping [BEHAVIORAL]
- active snapshot table [BEHAVIORAL]
- scan-session table [BEHAVIORAL]
- per-client request ordering state [BEHAVIORAL]
- worker-task tables, retry state, and backpressure counters [BEHAVIORAL]

### 3.2 Worker restrictions

Background workers:

- MUST NOT mutate shared engine state directly [BEHAVIORAL]
- MUST NOT publish a new manifest generation, logical-shard map, snapshot-table change, or WAL
  durable frontier directly [BEHAVIORAL]
- MAY read immutable task inputs, immutable manifest snapshots, frozen-memtable snapshots, and data
  files [BEHAVIORAL]
- MAY build temporary files, iterators, merge heaps, and scratch buffers that exist only for the
  lifetime of one task [BEHAVIORAL]
- MUST return a result object that the main thread validates before making any shared state visible
  change [BEHAVIORAL]

### 3.3 Visibility and ordering rules

- every externally visible write order is defined only by main-thread seqno assignment
  [BEHAVIORAL]
- every externally visible metadata publish is defined only by a main-thread commit step
  [BEHAVIORAL]
- workers MAY complete tasks out of dispatch order, but the main thread MUST serialize publication
  in a way that preserves engine semantics [BEHAVIORAL]
- a paged external scan MUST read one stable snapshot for its whole session even though the external
  API is latest-only [BEHAVIORAL]
- `ScanFetchNext` requests for the same `scan_id` MAY arrive from arbitrary clients, but the main
  thread MUST serialize them through one per-scan FIFO queue and process them one by one
  [BEHAVIORAL]
- the main thread MUST release any internal scan snapshot after EOF, whole-session cancellation,
  session expiry, or shutdown cleanup [BEHAVIORAL]
- before dispatching worker-side read work or registering durability waiters, the main thread MUST
  ask the engine for one operation decision: complete immediately, execute one immutable read plan,
  or wait on one engine-owned WAL waiter [BEHAVIORAL]
- the server MUST treat that engine decision as authoritative for storage semantics; it MAY still
  apply ordering, cancellation, and backpressure policy around the decision, but it MUST NOT
  duplicate snapshot validation, active-memtable fast-path selection, immutable-source capture, or
  WAL waiter registration outside the engine [BEHAVIORAL]
- WAL wake handling, explicit sync-waiter cancellation, maintenance polling/capture, maintenance
  result publication validation, worker read completion re-entry, and shutdown acknowledgement MUST
  all route through that same engine shell rather than through separate server-only storage hooks
  [BEHAVIORAL]

Worked example:

```text
Compaction worker finishes before an older flush worker:
  compaction result is not auto-published
  main thread validates its input-generation preconditions
  if stale, main thread rejects it without changing visible state
```

ASCII diagram:

```text
worker result -> main thread validation -> publish or reject
```

Normative pseudocode:

```text
function publish_rule(state, result):
    assert current_thread == main_thread
    if violates_current_preconditions(state, result):
        return Err(Stale)
    apply_visible_state_change(state, result)
    return Ok(())
```

## 4. Server Architecture

### 4.1 Thread roles

The server has two execution roles:

- `main thread`:
  receives bootstrap input and all external RPCs, enforces per-client ordering, performs
  validation, runs lightweight in-memory work, appends WAL records, updates the active memtable,
  allocates snapshots and scan sessions, manages per-scan fetch queues, dispatches background
  tasks, validates worker results, and publishes all shared-state mutations [BEHAVIORAL]
- `bgworker`:
  executes blocking or arbitrarily complex tasks on immutable inputs and returns logical results
  only [BEHAVIORAL]

### 4.2 Lightweight versus delegated work

The main thread MUST directly handle only work whose latency and complexity are bounded by small
in-memory operations over main-thread-owned state. [BEHAVIORAL]

Examples of main-thread work:

- startup config-path validation and engine-open checks [BEHAVIORAL]
- logical-shard metadata lookup [BEHAVIORAL]
- active-memtable point lookup [BEHAVIORAL]
- active-memtable-only scan-page production [BEHAVIORAL]
- active-memtable update for `put` and `delete` [BEHAVIORAL]
- scan-session allocation, fetch-queue admission, and cursor bookkeeping [BEHAVIORAL]
- stale-result checks and publish decisions [BEHAVIORAL]

Examples of delegated work:

- reading frozen memtables [BEHAVIORAL]
- reading or scanning any level files [BEHAVIORAL]
- merging iterators across multiple sources [BEHAVIORAL]
- WAL fsync or equivalent durable-frontier advancement [BEHAVIORAL]
- memtable flush file generation [BEHAVIORAL]
- compaction [BEHAVIORAL]
- checkpoint building [BEHAVIORAL]
- garbage collection of orphan durable artifacts [BEHAVIORAL]

### 4.3 Server lifetime and configuration

The server lifetime is process-scoped rather than RPC-scoped. [BEHAVIORAL]

Bootstrap input:

```text
ServerBootstrapArgs {
    config_path: string
}
```

Rules:

- `config_path` MUST be passed to the server process as a command-line argument [BEHAVIORAL]
- during startup, the main thread MUST call storage-engine `open(config_path)` before the server is
  considered ready [BEHAVIORAL]
- in the Rust binding, `start(args)`, `call(request)`, `shutdown()`, and `wait_stopped()` are
  asynchronous control-plane methods; they MUST preserve the lifecycle rules in this section
  without adding synchronous caller-facing start or wait APIs [BEHAVIORAL]
- the external logical RPC surface MUST NOT expose `Open` or `Close` methods [BEHAVIORAL]
- if startup open fails, the server MUST NOT admit external RPCs for that failed engine instance
  [BEHAVIORAL]
- configuration changes are reopen-only and therefore require server restart with the desired
  `config_path` [BEHAVIORAL]
- server shutdown MUST stop new request admission before releasing scan sessions, worker waiters,
  and the owned engine instance [BEHAVIORAL]
- dropping the last live Rust `PezhaiServer` handle MUST request the same internal shutdown
  cleanup path used by `shutdown()` so detached owner-runtime threads do not outlive the process
  host indefinitely [BEHAVIORAL]
- because handle drop cannot await completion, callers that need confirmation MUST still use
  `shutdown()` followed by `wait_stopped()` [BEHAVIORAL]

ASCII diagram:

```text
argv[--config <path>]
        |
        v
+---------------------------+
| bootstrap / main thread   |
| validate config_path      |
| open_engine(config_path)  |
+---------------------------+
      |               |
      | success       | failure
      v               v
+-------------+   +---------------+
| Ready        |   | StartFailed   |
| serve RPCs   |   | no RPC admit  |
+-------------+   +---------------+
      |
      | shutdown
      v
+---------------------------+
| Stopping                  |
| stop admission            |
| drain or cancel waiters   |
| release snapshots         |
| close owned engine        |
+---------------------------+
```

### 4.4 Asynchronous state model

The server design is intentionally asynchronous in both directions. [BEHAVIORAL]

Rules:

- external logical RPCs are asynchronous request/response interactions identified by
  `client_id` plus `request_id`, not synchronous function-call assumptions [BEHAVIORAL]
- internal communication between the main thread and background workers MUST be modeled as
  asynchronous message passing plus later completion on the main thread [BEHAVIORAL]
- the main thread MAY complete a request immediately, queue it behind earlier work, or hold it
  pending on worker completion as long as logical ordering rules are preserved [BEHAVIORAL]
- `ScanFetchNext` is the canonical externally visible queued operation: same-scan requests may wait
  in a server-owned queue and complete later in FIFO order [BEHAVIORAL]

Behavioral clarification: Global server state

```text
GlobalServerState
|
+-- lifecycle
|   +-- startup_state: Booting | Ready | Stopping | Stopped | StartFailed
|   +-- config_path: string
|   +-- engine_instance_id: u64
|
+-- visibility state
|   +-- last_committed_seqno: u64
|   +-- data_generation: u64
|   +-- current_metadata_map
|   +-- current_logical_shard_map
|   +-- wal_append_cursor
|   +-- wal_durable_frontier
|
+-- read/write structures
|   +-- active_memtable_ref
|   +-- frozen_memtable_registry
|   +-- snapshot_table
|   +-- scan_sessions: map<scan_id, ScanSessionState>
|
+-- async coordination
|   +-- external_request_state: map<(client_id, request_id), ExternalRequestState>
|   +-- worker_task_table: map<task_id, WorkerTaskState>
|   +-- wal_sync_waiters
|   +-- maintenance_retry_state
|
+-- ordering and backpressure
    +-- per_client_request_state
    +-- queue_depth_counters
    +-- configured_limits
```

Behavioral clarification: Request and task state

```text
External request
    |
    +--> RejectedAtAdmission(Busy or InvalidArgument or IO)
    |
    +--> Admitted
          |
          +--> CompletedOnMainThread
          |
          +--> QueuedInScanSession
          |     |
          |     +--> ActiveScanFetch
          |           |
          |           +--> CompletedOnMainThread
          |           |
          |           +--> WaitingForWorkerResult
          |
          +--> WaitingForWorkerResult
                    |
                    +--> MainThreadPublishOrReject
                              |
                              +--> ReplySuccess
                              |
                              +--> ReplyError

Cancellation may transition a queued or pre-publication running request to ReplyError(Cancelled).
```

Behavioral clarification: Scan session state

```text
ScanSessionState
|
+-- identity
|   +-- scan_id
|   +-- snapshot_handle
|   +-- snapshot_seqno
|   +-- data_generation
|
+-- scan definition
|   +-- range
|   +-- max_records_per_page
|   +-- max_bytes_per_page
|   +-- resume_after_key
|
+-- async fetch coordination
|   +-- fetch_request_queue: FIFO<FetchRequestRef>
|   +-- active_fetch_request_or_none
|   +-- in_flight_task_id_or_none
|   +-- terminal_state: Open | EofReached | Expired | Cancelled
|
+-- lifetime
    +-- expires_at
```

ASCII diagram:

```text
client A fetch(scan_id) --\
client B fetch(scan_id) ----> main thread -> session.fetch_request_queue
client C fetch(scan_id) --/                    |
                                               v
                                    dequeue one request
                                               |
                                  main-thread page or worker task
                                               |
                                  update resume_after_key / eof
                                               |
                                  reply to that request only
                                               |
                                    dequeue next waiting request
```

### 4.5 Logical RPC model

This document defines logical RPC only:

- every request has a method name, request id, payload, and optional cancellation metadata
  [BEHAVIORAL]
- every response has the matching request id, terminal status, and optional payload [BEHAVIORAL]
- this specification does not require one byte format for logical compatibility; the repository's
  concrete wire compatibility profile is defined in `docs/specs/tcp-rpc.md` [BEHAVIORAL]

### 4.6 Common status model

Every external and internal logical RPC response uses:

- `status.code`: one of `Ok`, `Busy`, `InvalidArgument`, `IO`, `Checksum`, `Corruption`, `Stale`,
  or `Cancelled` [BEHAVIORAL]
- `status.retryable`: boolean [BEHAVIORAL]
- `status.message`: optional implementation-defined diagnostic string [BEHAVIORAL]

Rules:

- `Busy` is server-defined backpressure and MUST be used only when the server rejects admission
  before a state mutation begins [BEHAVIORAL]
- engine-derived errors MUST reuse the same error-code names as
  `docs/specs/pezhai.md` [BEHAVIORAL]
- `status.retryable` MUST be `true` for `Busy` [BEHAVIORAL]
- `status.retryable` MUST be `false` for `InvalidArgument`, `Checksum`, and `Corruption`
  [BEHAVIORAL]
- `status.retryable` for `IO` and `Cancelled` depends on context as specified later in this
  document [BEHAVIORAL]

## 5. External Logical RPC

### 5.1 Common envelope

All external requests contain:

- `client_id`: opaque stable identifier for one client session or connection [BEHAVIORAL]
- `request_id`: orderable `u64` unique within `client_id` [BEHAVIORAL]
- `method`: one external method defined below [BEHAVIORAL]
- `payload`: method-specific request body [BEHAVIORAL]
- `cancel_token`: optional token that permits cancellation by transport-specific means
  [BEHAVIORAL]

All external responses contain:

- `client_id` [BEHAVIORAL]
- `request_id` [BEHAVIORAL]
- `status` [BEHAVIORAL]
- `payload`: method-specific response body when `status.code = Ok` [BEHAVIORAL]

### 5.2 External method set

The external method set is:

- `Put`
- `Delete`
- `Get`
- `ScanStart`
- `ScanFetchNext`
- `Sync`
- `Stats`

The storage-engine operations `create_snapshot` and `release_snapshot` remain internal-only in v1.
[BEHAVIORAL]

### 5.3 Lifecycle boundary

Rules:

- `Open` and `Close` are not part of the external logical RPC surface [BEHAVIORAL]
- engine open is part of server startup and engine close is part of server shutdown [BEHAVIORAL]
- the configuration file path used for `open(config_path)` comes from process bootstrap, not from
  any external request payload [BEHAVIORAL]
- once the server is in `Ready`, external clients interact only through `Put`, `Delete`, `Get`,
  `ScanStart`, `ScanFetchNext`, `Sync`, and `Stats` [BEHAVIORAL]

### 5.4 `Put`

Request:

```text
PutRequest {
    key: bytes
    value: bytes
}
```

Response:

```text
PutResponse {}
```

Rules:

- `Put` MUST preserve the `put()` semantics defined in
  `docs/specs/pezhai.md`
  [BEHAVIORAL]
- the main thread MUST validate the key and value, append the WAL record, update the active
  memtable, and assign visibility in that order [BEHAVIORAL]
- if the configured sync mode requires durability before acknowledgement, the main thread MAY wait
  on a background `WalSyncTask`, but it MUST NOT acknowledge success before the required durable
  frontier is reached [BEHAVIORAL]

### 5.5 `Delete`

Request:

```text
DeleteRequest {
    key: bytes
}
```

Response:

```text
DeleteResponse {}
```

Rules:

- `Delete` MUST preserve the `delete()` semantics defined in
  `docs/specs/pezhai.md`
  [BEHAVIORAL]
- the main thread MUST validate the key, append the WAL record, and insert the tombstone into the
  active memtable before returning success [BEHAVIORAL]
- if the configured sync mode requires durability before acknowledgement, the same `Put` durability
  rule applies [BEHAVIORAL]

### 5.6 `Get`

Request:

```text
GetRequest {
    key: bytes
}
```

Response:

```text
GetResponse {
    found: bool
    value: bytes?
    observation_seqno: u64
    data_generation: u64
}
```

Rules:

- external `Get` is latest-only and MUST NOT accept an explicit snapshot handle [BEHAVIORAL]
- the main thread MUST resolve the implicit read snapshot at request admission using current
  `last_committed_seqno` and current `data_generation` [BEHAVIORAL]
- the main thread MUST first probe the active memtable for the key [BEHAVIORAL]
- if the active memtable contains the first visible result, the main thread MUST answer without a
  worker hop [BEHAVIORAL]
- otherwise the main thread MUST dispatch a `GetTask` with the pinned snapshot and immutable source
  references taken from that generation [BEHAVIORAL]
- the response MUST include the resolved `observation_seqno` and `data_generation`
  [BEHAVIORAL]

### 5.7 `ScanStart`

Request:

```text
ScanStartRequest {
    start_bound: BoundWire
    end_bound: BoundWire
    max_records_per_page: u32
    max_bytes_per_page: u32
}
```

Response:

```text
ScanStartResponse {
    scan_id: u64
    observation_seqno: u64
    data_generation: u64
}
```

Rules:

- external `ScanStart` is latest-only and MUST NOT accept an explicit snapshot handle
  [BEHAVIORAL]
- `ScanStart` is the external projection of storage-engine `scan()` into a paged RPC protocol
  [BEHAVIORAL]
- the main thread MUST validate the range and page limits [BEHAVIORAL]
- `start_bound` MUST be `NegInf` or `Finite`, `end_bound` MUST be `Finite` or `PosInf`, and the
  half-open range `[start_bound, end_bound)` MUST be non-empty under `compare_bound(...)`;
  otherwise `ScanStart` MUST fail with `InvalidArgument` [BEHAVIORAL]
- `max_records_per_page` and `max_bytes_per_page` MUST both be greater than zero; otherwise
  `ScanStart` MUST fail with `InvalidArgument` [BEHAVIORAL]
- the main thread MUST create one internal snapshot handle and one scan session before returning
  success [BEHAVIORAL]
- the scan session MUST pin exactly one `snapshot_seqno` and one `data_generation` for the full
  lifetime of the session [BEHAVIORAL]
- `scan_id` is a server-allocated session handle and MUST NOT be scoped to one originating
  `client_id` [BEHAVIORAL]
- `ScanStart` MUST NOT return user rows; row delivery begins with `ScanFetchNext` [BEHAVIORAL]

### 5.8 `ScanFetchNext`

Request:

```text
ScanFetchNextRequest {
    scan_id: u64
}
```

Response:

```text
ScanFetchNextResponse {
    rows: list of { key: bytes, value: bytes }
    eof: bool
}
```

Rules:

- `ScanFetchNext` MUST read from the scan session created by `ScanStart` [BEHAVIORAL]
- `ScanFetchNext` for one `scan_id` MAY be issued by arbitrary clients that know the `scan_id`
  [BEHAVIORAL]
- the main thread MUST allow at most one active page-production step per `scan_id`
  [BEHAVIORAL]
- if additional `ScanFetchNext` requests for the same `scan_id` arrive while one request is active,
  the main thread MUST enqueue them in the scan session's FIFO fetch queue instead of returning
  `Busy` solely for that reason [BEHAVIORAL]
- queued `ScanFetchNext` requests for the same `scan_id` MUST be processed one by one in queue
  order [BEHAVIORAL]
- if one page can be produced from the pinned active memtable alone, the main thread MAY answer
  the active queued `ScanFetchNext` directly without worker dispatch [BEHAVIORAL]
- otherwise the main thread MUST dispatch a `ScanPageTask` that uses the session's internal
  snapshot handle and pinned manifest generation [BEHAVIORAL]
- every successful non-EOF `ScanFetchNext` response MUST contain at least one row; if the first
  remaining visible row alone exceeds `max_bytes_per_page`, the server MUST return that row as a
  single-row page and MAY exceed the byte limit for that response so the scan still makes forward
  progress [BEHAVIORAL]
- completion of one `ScanFetchNext` MUST update `resume_after_key` before the main thread begins
  the next queued fetch for that `scan_id` [BEHAVIORAL]
- if EOF closes a scan session while later `ScanFetchNext` requests remain queued for that
  `scan_id`, the main thread MUST fail those queued requests before deleting the session
  [BEHAVIORAL]
- when `eof = true`, the main thread MUST release the internal snapshot handle and delete the scan
  session before replying success [BEHAVIORAL]

### 5.9 `Sync`

Request:

```text
SyncRequest {}
```

Response:

```text
SyncResponse {
    durable_seqno: u64
}
```

Rules:

- `Sync` MUST preserve the storage-engine `sync()` semantics [BEHAVIORAL]
- the main thread MUST translate `Sync` into one durability target on the current WAL append
  frontier [BEHAVIORAL]
- `SyncResponse.durable_seqno` MUST be the greatest committed seqno whose WAL record is guaranteed
  durable at reply time; if no committed seqno is yet durable, it MUST be `0` [BEHAVIORAL]
- the blocking durable-frontier advancement work MUST execute on the engine-owned dedicated WAL
  sync thread, not through the generic worker pool [BEHAVIORAL]
- multiple waiting writes and explicit `Sync` requests MAY share one group-commit batch when their
  required durable frontier overlaps [BEHAVIORAL]
- if the current durable frontier already covers the requested seqno, the main thread MUST reply
  immediately without dispatching new WAL sync work [BEHAVIORAL]

### 5.10 `Stats`

Request:

```text
StatsRequest {}
```

Response:

```text
StatsResponse {
    observation_seqno: u64
    data_generation: u64
    levels: LevelStats[]
    logical_shards: LogicalShardStats[]
}
```

Rules:

- `Stats` MUST preserve the storage-engine `stats()` semantics [BEHAVIORAL]
- the main thread MUST answer `Stats` directly from current owned state [BEHAVIORAL]
- `Stats` MUST report the latest current logical-shard map, not a historical read snapshot
  [BEHAVIORAL]

## 6. Internal Logical RPC

### 6.1 Common envelope

All internal messages contain:

- `task_id`: unique within one open engine instance [BEHAVIORAL]
- `origin`: `external_request`, `automatic_maintenance`, or `internal_control` [BEHAVIORAL]
- `origin_request_ref`: optional `(client_id, request_id)` for correlation with asynchronous
  external work [BEHAVIORAL]
- `cancel_token`: optional [BEHAVIORAL]
- `payload`: one exact message type defined below [BEHAVIORAL]

Internal responses contain:

- `task_id` [BEHAVIORAL]
- `status` [BEHAVIORAL]
- `payload`: one exact result type defined below when `status.code = Ok` [BEHAVIORAL]

### 6.2 Main-thread internal commands

The main thread accepts these internal control commands:

- `CreateSnapshotCmd`
- `ReleaseSnapshotCmd`
- `CancelTaskCmd`

Definitions:

```text
CreateSnapshotCmd {}

CreateSnapshotResult {
    handle: SnapshotHandle
}

ReleaseSnapshotCmd {
    handle: SnapshotHandle
}

ReleaseSnapshotResult {}

CancelTaskCmd {
    task_id: u64
    reason: string?
}

CancelTaskResult {}
```

Rules:

- `CreateSnapshotCmd` and `ReleaseSnapshotCmd` execute on the main thread only [BEHAVIORAL]
- `CreateSnapshotCmd` and `ReleaseSnapshotCmd` MUST preserve the storage-engine
  `create_snapshot()` and `release_snapshot()` semantics exactly [BEHAVIORAL]
- `CancelTaskCmd` marks queued work cancelled immediately and marks running work cancelled on a
  best-effort basis [BEHAVIORAL]

### 6.3 Worker task set

The exact worker task set is:

- `GetTask`
- `ScanPageTask`
- `FlushTask`
- `CompactTask`
- `CheckpointTask`
- `GcTask`

#### 6.3.1 `GetTask`

```text
GetTask {
    key: bytes
    snapshot_seqno: u64
    data_generation: u64
    frozen_memtable_refs: FrozenMemtableRef[]
    candidate_files: DataFileRef[]
}

GetTaskResult {
    found: bool
    value: bytes?
}
```

Rules:

- `GetTask` MUST NOT probe the active memtable [BEHAVIORAL]
- `GetTask` MUST search only the immutable sources named in the task payload [BEHAVIORAL]
- the server MUST treat `GetTask` input as an opaque engine-owned point-read plan rather than
  rebuilding candidate files or frozen-source lists itself [BEHAVIORAL]

#### 6.3.2 `ScanPageTask`

```text
ScanPageTask {
    scan_id: u64
    snapshot_handle: SnapshotHandle
    range: KeyRange
    resume_after_key: bytes?
    max_records_per_page: u32
    max_bytes_per_page: u32
    active_memtable_ref: ActiveMemtableRef?
    frozen_memtable_refs: FrozenMemtableRef[]
    candidate_files: DataFileRef[]
}

ScanPageTaskResult {
    rows: list of { key: bytes, value: bytes }
    eof: bool
    next_resume_after_key: bytes?
}
```

Rules:

- `ScanPageTask` MAY include the active memtable because the session-pinned generation already
  turns it into an immutable read source for that snapshot [BEHAVIORAL]
- `ScanPageTask` MUST merge all named sources using the pinned snapshot rules from the storage
  engine [BEHAVIORAL]
- the server MUST treat `ScanPageTask` input as an opaque engine-owned range-scan plan rather than
  deriving its own latest-read view or immutable-source set [BEHAVIORAL]
- `ScanPageTaskResult.rows` MUST be non-empty whenever `eof = false` [BEHAVIORAL]
- `ScanPageTaskResult.next_resume_after_key` MUST be the greatest emitted key of the page when
  `eof = false` [BEHAVIORAL]

#### 6.3.3 Dedicated WAL sync thread

```text
WalSyncRequest {
    wal_segment_id: u64
    durable_offset_target: u64
    durable_seqno_target: u64
}

WalSyncBatchResult {
    wal_segment_id: u64
    durable_offset_reached: u64
    durable_seqno_target: u64
}
```

Rules:

- the dedicated WAL sync thread advances durability only for bytes already appended by the main
  thread [BEHAVIORAL]
- the dedicated WAL sync thread MUST NOT change the WAL append cursor, seqno allocator, rollover
  state, or published durable frontier directly [BEHAVIORAL]
- the main thread remains responsible for durable-frontier publication and waiter release after one
  sync-thread result is received [BEHAVIORAL]

#### 6.3.4 `FlushTask`

```text
FlushTask {
    source_generation: u64
    source_frozen_memtable_id: u64
    source_memtable_ref: FrozenMemtableRef
    output_file_ids: u64[]
}

FlushTaskResult {
    source_generation: u64
    source_frozen_memtable_id: u64
    outputs: DataFileMeta[]
}
```

Rules:

- `FlushTask` prepares flush output files but MUST NOT publish them [BEHAVIORAL]
- the captured task input MUST identify one immutable frozen-memtable source without borrowing the
  live mutable engine state; implementations MAY satisfy that by sharing one immutable reference or
  by any equivalent immutable handle [BEHAVIORAL]
- the main thread MUST validate `source_generation` and `source_frozen_memtable_id` before
  publication [BEHAVIORAL]

#### 6.3.5 `CompactTask`

```text
CompactTask {
    source_generation: u64
    input_file_ids: u64[]
    input_files: DataFileRef[]
    output_file_ids: u64[]
}

CompactTaskResult {
    source_generation: u64
    input_file_ids: u64[]
    outputs: DataFileMeta[]
}
```

Rules:

- `CompactTask` prepares replacement output files for one exact input set [BEHAVIORAL]
- the main thread MUST validate the exact input file set against the current generation before
  publication [BEHAVIORAL]

#### 6.3.6 `CheckpointTask`

```text
CheckpointTask {
    checkpoint_max_seqno: u64
    manifest_generation: u64
}

CheckpointTaskResult {
    checkpoint_max_seqno: u64
    manifest_generation: u64
    checkpoint_file: MetadataCheckpointMeta
}
```

Rules:

- `CheckpointTask` prepares durable checkpoint artifacts only [BEHAVIORAL]
- `CURRENT` update or other publication steps remain main-thread work [BEHAVIORAL]

#### 6.3.7 `GcTask`

```text
GcTask {
    deletable_files: DurableFileRef[]
}

GcTaskResult {
    deleted_files: DurableFileRef[]
}
```

Rules:

- `GcTask` MUST operate only on files the main thread has already marked safe to delete
  [BEHAVIORAL]
- failure to delete one file MUST NOT mutate shared visibility state [BEHAVIORAL]

## 7. Request Routing Rules

### 7.1 External request admission

- the main thread MUST admit or reject every external request [BEHAVIORAL]
- the server MUST NOT admit external requests until startup open has succeeded and the server is in
  `Ready` state [BEHAVIORAL]
- if the server is not in `Ready` state, every external method MUST fail before worker dispatch
  [BEHAVIORAL]
- requests received while lifecycle state is `Booting` or `Stopping` MUST fail with `IO` and
  `retryable = true`; requests received while lifecycle state is `StartFailed` or `Stopped` MUST
  fail with `IO` and `retryable = false` [BEHAVIORAL]
- admission rejection due to bounded queue or resource limits MUST use `Busy`
  with `retryable = true` [BEHAVIORAL]

### 7.2 `Get` routing

Normative pseudocode:

```text
function handle_get(state, request):
    validate_key(request.key)
    snapshot = {
        snapshot_seqno = state.last_committed_seqno,
        data_generation = state.data_generation
    }

    record = find_visible_record_in_one_memtable(
        state.current_data_manifest.active_memtable_ref,
        request.key,
        snapshot.snapshot_seqno)
    if record != None:
        return Ok(visible_result(record), snapshot)

    task = build_get_task_from_generation(state, request.key, snapshot)
    enqueue_worker_task(task)
    return wait_for_worker_and_reply(task, snapshot)
```

### 7.3 `ScanStart` and `ScanFetchNext` routing

Rules:

- `ScanStart` MUST allocate one `scan_id` and one internal snapshot handle [BEHAVIORAL]
- the scan session state MUST contain:
  - `scan_id`
  - `snapshot_handle`
  - `snapshot_seqno`
  - `data_generation`
  - `range`
  - `max_records_per_page`
  - `max_bytes_per_page`
  - `resume_after_key`
  - `fetch_request_queue`
  - `active_fetch_request_or_none`
  - `in_flight_task_id_or_none`
  - `terminal_state`
  - `expires_at`
  [BEHAVIORAL]
- `ScanFetchNext` MUST dispatch work against the session-pinned snapshot, not the latest current
  state [BEHAVIORAL]
- `ScanFetchNext` requests for one `scan_id` MUST be enqueued and serialized by the main thread
  even when they arrive from different clients [BEHAVIORAL]
- `ScanFetchNext` MAY complete on the main thread when the pinned generation can satisfy the page
  from the active memtable alone [BEHAVIORAL]

Normative pseudocode:

```text
function handle_scan_start(state, request):
    validate_scan_range_and_page_limits(request)
    snapshot = create_snapshot(state)
    scan_id = state.next_scan_id
    state.next_scan_id <- state.next_scan_id + 1
    state.scan_sessions[scan_id] <- {
        scan_id = scan_id,
        snapshot_handle = snapshot,
        snapshot_seqno = snapshot.snapshot_seqno,
        data_generation = snapshot.data_generation,
        range = [request.start_bound, request.end_bound),
        max_records_per_page = request.max_records_per_page,
        max_bytes_per_page = request.max_bytes_per_page,
        resume_after_key = None,
        fetch_request_queue = empty_fifo(),
        active_fetch_request_or_none = None,
        in_flight_task_id_or_none = None,
        terminal_state = Open,
        expires_at = compute_scan_expiry()
    }
    return Ok({
        scan_id = scan_id,
        observation_seqno = snapshot.snapshot_seqno,
        data_generation = snapshot.data_generation
    })

function handle_scan_fetch_next(state, request):
    session = lookup_scan_session(state, request.scan_id)
    enqueue(session.fetch_request_queue, request_ref(request))
    maybe_start_next_scan_fetch(state, session)

function maybe_start_next_scan_fetch(state, session):
    if session.active_fetch_request_or_none != None:
        return

    request = dequeue_first_live_request(session.fetch_request_queue)
    if request == None:
        return

    session.active_fetch_request_or_none <- request

    page_or_plan = engine_execute_scan_operation(state.engine, session)
    if page_or_plan == complete_page:
        finish_scan_fetch_request(state, session, complete_page)
        return

    task = extract_scan_page_task(page_or_plan)
    session.in_flight_task_id_or_none <- task.task_id
    enqueue_worker_task(task)
```

Behavioral clarification:

```text
finish_scan_fetch_request(state, session, page_or_worker_result):
    apply_page_result_to_session(session, page_or_worker_result)
    reply_to(session.active_fetch_request_or_none, page_or_worker_result)
    session.active_fetch_request_or_none <- None
    session.in_flight_task_id_or_none <- None

    if page_or_worker_result.eof:
        session.terminal_state <- EofReached
        fail_remaining_fetch_requests(session.fetch_request_queue, InvalidArgument)
        release_snapshot(state, session.snapshot_handle)
        delete state.scan_sessions[session.scan_id]
        return

    maybe_start_next_scan_fetch(state, session)
```

### 7.4 `Sync` routing

Rules:

- the main thread MUST register one waiter keyed by durable seqno frontier, not by one permanent
  segment identity [BEHAVIORAL]
- if the current durable frontier already covers the target seqno, the request MUST complete
  immediately without new dispatch [BEHAVIORAL]
- the server MUST use the engine-owned `sync` operation path to create or complete the waiter; it
  MUST NOT keep a separate server-only sync-target planner [BEHAVIORAL]
- otherwise the main thread MUST issue one request to the dedicated WAL sync thread and wait for a
  returned frontier that covers the request [BEHAVIORAL]
- multiple waiting writes and explicit `Sync` requests MAY share one sync-thread batch when their
  required durable frontier overlaps [BEHAVIORAL]
- the sync thread MUST flush when either the unsynced bytes since the last published durable
  frontier reach `wal.group_commit_bytes`, or the oldest pending waiter reaches
  `wal.group_commit_max_delay_ms`, whichever happens first [BEHAVIORAL]
- when the sync thread returns success, the main thread MUST advance the durable frontier and
  release all satisfied waiters [BEHAVIORAL]

### 7.5 Write routing

Rules:

- `Put` and `Delete` MUST allocate seqnos and append WAL records on the main thread
  [BEHAVIORAL]
- the main thread MUST update the active memtable before returning success [BEHAVIORAL]
- in `PerWrite` mode, the main thread MUST register a durability waiter after the WAL append and
  MUST acknowledge only after the dedicated WAL sync thread reports a covering durable frontier
  [BEHAVIORAL]
- if thresholds are crossed, the main thread MAY freeze the active memtable and enqueue background
  maintenance after the write becomes visible [BEHAVIORAL]

## 8. Background Maintenance

### 8.1 Dispatch rules

Automatic maintenance includes:

- memtable flush [BEHAVIORAL]
- compaction [BEHAVIORAL]
- checkpoint creation [BEHAVIORAL]
- garbage collection [BEHAVIORAL]

The main thread decides when to dispatch these tasks. [BEHAVIORAL]

### 8.2 Publication rules

Rules:

- a worker-prepared flush result MUST return to the main thread as `FlushTaskResult`
  [BEHAVIORAL]
- a worker-prepared compaction result MUST return to the main thread as `CompactTaskResult`
  [BEHAVIORAL]
- the main thread MUST append any required publish WAL record before installing the new visible
  generation [BEHAVIORAL]
- the main thread MUST reject stale maintenance results using the same source-generation or exact
  input-set checks required by the storage-engine specification [BEHAVIORAL]

### 8.3 Retry rules

- recoverable worker `IO` during externally visible `Get` or `ScanFetchNext` MUST be returned to
  the caller with `status.code = IO` and `status.retryable = true` [BEHAVIORAL]
- WAL sync-thread `IO` for one in-flight batch MUST fail every affected waiter in that batch and
  MUST place the live engine instance into the same write-stopped state used for fatal append-path
  failure until the next successful restart [BEHAVIORAL]
- recoverable worker `IO` during automatic flush, compaction, checkpoint, or garbage collection
  MUST be retried internally with bounded backoff and MUST NOT surface as an external admin RPC
  failure because no such external admin RPC exists in v1 [BEHAVIORAL]
- if a live append-path `IO` forces the engine into a write-stopped state under the storage-engine
  rules, the main thread MUST stop admitting new writes until the next successful server restart
  that reopens the engine [BEHAVIORAL]

Worked example:

```text
Background compaction hits one transient read IO:
  worker returns IO
  main thread records the attempt
  main thread schedules a retry later
  no external client sees a compaction RPC failure
```

## 9. Ordering, Cancellation, and Backpressure

### 9.1 Per-client ordering

Rules:

- the first admitted request for one `client_id` establishes that client's starting `request_id`
  [BEHAVIORAL]
- later requests from the same `client_id` MUST use exactly the next greater `request_id`
  [BEHAVIORAL]
- duplicate, regressing, or gap-creating `request_id` values MUST be rejected with
  `InvalidArgument` [BEHAVIORAL]
- responses for one `client_id` MUST be emitted in request order [BEHAVIORAL]
- serialization of same-`scan_id` `ScanFetchNext` requests is defined by scan-session queue order
  rather than by client identity [BEHAVIORAL]
- cross-client response order is unconstrained except where global storage-engine semantics require
  one seqno order for writes [BEHAVIORAL]

### 9.2 Cancellation

Rules:

- every external request MAY carry cancellation metadata [BEHAVIORAL]
- every internal task MAY carry cancellation metadata derived from the external request or internal
  control path [BEHAVIORAL]
- queued tasks cancelled before start MUST return `Cancelled` without executing worker logic
  [BEHAVIORAL]
- running tasks MUST observe cancellation on a best-effort basis between expensive steps
  [BEHAVIORAL]
- queued explicit `Sync` waiters MAY be cancelled before they become durable [BEHAVIORAL]
- once an operation has crossed an externally visible or durability-relevant point, cancellation
  MUST NOT roll back its effects; the server MAY drop response delivery to the cancelled client, but
  the internal operation MUST run to its normal publish or completion point [BEHAVIORAL]
- `Put` and `Delete` durability waiters MUST NOT be cancelled after the WAL append that defines the
  visible write seqno has completed [BEHAVIORAL]
- if a client disconnects, the main thread MUST cancel that client's queued tasks and queued
  external requests immediately, and MUST best-effort cancel that client's active external requests
  only when they have not yet crossed an externally visible or durability-relevant point
  [BEHAVIORAL]
- disconnect of one client MUST NOT by itself cancel a scan session that may continue through
  `ScanFetchNext` requests from other clients [BEHAVIORAL]
- cancellation of one queued `ScanFetchNext` request MUST remove only that request from the
  session queue when possible [BEHAVIORAL]
- cancellation of a whole scan session, including expiry or shutdown cleanup, MUST release its
  internal snapshot handle [BEHAVIORAL]

### 9.3 Backpressure

The server MUST enforce finite implementation-defined limits for:

- total queued external requests [BEHAVIORAL]
- total queued worker tasks [BEHAVIORAL]
- total active scan sessions [BEHAVIORAL]
- total in-flight page tasks [BEHAVIORAL]
- total queued `ScanFetchNext` requests per scan session [BEHAVIORAL]
- total waiting WAL sync requests [BEHAVIORAL]

Rules:

- if admitting one request would exceed a relevant limit, the main thread MUST reject it with
  `Busy` before mutating visible state [BEHAVIORAL]
- limits MAY differ by method type [BEHAVIORAL]
- `ScanStart` SHOULD be rejected before snapshot creation if the scan-session table is full
  [BEHAVIORAL]

## 10. Error Mapping

### 10.1 External error rules

- `InvalidArgument`, `Checksum`, and `Corruption` MUST be returned with `retryable = false`
  [BEHAVIORAL]
- `Busy` MUST be returned with `retryable = true` [BEHAVIORAL]
- user-request `IO` caused by worker execution of `Get`, `ScanFetchNext`, or `Sync` MUST be
  returned with `retryable = true` [BEHAVIORAL]
- `IO` caused only by server lifecycle state `Booting` or `Stopping` MUST use `retryable = true`
  [BEHAVIORAL]
- `IO` caused by lifecycle state `StartFailed` or `Stopped` MUST use `retryable = false`
  [BEHAVIORAL]
- `Cancelled` from client-driven or deadline-driven cancellation SHOULD use `retryable = true`
  [BEHAVIORAL]
- unknown or expired `scan_id` on `ScanFetchNext` SHOULD be returned as `InvalidArgument`
  [BEHAVIORAL]
- `Stale` SHOULD remain internal for automatic maintenance and SHOULD NOT be exposed by external v1
  methods [BEHAVIORAL]

### 10.2 Internal error rules

- `Stale` is a normal internal outcome for flush, compaction, and other publish candidates whose
  preconditions no longer match current state [BEHAVIORAL]
- internal `Busy` indicates queue saturation only and MUST NOT partially publish work
  [BEHAVIORAL]
- internal `Cancelled` means either admission-time cancellation or best-effort task interruption
  [BEHAVIORAL]

## 11. Acceptance Criteria

An implementation conforms to this specification only if all of the following hold:

1. all shared mutable engine state is mutated only by the main thread [BEHAVIORAL]
2. server startup takes one configuration-file path from the command line, opens the engine before
   serving requests, and exposes no external `Open` or `Close` RPC [BEHAVIORAL]
3. external `Get` resolves latest-only reads and can complete on the main thread for active-
   memtable hits [BEHAVIORAL]
4. external `Get` delegates to `GetTask` when frozen memtables or files are needed [BEHAVIORAL]
5. external `ScanStart` creates one internal snapshot and external `ScanFetchNext` reuses it for
   the full scan session [BEHAVIORAL]
6. external `ScanFetchNext` requests for the same `scan_id` are queued and processed one by one
   even when they arrive from arbitrary clients [BEHAVIORAL]
7. external scan sessions release their internal snapshot on EOF, cancellation, expiry, and server
   shutdown cleanup [BEHAVIORAL]
8. workers never publish flush, compaction, checkpoint, WAL durable-frontier, or metadata changes
   directly [BEHAVIORAL]
9. `Put` and `Delete` acknowledgement obey the configured sync-mode durability rules
   [BEHAVIORAL]
10. recoverable worker `IO` for user-visible reads or sync is surfaced externally as retryable
   `IO` [BEHAVIORAL]
11. recoverable worker `IO` for automatic maintenance is retried internally [BEHAVIORAL]
12. bounded queues cause admission-time `Busy` rather than unbounded growth [BEHAVIORAL]
13. per-client request ordering is enforced by the main thread while same-scan fetch ordering is
    enforced by per-scan queues [BEHAVIORAL]

Required test categories:

- active-memtable `Get` hit returns without worker dispatch
- `Get` miss in active memtable dispatches one `GetTask`
- server startup reads config path from command line and rejects serving when open fails
- `ScanStart` allocates one internal snapshot and `ScanFetchNext` pages through a stable view
- concurrent `ScanFetchNext` requests on the same `scan_id` from different clients are queued and
  processed one by one
- cancellation of one queued `ScanFetchNext` removes only that queued request
- scan expiry or shutdown cleanup releases the internal snapshot
- worker `FlushTaskResult` is rejected as `Stale` when source generation changes before publish
- worker `CompactTaskResult` is rejected as `Stale` when exact input files no longer match
- shared `WalSyncTask` satisfies multiple waiters
- backpressure rejects requests with `Busy` before state mutation
- client disconnect cancels that client's queued work without destroying a shared scan session
