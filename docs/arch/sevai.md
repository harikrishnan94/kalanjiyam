## Pezhai Server Architecture

This architecture note describes the runtime layers that back the scaffolded
`pezhai::sevai::PezhaiServer` handle, the stub in-memory runtime, and the
binary-owned raw TCP adapter.

The workspace split keeps reusable server code in the `pezhai` library crate
under the `pezhai::sevai` module. The `pezhai-sevai` package parses
`--config <path>`, starts the shared runtime, and owns process lifetime wiring
plus the TCP/protobuf transport binding.

### Bootstrapping and Config

The `pezhai-sevai` binary is a thin Tokio program that accepts
`--config <path>` and passes that path to `pezhai::sevai::PezhaiServer`.
Config parsing remains library-owned so bootstrap validation stays consistent
between tests and the executable package. The parser consumes the existing
`[engine]`, `[wal]`, and `[lsm]` tables verbatim so future integration will be
compatible, then requires the `[sevai].listen_addr` field. The TCP adapter
in `pezhai-sevai` binds the `listen_addr` before admitting RPCs, so startup
fails fast when the address is malformed or already in use.

### Handle and Ownership Boundaries

`PezhaiServer` is a cloneable async handle whose clones share an unbounded async
channel to a single background owner actor. That channel acts as the owner
actor's mailbox. The owner actor owns all mutable state, per-client ordering
information, scan sessions, and the stubbed data map. Clients interact through
the handle methods `start`, `call`, `cancel`, `shutdown`, and `wait_stopped`,
ensuring that all effects funnel through one owner and that the boundary
between handles and the owner actor remains the long-term seam. When the TCP
adapter owned by `pezhai-sevai` is active, `wait_stopped` also waits for the
listener task and any live connection handlers to unwind before reporting that
shutdown is complete.

The long-term actor services are the owner actor, a worker actor pool for
immutable blocking jobs, and a dedicated WAL sync actor for durability batches.
Handles submit work only through the owner actor mailbox, which preserves the
single serialized publication boundary defined in `docs/specs/sevai.md`.
The owner actor does not write transport bytes directly; each adapter-owned
connection handler encodes the response it receives and writes it back to the
socket after the logical request completes.

### Current Scaffold Runtime Flow

The current scaffold keeps ordinary request execution inside the owner actor and
the adapter-owned connection handlers around it. Tokio may schedule those
handlers on any runtime worker, so the flow below is task-oriented rather than
thread-oriented. The owner actor is still the only component that mutates
shared server state.

```text
transport peer
    |
    v
+---------------------------+
| TCP connection handler    | ----------------------------------------+
| - read one frame          |     mpsc::UnboundedSender<OwnerCommand> |
| - decode RequestEnvelope  |                                         |
| - call server.call()      |                                         |
| - await oneshot reply     | <------------------------------------+  |
| - encode ResponseEnvelope |      oneshot::Sender<ExternalResponse> |  |
| - write response bytes    |                                      |  |
+---------------------------+                                      |  |
    |                                                              |  |
    v                                                              |  |
transport peer                                                     |  |
                                                                   |  |
                     +---------------------------------------------+  |
                     |                                                |
                     v                                                |
          +-------------------------------+                           |
          | owner actor                   |                           |
          | - recv mailbox commands       |                           |
          | - validate and mutate state   |                           |
          | - queue and complete scans    |                           |
          | - release replies in          |                           |
          |   per-client request order    |                           |
          +-------------------------------+                           |
                     ^                                                |
                     |                                                |
                     +---- CompleteScanFetch deferred re-entry -------+
                          from scan-fetch timer task
```

Shutdown still has two distinct wait points. `shutdown()` sends
`OwnerCommand::Shutdown` through the owner mailbox. The TCP adapter uses
`wait_owner_stopped()` to stop accepting new work and unwind live connection
tasks, while `wait_stopped()` resolves only after the owner completion result is
available and all registered runtime tasks have finished.

### Conceptual Actor Topology

The longer-term architecture remains actor-oriented even though the current
scaffold does not yet route ordinary requests through worker actors or the WAL
sync actor.

```text
external handle or transport adapter
                |
                v
      +----------------------+
      | owner actor          |
      | - sole publish point |
      | - owns mutable state |
      +----------------------+
         |               ^
         | owner-issued  | results validated and published
         v               |
 +------------------+    |
 | worker actor     |----+
 | pool             |
 | - immutable jobs |
 | - read/prepare   |
 +------------------+

         | owner-issued durability batches
         v
 +------------------+
 | WAL sync actor   |
 | - durability     |
 | - owner-requested|
 | - no publish     |
 +------------------+
```

The first diagram is implementation-accurate for the current scaffold in
`crates/pezhai/src/sevai.rs` and `crates/pezhai-sevai/src/adapter.rs`. The
second diagram is conceptual architecture: it describes the intended actor
decomposition and publication boundary without claiming that the current code
already routes normal requests through a worker actor pool or a WAL sync actor.

### Logical RPC Model

Logical request and response structs mirror the RPC surface defined in
`docs/specs/sevai.md`. Requests carry `client_id`, `request_id`, and optional
`cancel_token`. The owner actor enforces per-client request order, rejects
duplicates/regressions, queues `ScanFetchNext` operations per scan session, and
tracks cancellation state for queued work. Responses return the mapped `Status`
plus method payloads, and `cancel` can mark queued tasks before they run while
leaving already-complete, non-cancellable requests untouched.

### Stub Runtime Behavior

The owner actor keeps a stable snapshot of the keyspace in an in-memory
`BTreeMap<Vec<u8>, Vec<u8>>`. Mutations increment `last_committed_seqno`, and
each response reflects the latest `observation_seqno` and `data_generation`.
`Get` returns the latest values without spawning workers; `Put`/`Delete` mutate
the map immediately. `ScanStart` eagerly captures matching rows into a scan
session structure (range bounds, resume key, page limits, captured snapshot
metadata, and queued fetch state) and returns a `scan_id`. `ScanFetchNext`
pages through that captured row set, guaranteeing at least one row per non-EOF
reply. EOF deletes the session; cancellation removes only the cancelled queued
request or returns a cancelled response for the active fetch while leaving the
session available for later fetches. `Stats` reports the owner
seqno/generation plus stub empty level stats with one logical shard entry
covering the whole space.

### Current Scaffold Deviations From Spec

The current scaffold is intentionally narrower than `docs/specs/sevai.md`.

- The current request path is a stub owner loop over in-memory state. It does
  not yet route storage operations through the engine-owned internal operation
  shell described in the spec.
- The conceptual worker actor pool and dedicated WAL sync actor are not yet in
  the live request path. Ordinary `Get`, `ScanFetchNext`, and `Stats` work stay
  inside the owner actor plus adapter-owned runtime tasks.
- `ScanStart` captures rows eagerly into the session instead of creating the
  internal snapshot handle and delegated scan task model described by the spec.
- Write acknowledgement does not yet model `sync_mode`-driven WAL append,
  durable-frontier tracking, or dedicated WAL sync batching.
- Cancelling one `ScanFetchNext` request does not close the whole session in the
  scaffold. Session deletion happens on EOF or shutdown cleanup, not on
  per-request cancellation alone.

### TCP Adapter

The TCP adapter lives in the `pezhai-sevai` package and runs inside the same
Tokio runtime as the owner actor. `pezhai-sevai` owns protobuf generation from
`crates/pezhai-sevai/proto/sevai.proto` and includes those generated wire types
for frame decoding and encoding. The adapter listens on the
`[sevai].listen_addr` endpoint and performs framed exchanges: each loop
iteration reads 4 big-endian bytes, then that many protobuf payload bytes,
decodes a `RequestEnvelope`, maps it to a logical request, and dispatches the
work through the shared server handle. Each connection handler processes one
request/response cycle at a time and does not read the next frame until it has
written the current response back to the socket. During shutdown, the adapter
stops accepting new connections, lets live handlers finish the request they are
already processing, and prevents those handlers from reading any further work
before `wait_stopped` observes full shutdown completion.
Errors from malformed frames or bytes are returned as transport-level failures
before reaching the logical layer.

This layered architecture keeps the transport adapter thin, preserves the
single owner-actor boundary, and records the scaffolded in-memory behavior that
future realistic storage engines will replace.
