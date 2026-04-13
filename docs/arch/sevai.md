## Pezhai Server Architecture

This document is the architecture source of truth for the asynchronous
`pezhai::sevai` runtime and the `pezhai-sevai` binary package.

The server remains transport-agnostic inside the `pezhai` library crate.
`pezhai-sevai` owns only process bootstrap plus the TCP/protobuf adapter.

### Package boundaries

- `crates/pezhai/src/sevai.rs` owns the public server handle, owner-actor
  state, transport-agnostic request and response types, and shutdown
  coordination.
- `crates/pezhai-sevai/src/main.rs` owns `--config <path>` bootstrap and
  process lifetime wiring.
- `crates/pezhai-sevai/src/adapter.rs` owns the raw TCP listener, framed
  protobuf decoding and encoding, and the mapping to the transport-agnostic
  server API.

### Control-plane model

The server lifetime is process-scoped rather than RPC-scoped.

Boot sequence:

1. Parse `--config <path>`.
2. Load and validate the shared `config.toml`.
3. Start one owner actor.
4. Bind the TCP adapter to `[sevai].listen_addr`.
5. Admit external RPCs only after the owner runtime is ready.

Shutdown sequence:

1. Stop admitting new requests.
2. Fail or drain queued waiters according to the server spec.
3. Stop accepting new TCP work.
4. Wait for the owner task and registered runtime tasks to unwind.

### Ownership boundaries

The owner actor is the only component allowed to mutate shared server state.
That state currently includes:

- request-order tracking keyed by `client_id`
- scan-session tables and per-scan FIFO fetch queues
- current stats view published to `Stats`
- the temporary in-memory key/value state used by the milestone-1 runtime

No transport task mutates shared state directly. The TCP adapter decodes one
request, submits it through the `PezhaiServer` handle, waits for one logical
response, then writes one framed reply.

### Config-driven limits

The shared `config.toml` file includes a `[server_limits]` table.

The current server implementation consumes these limits directly:

- `max_pending_requests`
- `max_scan_sessions`
- `max_scan_fetch_queue_per_session`

The remaining server-limit fields are part of the stable config contract now
and are reserved for later worker-pool, durability-waiter, and scan-expiry
milestones.

### Relationship to the engine core

The storage engine foundation now lives under the `pezhai` crate's internal
modules:

- `iyakkam`: public engine shell and shared operation-shape helpers
- `nilaimai`: mutable engine state
- `pathivu`: WAL and checksum helpers
- `idam`: durable path layout helpers
- `pani`: maintenance-planning boundary

Milestone 1 does not yet route normal server requests through the direct
`PezhaiEngine` shell. The current owner actor still serves requests from its
own in-memory state while the durable engine modules are established and tested.

That temporary split is intentional:

- the public server API stays stable
- the new engine API and config surface land without changing the TCP wire
- later milestones can move request planning into `iyakkam` without replacing
  the transport adapter or the control-plane lifecycle

### Current execution model

The runtime still uses one owner task plus adapter-owned connection tasks.

```text
TCP peer
  -> adapter connection task
  -> PezhaiServer handle
  -> owner actor
  -> logical response
  -> adapter connection task
  -> TCP peer
```

For milestone 1, request execution remains owner-local and in-memory.
Background worker pools, WAL sync batching, delegated file reads, and durable
maintenance publication are planned next but are not active yet.
