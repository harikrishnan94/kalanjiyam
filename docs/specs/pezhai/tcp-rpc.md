# Pezhai TCP RPC Transport Specification

This document defines the concrete proto3-over-TCP transport binding for the
logical server API defined in `docs/specs/pezhai/server.md`.

Normative keywords:

- `MUST`: required for correctness or compatibility
- `MUST NOT`: forbidden
- `SHOULD`: strongly recommended unless there is a clear reason not to
- `MAY`: optional

All text in this repository is ASCII-only by rule.

## 1. Relationship to Other Specs

- `docs/specs/pezhai/server.md` is authoritative for logical request and
  response semantics, ordering, cancellation rules, and lifecycle rules.
- this document is authoritative for byte-level framing, protobuf schema
  usage, and the concrete loopback TCP binding used by the benchmark harness
- startup and shutdown are process-scoped control-plane behavior and remain
  outside the network RPC surface

## 2. Transport Summary

The concrete wire transport is:

- proto3 messages over raw TCP
- fixed frame prefix: 4-byte big-endian `u32` payload length
- one protobuf message payload per frame
- no gRPC

The benchmark binding uses loopback TCP and communicates with the server only
through this transport.

## 3. Protobuf Contract

### 3.1 Schema scope

`proto/sevai.proto` MUST define the full current external logical RPC surface:

- `Put`
- `Delete`
- `Get`
- `ScanStart`
- `ScanFetchNext`
- `Sync`
- `Stats`

It MUST NOT define network RPC methods for startup or shutdown.

### 3.2 Common envelope

Request and response messages MUST carry the same logical envelope shape as
the server spec:

- request: `client_id`, `request_id`, optional `cancel_token`, and one method
  payload
- response: `client_id`, `request_id`, terminal `Status`, and optional success
  payload

The protobuf schema MUST include wire equivalents for:

- `Status`
- `Bound`
- `GetResponse`
- `ScanStartResponse`
- `ScanFetchNextResponse`
- `ScanRow`
- `SyncResponse`
- `LevelStats`
- `LogicalShardStats`

### 3.3 Ordering model

Per-client ordering semantics are preserved by the existing logical model that
keys ordering on `client_id` plus `request_id`.

For benchmark clients, each TCP connection issues at most one in-flight request
at a time. Overall concurrency comes from the number of clients.

## 4. Framing Rules

Each frame is:

1. 4-byte big-endian `u32` declared payload length
2. exactly that many payload bytes, containing one protobuf message

Rules:

- sender MUST write exactly one complete frame per logical message
- receiver MUST support partial reads while assembling the 4-byte length and
  payload bytes
- receiver MUST reject invalid declared lengths according to implementation
  limits and return an error
- receiver MUST reject malformed protobuf payload bytes as decode errors
- receiver MUST only pass fully decoded messages to logical routing

No streaming chunk format or delimiter framing is allowed in this binding.

## 5. Rust Code Generation and Runtime

Rust-side protobuf code generation uses the `protoc` plugin workflow, not a
default `buf build` workflow.

Rules:

- `build.rs` MUST invoke `protoc` with `protoc-gen-buffa`
- `build.rs` MUST emit `cargo:rerun-if-changed` entries for `.proto` inputs
- generated Rust sources MUST be written under `OUT_DIR`
- generated Rust sources MUST NOT be checked into the repository

Tooling requirements:

- `protoc` available on `PATH`, or configured via `PROTOC`
- `protoc-gen-buffa` available on `PATH`, or configured via
  `PROTOC_GEN_BUFFA`

Rust decoding uses `anthropic/buffa` generated types and views for zero-copy
deserialization on the Rust side.

## 6. TCP Adapter Role

The TCP adapter hosts a loopback listener and maps protobuf request frames to
the existing `ExternalRequest` API exposed by `PezhaiServer`.

The adapter:

- preserves logical method semantics from `docs/specs/pezhai/server.md`
- preserves per-client ordering semantics by forwarding `client_id` and
  `request_id` unchanged
- maps logical status and payload variants without changing behavior
- returns one framed protobuf response for each framed protobuf request

## 7. Benchmark Harness Binding

The same-process benchmark harness starts:

- one in-process `PezhaiServer`
- one loopback TCP listener adapter
- multiple same-process TCP benchmark clients

All benchmark operations MUST use the TCP RPC path.

Workload requirements:

- prepopulate exactly `N` keys through TCP
- issue one TCP `Sync` after populate and before timed measurement
- use the populated key set as the benchmark key universe
- value bytes are randomized per request in the inclusive configured range
  during both populate and timed measurement
- `Scan` is one full operation: random start key, forward scan, stop at EOF
  or after 1000 keys total
- default scan page limits are 128 records and 256 KiB per page

CLI percentages:

- explicit `--get-percent`, `--put-percent`, `--delete-percent`,
  and `--scan-percent` override preset values when both are provided
- if neither preset nor explicit percentages are provided, the benchmark uses
  the `balanced` preset: 60 percent `Get`, 25 percent `Put`, 10 percent
  `Delete`, and 5 percent `Scan`

Integration note:

- the benchmark code and benchmark docs MUST keep that default preset aligned
