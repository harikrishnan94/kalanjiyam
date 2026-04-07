# Pezhai TCP RPC Transport Specification

This document defines the concrete proto3-over-TCP transport binding for the
logical server API defined in `docs/specs/sevai.md`.

Normative keywords:

- `MUST`: required for correctness or compatibility
- `MUST NOT`: forbidden
- `SHOULD`: strongly recommended unless there is a clear reason not to
- `MAY`: optional

All text in this repository is ASCII-only by rule.

## 1. Relationship to Other Specs

- `docs/specs/sevai.md` is authoritative for logical request and response
  semantics, ordering, cancellation rules, and lifecycle rules.
- this document is authoritative for byte-level framing, protobuf schema
  usage
- startup and shutdown are process-scoped control-plane behavior and remain
  outside the network RPC surface

## 2. Transport Summary

The concrete wire transport is:

- proto3 messages over raw TCP
- fixed frame prefix: 4-byte big-endian `u32` payload length
- one protobuf message payload per frame
- no gRPC

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

For clients, each TCP connection can handle multiple in-flight requests.

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

## 6. TCP Adapter Role

The TCP adapter hosts a loopback listener and maps protobuf request frames to
the existing `ExternalRequest` API exposed by `PezhaiServer`.

The adapter:

- preserves logical method semantics from `docs/specs/sevai.md`
- preserves per-client ordering semantics by forwarding `client_id` and
  `request_id` unchanged
- maps logical status and payload variants without changing behavior
- returns one framed protobuf response for each framed protobuf request
