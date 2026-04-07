# Workspace Package Layout

This architecture note describes the Cargo workspace boundaries for the
repository.

## Packages

The workspace root is the `kalanjiyam` package and also owns the Cargo workspace
manifest.

- root `kalanjiyam` package: tiny product-level library helpers such as
  repository metadata strings
- `crates/pezhai`: reusable storage-engine library code, crate-level error
  type, config parsing, and the `pezhai::sevai` runtime module
- `crates/pezhai-sevai`: executable package that depends on `pezhai` and owns
  CLI bootstrap, process-lifetime wiring, and the TCP/protobuf transport layer

Because the root is a real package, root-level Cargo commands target the
`kalanjiyam` package by default. Workspace-wide workflows should use explicit
`--workspace` flags when they intend to cover every package.

## Ownership Rules

Reusable logical runtime logic MUST live in `pezhai`, not in `pezhai-sevai`.
The binary package owns only transport concerns plus process bootstrap, so the
transport-agnostic server behavior remains reusable from the library crate.

Product-level helper functions that do not belong to the runtime surface MAY
remain in `kalanjiyam`. The `kalanjiyam` crate is not a compatibility shim for
old server APIs and does not re-export the sevai runtime.

## Shared Assets

The workspace root continues to own repository-wide assets that are shared
across packages:

- `Cargo.lock`
- `docs/`
- `buf.yaml`
- repo-wide formatter and lint configuration

The `pezhai-sevai` package owns the protobuf build script and generates Rust
types from `crates/pezhai-sevai/proto/sevai.proto` at build time.
