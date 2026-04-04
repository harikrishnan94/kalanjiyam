# Learnings / Skills

This document captures the local developer workflow for `kalanjiyam`.
It is intentionally lightweight and should evolve with the repository.

## Build

Build the project in debug mode:

```bash
cargo build
```

Build in release mode:

```bash
cargo build --release
```

## Run

Run the binary locally:

```bash
cargo run
```

Run the binary with an explicit config path:

```bash
cargo run -- --config /path/to/store/config.toml
```

## Test

Run all unit and integration tests:

```bash
cargo test
```

Run a specific test target:

```bash
cargo test --test smoke
```

## Protobuf Tooling

The proto3 transport code is generated at build time.

Required tools:

- `protoc` on `PATH` or set via `PROTOC`
- `protoc-gen-buffa` on `PATH` or set via `PROTOC_GEN_BUFFA`

Install example for the plugin:

```bash
cargo install protoc-gen-buffa
```

Generation behavior:

- `build.rs` drives protobuf generation through `protoc` plus
  `protoc-gen-buffa`
- generated Rust files go to `OUT_DIR`
- generated files are not checked into the repository

## Benchmarks

Compile the TCP benchmark target only:

```bash
cargo bench --bench sevai_tcp_bench --no-run
```

Run the TCP benchmark with defaults:

```bash
cargo bench --bench sevai_tcp_bench -- \
  --populate-clients 4 \
  --clients 4 \
  --populate-keys 50000 \
  --duration 30
```

Run with an explicit config file:

```bash
cargo bench --bench sevai_tcp_bench -- \
  --config /path/to/store/config.toml \
  --populate-clients 4 \
  --clients 4 \
  --populate-keys 50000 \
  --duration 30
```

Run with an explicit operation mix:

```bash
cargo bench --bench sevai_tcp_bench -- \
  --populate-clients 4 \
  --clients 4 \
  --populate-keys 50000 \
  --duration 30 \
  --get-percent 70 \
  --put-percent 20 \
  --delete-percent 5 \
  --scan-percent 5
```

Run with a preset:

```bash
cargo bench --bench sevai_tcp_bench -- \
  --populate-clients 4 \
  --clients 4 \
  --populate-keys 50000 \
  --duration 30 \
  --preset <name>
```

Notes:

- explicit percentages override preset values when both are provided
- `--populate-clients` controls how many TCP clients pre-populate the key set;
  it defaults to `4`
- if `--config` is omitted, the benchmark creates a temporary store and writes
  a default `config.toml` using repository semantic defaults
- default preset is `balanced`: 60 percent `Get`, 25 percent `Put`, 10
  percent `Delete`, and 5 percent `Scan`

## Formatting

Check formatting:

```bash
cargo fmt --check
```

Apply formatting:

```bash
cargo fmt
```

## GitHub Checks

GitHub Actions validates the same formatting command with:

```bash
cargo fmt --all --check
```

GitHub Actions also rejects commits when any commit-message line is
longer than 72 characters.

## Notes

- Keep the binary entry point minimal.
- Put reusable logic in the library crate.
- Prefer adding tests before adding complexity.
- Add benchmarks only when there is behavior worth measuring.
