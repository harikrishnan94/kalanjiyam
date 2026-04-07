# kalanjiyam

`kalanjiyam` is  a distributed, ACID-compliant key-value store.
Its storage engine is named `Pezhai`.

## Workspace

This repository uses the root `kalanjiyam` package as the workspace package owner.
The workspace contains three packages:

- `kalanjiyam`: root package with tiny product-level library helpers
- `pezhai`: storage engine library and `pezhai::sevai` runtime module
- `pezhai-sevai`: server binary package that owns TCP/protobuf transport wiring
  and protobuf code generation from `crates/pezhai-sevai/proto/sevai.proto`

Use explicit workspace flags when you want a repository-wide Cargo command:

```bash
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

Run the server locally with:

```bash
cargo run -p pezhai-sevai -- --config /path/to/config.toml
```

## Python Benchmarks

The repository also includes a supported Python benchmark workflow for the current in-memory
`pezhai-sevai` scaffold under `benchmarks/`.

Install the benchmark dependency:

```bash
python3 -m venv .venv-bench
source .venv-bench/bin/activate
python3 -m pip install -r requirements-bench.txt
```

Build the server binary once, then run one or more isolated workloads:

```bash
cargo build -p pezhai-sevai
python3 -m benchmarks \
  --server-binary target/debug/pezhai-sevai \
  --workload pure-get \
  --workload pure-scan \
  --initial-key-count 1000 \
  --warmup-seconds 2 \
  --measure-seconds 5 \
  --json-output /tmp/pezhai-bench.json
```

Each workload runs against a fresh server process with a fresh temporary config and a fresh
preload cycle. The runner generates Python protobuf bindings at runtime with local `protoc`.
To preserve the current scaffold's monotonic per-client ordering rule under load, it isolates
each logical in-flight lane onto its own TCP connection and `client_id` stream.

Use `--mixed-profile balanced` or `--mixed-ratios put=..,delete=..,get=..,scan=..` for the
`mixed` workload or the wrapper below.

## Portable Wrapper

`benchmarks/run-benchmark.sh <bench-config> <server-config>` validates prerequisites,
parses the lightweight `KEY=VALUE` bench config, merges the provided server template while
overwriting only `sevai.listen_addr`, and invokes `python3 -m benchmarks`. The wrapper
fails fast with precise setup hints if Python, protobuf, `protoc`, or the configured server
binary are missing. Path-valued keys such as `SERVER_BINARY` or `JSON_OUTPUT` may contain
`${workspaceRoot}`, which the wrapper expands to the repo root derived from its own directory.
Example bench configs live under `benchmarks/configs/` and the sample server template is
`benchmarks/server/default.toml`. The wrapper validates prerequisites only; it does not build
the server binary or create the Python environment for you. The committed preset configs use
`INITIAL_KEY_COUNT=1000000`, `WARMUP_SECONDS=5`, and `MEASURE_SECONDS=10`.

```bash
benchmarks/run-benchmark.sh \
  benchmarks/configs/mixed-balanced.conf \
  benchmarks/server/default.toml
```

During a run, stdout now prints live `populate:` progress once per second and `measure:` lines
once per second with current TPS plus `p50`, `p95`, and `p99` latency snapshots.

## Design Docs

The primary storage-engine specification lives in `docs/specs/pezhai.md`.
The asynchronous server specification lives in `docs/specs/sevai.md`.
The TCP RPC transport specification lives in `docs/specs/tcp-rpc.md`.
The workspace package layout lives in `docs/arch/workspace.md`.

## AI Agent Instructions

This repository keeps a portable pull-request workflow in
`docs/ai/create-pull-request.md`.

Thin wrappers point different agents to the same source of truth:

- Codex: `.codex/skills/create-pull-request/`
- Claude: `CLAUDE.md`
- Cursor: `.cursor/rules/create-pull-request.mdc`
- Cline: `.clinerules/create-pull-request.md`
- GitHub Copilot: `.github/copilot-instructions.md`

## GitHub Checks

GitHub Actions validates two repository rules on pushes and pull requests:

- each commit-message line must stay within 72 characters
- Rust formatting must pass `cargo fmt --all --check`
