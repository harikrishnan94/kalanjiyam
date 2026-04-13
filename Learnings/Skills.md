# Learnings / Skills

This document captures the local developer workflow for `kalanjiyam`.
It is intentionally lightweight and should evolve with the repository.

## Build

Build the whole workspace in debug mode:

```bash
cargo build --workspace
```

Build the whole workspace in release mode:

```bash
cargo build --workspace --release
```

## Run

Run the binary locally:

```bash
cargo run -p pezhai-sevai -- --config /path/to/config.toml
```

The shared config schema currently includes `[engine]`, `[wal]`, `[lsm]`,
`[maintenance]`, `[server_limits]`, and `[sevai]`.

## Test

Run all unit and integration tests:

```bash
cargo test --workspace
```

Run a specific test target:

```bash
cargo test -p kalanjiyam --test smoke
```

The `kalanjiyam` smoke test lives at the workspace root under `tests/smoke.rs`.

## Benchmarks

Build the current server binary once before running the Python benchmark workflow:

```bash
cargo build -p pezhai-sevai
```

Install the Python protobuf runtime used by the benchmark runner:

```bash
python3 -m venv .venv-bench
source .venv-bench/bin/activate
python3 -m pip install -r requirements-bench.txt
```

Run one or more isolated benchmark workloads:

```bash
python3 -m benchmarks \
  --server-binary target/release/pezhai-sevai \
  --workload pure-put \
  --workload pure-get \
  --initial-key-count 1000 \
  --warmup-seconds 2 \
  --measure-seconds 5
```

The runner keeps each logical in-flight lane on its own TCP connection and
`client_id` stream so the server's ordering checks remain valid.

Use `--mixed-profile balanced` or `--mixed-ratios put=..,delete=..,get=..,scan=..` for
`mixed`, or run `benchmarks/run-benchmark.sh <bench-config> <server-config>` with one of the
presets in `benchmarks/configs/` plus `benchmarks/server/default.toml`. The wrapper validates
prerequisites and prints setup commands on failure; it does not build the binary or create the
Python environment. The committed preset configs use `INITIAL_KEY_COUNT=1000000`,
`WARMUP_SECONDS=5`, and `MEASURE_SECONDS=10`.

```bash
benchmarks/run-benchmark.sh \
  benchmarks/configs/pure-get.conf \
  benchmarks/server/default.toml
```

The benchmark prints live `populate:` progress every second during preload and `measure:` lines
every second during the measurement window with current TPS and `p50`/`p95`/`p99` latency.

Run the benchmark test suite:

```bash
python3 -m unittest benchmarks.tests.test_tcp_bench
```

Run the benchmark coverage report:

```bash
python3 benchmarks/tests/coverage_check.py
```

This report is best-effort in-process coverage. The benchmark smoke suite
intentionally exercises subprocess CLI paths that stdlib `trace` does not
follow across process boundaries.

## Formatting

Check formatting:

```bash
cargo fmt --all --check
```

Apply formatting:

```bash
cargo fmt --all
```

## Lint

Run Clippy for all workspace packages and targets:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

## Coverage

Generate the text summary and HTML report for the whole workspace:

```bash
cargo llvm-cov --workspace --html
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
- The repository root is the `kalanjiyam` package, so use `--workspace`
  whenever a command should cover every package.
- Put reusable logic in the `pezhai` library crate.
- Prefer adding tests before adding complexity.
- Add benchmarks only when there is behavior worth measuring.
