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

The repository does not yet include committed benchmark targets.
When benchmark files are added under `benches/`, use:

```bash
cargo bench --workspace
```

If you only want to verify that benchmark code compiles, use:

```bash
cargo bench --workspace --no-run
```

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
