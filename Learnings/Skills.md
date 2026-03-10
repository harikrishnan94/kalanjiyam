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

## Test

Run all unit and integration tests:

```bash
cargo test
```

Run a specific test target:

```bash
cargo test --test smoke
```

## Benchmarks

The repository does not yet include committed benchmark targets.
When benchmark files are added under `benches/`, use:

```bash
cargo bench
```

If you only want to verify that benchmark code compiles, use:

```bash
cargo bench --no-run
```

## Formatting

Check formatting:

```bash
cargo fmt --check
```

Apply formatting:

```bash
cargo fmt
```

## Notes

- Keep the binary entry point minimal.
- Put reusable logic in the library crate.
- Prefer adding tests before adding complexity.
- Add benchmarks only when there is behavior worth measuring.
