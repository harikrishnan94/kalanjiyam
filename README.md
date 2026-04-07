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
