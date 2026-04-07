# kalanjiyam

`kalanjiyam` is  a distributed, ACID-compliant key-value store.
Its storage engine is named `Pezhai`.

## Design Docs

The primary storage-engine specification lives in `docs/specs/pezhai.md`.
The asynchronous server specification lives in `docs/specs/sevai.md`.
The TCP RPC transport specification lives in `docs/specs/tcp-rpc.md`.

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
