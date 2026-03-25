# AGENTS.md

## Scope

- These instructions apply to the entire repository.
- Do not assume, invent, fill in gaps, or silently choose unspecified
  behavior. If something is unclear, missing, or ambiguous, stop and ask
  for explicit user approval before proceeding.

## Project Baseline

- Build system: Cargo.
- Dependency acquisition method: crates.io via `Cargo.toml` and
  `Cargo.lock`.
- Language edition: Rust 2024.

## Naming

- Database components, modules, and subsystems should use transliterated
  Tamil written in ASCII.
- For general-purpose files, tests, docs, and supporting code, use clear
  English names unless Tamil naming is explicitly requested.
- Do not use Tamil script or any non-ASCII characters in source code,
  docs, tests, or comments.
- Public API names may remain in English when that improves clarity for
  Rust users.

## Required Workflow

1. Inspect the current repository state before making changes.
2. Look up the latest relevant official documentation before proposing
   or making changes.
3. Base all recommendations and code changes on the current repository
   state plus the latest documentation.
4. If documentation, repository state, or user intent conflicts or is
   incomplete, pause and ask the user instead of guessing.

## Documentation Policy

- Always consult the latest official documentation relevant to the task
  before changing build files, dependencies, compiler settings, module
  structure, or public APIs.
- Treat documentation lookup as mandatory, not optional.
- Do not rely on stale memory for Cargo, crates.io, the Rust compiler,
  or standard library behavior when current documentation can be checked.
- If the latest documentation cannot be accessed, say so clearly and
  wait for user approval before continuing.

## Architecture Documentation

- Architecture documents live under `docs/arch/*.md`. Treat the files
  in that directory collectively as the single source of truth for the
  project's architecture and design decisions.
- Specification documents live under `docs/specs/*.md`. Treat the files
  in that directory collectively as the single source of truth for
  externally visible behavior, invariants, and semantics.
- `docs/arch/` documents must describe the high-level implementation
  approach in programming-language-agnostic terms, including major data
  structures, algorithms, file/layout rules, performance
  characteristics, and important trade-offs that affect behavior or
  operational expectations.
- `docs/arch/` documents must record significant design decisions
  together with the rationale behind them whenever that context is
  needed to preserve behavior, semantics, or maintenance intent across
  future rewrites.
- `docs/specs/` documents must define the intended behavior and
  semantics of the database in language-agnostic terms so the library
  can be reimplemented in another language without changing observable
  behavior.
- `docs/specs/` documents must capture the full behavioral spec,
  including externally visible semantics, invariants, edge cases,
  ordering rules, persistence and recovery expectations, error cases,
  and any other non-trivial details needed to reproduce the same
  results.
- The combined `docs/arch/` and `docs/specs/` content must be detailed
  enough that an agent can use them as the single point of truth to
  rewrite the library in any language and still reproduce the same
  semantics and behavior.
- Every task must include an explicit decision about whether any file
  under `docs/arch/` or `docs/specs/` needs to be updated.
- Update the relevant `docs/arch/` file whenever the core architecture
  changes or evolves.
- Update the relevant `docs/specs/` file with every significant change
  that affects externally visible behavior, semantics, edge cases, or
  invariants.
- If no file under `docs/arch/` or `docs/specs/` is updated for a
  task, the agent must explicitly state why no update was required.

## Project Layout

```
kalanjiyam/
├── Cargo.toml           # Workspace manifest
├── Cargo.lock           # Locked dependency versions (commit this)
├── README.md
├── docs/specs/*.md       # Single source of truth for spec
├── docs/arch/*.md       # Single source of truth for architecture
├── AGENTS.md
├── Learnings/
│   └── Skills.md        # Build, test, and benchmark workflows
├── src/
│   ├── lib.rs           # Library crate root; re-export public API here
│   ├── error.rs         # Crate-level error type
│   └── main.rs          # Thin binary entry point
└── tests/
    └── integration/     # Integration tests (one file per subsystem)
```

- Keep the binary (`main.rs`) thin. Move all reusable logic into the
  library crate (`lib.rs` and its submodules).
- Each database subsystem lives in its own module file under `src/`.
- Name module files after their Tamil transliteration
  (e.g., `src/idam.rs` for the storage layer).

## Build and Dependency Rules

- Use Cargo for compilation, testing, benchmarking, and project
  structure changes.
- Before making changes, inspect `Cargo.toml` and `Cargo.lock` to
  understand the current dependency graph.
- All third-party dependencies must be added via crates.io in
  `Cargo.toml`. Do not vendor, apply `[patch]` overrides, or use
  path hacks without explicit user approval.
- Annotate every new dependency added to `Cargo.toml` with an inline
  comment explaining why it is needed.
- Favor `std` solutions before adding crates.io dependencies.
- Commit `Cargo.lock` to the repository to ensure reproducible builds.
- Keep `Cargo.toml` consistent with the repository's existing workspace
  layout and feature-flag naming.

## Testing and Coverage Rules

- Use `cargo test` for test execution.
- Use `cargo llvm-cov` (or the repository's designated coverage target)
  to produce both the text summary and the HTML coverage report.
- Agent-generated tests must include coverage verification for the
  agent's changes.
- For agent-generated changes, the agent must achieve 100% measured
  coverage for the relevant new or modified code whenever feasible.
- If 100% coverage cannot be achieved, the agent must explicitly tell
  the user, explain the reason, and offer concrete suggestions for
  closing the remaining gap.
- Do not claim coverage goals were met without running the relevant
  coverage workflow and reporting the result.
- Unit tests live in a `#[cfg(test)]` module at the bottom of the file
  they test.
- Integration tests live under `tests/integration/`, one file per
  subsystem.
- Keep tests simple, local, and readable. Prefer deterministic inputs
  over random data.

## Rust Rules

- Target Rust edition 2024.
- Do not assume that an unstable or nightly-only feature is available
  without explicitly confirming it and obtaining user approval.
- Do not introduce fallback behavior for older Rust editions unless the
  user explicitly requests it.
- Always check the latest stable Rust release notes and the edition
  guide before proposing edition-specific features.

## Error Handling

- Define a crate-level error type in `src/error.rs` and re-export it
  from `lib.rs`.
- Use the `?` operator for propagation; avoid `unwrap` or `expect`
  outside of tests and examples.
- In tests, `unwrap` is acceptable; add a short comment explaining what
  would cause the failure.
- Prefer `std::error::Error`-implementing types over ad hoc string
  errors.

## Commenting Rules

- Any newly created non-trivial source file must start with a short
  `//!` module-level doc comment describing its responsibility and
  boundary with neighboring files.
- Every new or materially changed item in the public API must have a
  `///` doc comment covering its purpose, important constraints, failure
  behavior, and any non-obvious ownership or lifetime rules.
- New public enums, option structs, trait impls, and other public API
  additions must not be left undocumented.
- Keep `//!` module-level comments at the top of each non-obvious module
  so readers can quickly understand why the module exists, how it should
  be used, and any important constraints or pitfalls.
- Every new non-obvious struct or enum in `src/` or `tests/` must have a
  `///` doc comment explaining why it exists, what state or invariants
  it owns, and how it is meant to be used.
- Add clear, concise why and how comments to all non-trivial functions,
  complex match arms, and unsafe blocks.
- Every new non-trivial free function, syscall wrapper, persistence
  helper, parsing helper, fault-injection utility, and similar helper
  must have a short why/how comment unless that behavior is already
  explained by an immediately adjacent overview comment.
- Document every `unsafe` block with a `// SAFETY:` comment explaining
  the invariant that makes it sound.
- Prefer plain language over unnecessary jargon so comments remain
  accessible to developers with varying levels of experience.
- Keep comments structured, accurate, and non-redundant; do not restate
  what the code already makes obvious.
- Use comments to capture design decisions, trade-offs, assumptions, and
  potential pitfalls when that context will help future maintenance.
- Update comments whenever the surrounding code changes so they remain
  accurate and relevant.
- `ARCH.md` does not replace inline code comments. Architectural updates
  do not satisfy the requirement to document new code where readers will
  use and maintain it.

## Documentation Completion Gate

- Before finishing any task, inspect the staged and unstaged diff and
  verify comment coverage for new public API surface, new non-obvious
  types, new complex helpers, and new test infrastructure.
- Do not treat a task as complete while comment coverage for those
  changes is missing or obviously thinner than the surrounding codebase.
- If you cannot confidently document a new behavior, invariant,
  ownership rule, or failure mode from the repository state and latest
  documentation, stop and ask the user instead of shipping minimally
  commented code.
- Final responses must explicitly state which files received comment or
  documentation updates.
- Final responses must also list any changed files that intentionally
  did not need new comments, together with a brief reason.

## Codegen Priorities

- Optimize first for readability, maintainability, and reducing cognitive
  overload for future readers.
- After readability and maintainability are satisfied, optimize for
  reducing incremental compilation time (prefer fewer monomorphized
  generics; prefer `dyn Trait` where performance is not critical).
- Prefer `std::fmt::Display`, `std::fmt::Write`, and format macros over
  ad hoc string buffers and manual concatenation unless a clear,
  documented reason makes another choice better.
- Prefer supported stable standard features and actively look for
  opportunities to use them when they improve clarity, maintainability,
  or compile-time behavior.
- Use `#[must_use]` on types and functions whose return values must not
  be silently dropped.
- Avoid `clone` in hot paths; document any clone that is intentional but
  non-obvious.

## Style and Quality Gates

- Generated and edited code must strictly obey the repository's
  `rustfmt.toml` and the `clippy.toml` (or inline `#![deny(...)]`
  settings).
- Treat `rustfmt.toml` and `clippy.toml` as the source of truth for
  formatting and linting decisions.
- If either configuration file is missing, do not infer or invent the
  style. Ask the user how to proceed before generating or mass-editing
  code.
- Resolve all `clippy` warnings with `cargo clippy -- -D warnings`
  before considering a task complete.
- Do not introduce style changes unrelated to the task.

## Line Length Rules

- Non-Rust files (`.md`, `.toml`, scripts, etc.) must use a maximum
  line length of 100 characters.
- Rust source files are exempt; their line length is governed entirely
  by `rustfmt.toml`. Do not manually wrap Rust code to fit the
  100-character limit.
- If a non-Rust file cannot be formatted to fit this limit, or another
  protocol, standard, or external mandate requires a different layout,
  treat that case as an explicit exception.
- Outside those exceptions, the 100-character maximum is mandatory for
  all non-Rust files.

## Approval Boundaries

- Ask before introducing new dependencies, changing public interfaces,
  altering the module layout in a non-local way, or making assumptions
  not directly supported by the repository and latest documentation.
- When presenting options, clearly separate verified facts from
  proposals.
- When uncertain, prefer a short clarification request over speculative
  implementation.

## Commit Message Rules

- Commit messages generated by agents must be formatted properly.
- Wrap all commit message lines at 72 characters or fewer.
- Use the first line as a single high-level summary.
- Use the body to describe each changed module or component.
- If multiple components changed, end with an integration note that
  explains how the change fits together.
- Do not generate overly long subjects or body lines that exceed the
  72-character limit.

Template:

```text
<module-or-component-names>: <single high-level description>

<component-name-1>:
<longer description of the changes to this component, including
motivation and rationale>

<component-name-2>:
<longer description of the changes to this component, including
motivation and rationale>

Integration: (Only if needed to explain how the change works across
components)
<how the change works across components>
```

## Response Expectations for Future Tasks

- State what was verified from the repository.
- State what was verified from the latest documentation.
- Call out any missing information or assumptions that need user
  approval.
- State that the staged and unstaged diff was reviewed for documentation
  coverage before finishing.
- Explicitly list which files received comment or documentation updates.
- Explicitly list any changed files that did not need new comments, with
  a brief reason.
- Explicitly state at the end whether `ARCH.md` was updated and
  summarize the changes made, or state why no `ARCH.md` change was
  required.
- Keep changes minimal, traceable, and consistent with the existing
  codebase.

## Quick-Reference Commands

| Task                       | Command                               |
|----------------------------|---------------------------------------|
| Build                      | `cargo build`                         |
| Run tests                  | `cargo test`                          |
| Run a specific test        | `cargo test <test_name>`              |
| Lint                       | `cargo clippy -- -D warnings`         |
| Format                     | `cargo fmt`                           |
| Generate docs              | `cargo doc --open`                    |
| Run benchmarks             | `cargo bench`                         |
| Check without building     | `cargo check`                         |
| Coverage (text + HTML)     | `cargo llvm-cov --html`               |
