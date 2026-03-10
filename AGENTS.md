# AGENTS.md

## Purpose

This repository is the starting point for `kalanjiyam`, a distributed
ACID-compliant key-value store.
Keep the codebase intentionally small, clear, and easy to extend.

## Naming

- Database components, modules, and subsystems should use
  transliterated Tamil written in ASCII.
- For general-purpose files, tests, docs, and supporting code, 
  commit message use clear English names unless Tamil naming
  is explicitly requested.
- Do not use Tamil script or any non-ASCII characters in source code,
  docs, tests, or comments.
- Public API names may remain in English when that improves clarity for
  Rust users.

## Code Style

- Prefer small files and focused modules.
- Keep the binary thin and move reusable logic into the library.
- Favor standard library solutions before adding dependencies.
- Avoid premature implementation of distributed systems logic until the
  design is explicit.
- Write clear doc comments on public modules, types, and functions.
- Keep tests simple, local, and readable.
- Use ASCII-only text everywhere in the repository.

## Documentation

- Update `README.md` when the project intent or layout changes.
- Update `Learnings/Skills.md` when build, test, or benchmark workflows
  change.
- Explain why a new component exists before expanding its
  implementation.

## Commit Messages

- When asked to draft a commit message, use the template below.
- Keep every commit-message line under 72 characters.
- Use the first line as a single high-level summary.
- Use the body to describe each changed module or component.
- If multiple components changed, end with an integration note that
  explains how the change fits together.

Template:

```text
<module-or-component-names>: <single high-level description>

<component-name-1>:
<longer description of the changes to this component, including motivation and rationale>
<component-name-2>:
<longer description of the changes to this component, including motivation and rationale>

Integration: (Only if needed to explain how the change works across components)
<how the change works across components>
```
