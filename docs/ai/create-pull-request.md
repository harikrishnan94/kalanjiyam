# Create Pull Request

Use this document as the canonical pull-request workflow for AI agents in
this repository.

## Goal

Turn local changes into a reviewable pull request with:

- a short, descriptive branch name
- `main` as the default base branch
- a concise title and body grounded in the actual diff
- a short validation summary

## Repository Constraints

- Use ASCII-only text in repository files and comments.
- If a branch name idea comes from Tamil wording, transliterate it to
  ASCII before using it in the repository.
- Prefer small, focused pull requests.

## Trigger Cases

Use this workflow when asked to:

- create a branch for new work
- rename or validate a branch name
- draft a pull request title or body
- open a pull request with GitHub CLI
- summarize local changes for review

## Base Branch

- Default the pull request base branch to `main`.
- Only use a different base branch when the user explicitly asks for it
  or the repository clearly requires it.

## Branch Naming

Use this branch template:

```text
<type>-<very-short-feature-name>
```

Preferred `type` values:

- `feature`
- `bug`
- `refactor`
- `docs`
- `test`
- `chore`
- `perf`

Branch naming rules:

- keep everything lowercase
- use hyphens between words
- keep the feature segment short and specific
- prefer 2 to 5 words in the feature segment
- keep an existing branch name when it already matches the work

Examples:

```text
feature-bootstrap-replication
bug-fix-startup-panic
refactor-cleanup-wal-init
```

## Pull Request Workflow

### 1. Inspect the current state

Collect only the context needed to draft the pull request:

- `git status --short --branch`
- `git branch --show-current`
- `git log --oneline main..HEAD`
- `git diff --stat main...HEAD`

Inspect the changed files before writing the summary.

### 2. Validate the branch name

- If the current branch already matches the naming rule, keep it.
- If it does not, propose a compliant name before opening the pull
  request.
- Do not invent a broad or vague feature segment.

### 3. Draft the pull request title

Write the title in imperative form when possible.

Good patterns:

- `Add WAL bootstrap guard`
- `Fix startup panic in node init`
- `Refactor memtable flush setup`

### 4. Draft the pull request body

Use `docs/ai/pull-request-body-template.md`.

Fill only the sections that help reviewers understand:

- what changed
- why the change exists
- what validation ran
- any risk, limitation, or follow-up

Remove empty sections instead of leaving placeholders.

### 5. Verify readiness

Before creating the pull request:

- ensure the branch has commits beyond `main`
- confirm whether the working tree is clean
- report tests, formatting, or manual verification honestly
- stop at a draft if the user asked only for a draft

### 6. Open the pull request

If `gh` is available and the user wants the pull request opened:

- push the branch if needed
- run `gh pr create --base main --title <title> --body-file <file>`
- return the resulting pull request URL

If `gh` is unavailable or unauthenticated, return:

- the branch name
- the base branch
- the final title
- the final body
- the exact `gh pr create` command to run later

## Output Checklist

Return these items whenever possible:

- current branch name
- base branch
- final pull request title
- final pull request body
- validation summary
- pull request URL or blocking reason
