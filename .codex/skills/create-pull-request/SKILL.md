---
name: create-pull-request
description: Create, draft, or finalize a pull request for the current repository. Use when Codex needs to inspect the current branch, choose or validate a branch name, summarize changes against the default base branch, prepare a clear PR title and body, and optionally open the PR with GitHub CLI. This skill defaults the base branch to main and follows the branch naming pattern <type>-<very-short-feature-name>.
---

# Create Pull Request

## Overview

Use `docs/ai/create-pull-request.md` as the canonical repository guide
for pull-request work.

Use `docs/ai/pull-request-body-template.md` when drafting the body.

## Codex Notes

When this skill triggers:

- inspect the local branch and diff before drafting the PR
- default the base branch to `main`
- follow the branch template `<type>-<very-short-feature-name>`
- keep names short, lowercase, and hyphenated
- transliterate Tamil wording to ASCII before using it in the repo
- return the PR URL when `gh pr create` succeeds
