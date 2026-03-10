#!/usr/bin/env bash

set -euo pipefail

readonly zero_sha='0000000000000000000000000000000000000000'

commit_range() {
  case "${GITHUB_EVENT_NAME:-}" in
    pull_request|pull_request_target)
      printf '%s..%s\n' "${PR_BASE_SHA}" "${PR_HEAD_SHA}"
      ;;
    push)
      if [[ -n "${PUSH_BEFORE_SHA:-}" && "${PUSH_BEFORE_SHA}" != "${zero_sha}" ]]; then
        printf '%s..%s\n' "${PUSH_BEFORE_SHA}" "${PUSH_AFTER_SHA:-HEAD}"
      else
        printf '%s^!\n' "${PUSH_AFTER_SHA:-HEAD}"
      fi
      ;;
    *)
      printf '%s^!\n' "${PUSH_AFTER_SHA:-HEAD}"
      ;;
  esac
}

readonly range="$(commit_range)"

mapfile -t commits < <(git rev-list --reverse "${range}")

if [[ "${#commits[@]}" -eq 0 ]]; then
  echo "No commits found for range ${range}."
  exit 0
fi

invalid=0

for commit in "${commits[@]}"; do
  subject="$(git log --format=%s -n 1 "${commit}")"
  line_number=0

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line_number=$((line_number + 1))
    line_length="$(printf '%s\n' "${line}" | awk 'NR == 1 { print length($0) }')"

    if (( line_length > 72 )); then
      echo "Commit ${commit} failed validation."
      echo "Subject: ${subject}"
      echo "Line ${line_number} has ${line_length} characters."
      invalid=1
    fi
  done < <(git log --format=%B -n 1 "${commit}")
done

if (( invalid != 0 )); then
  echo "Each commit message line must be 72 characters or fewer."
  exit 1
fi

echo "Validated ${#commits[@]} commit message(s)."
