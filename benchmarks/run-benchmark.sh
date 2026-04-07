#!/bin/sh

# Portable shell entrypoint for benchmark config-file driven runs.

set -eu

PYTHON_BIN=${PYTHON_BIN:-python3}

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "error: python interpreter not found: $PYTHON_BIN" >&2
  exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
workspace_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

if [ -n "${PYTHONPATH:-}" ]; then
  export PYTHONPATH="$workspace_root:$PYTHONPATH"
else
  export PYTHONPATH="$workspace_root"
fi
export WORKSPACE_ROOT="$workspace_root"

exec "$PYTHON_BIN" -m benchmarks.wrapper "$@"
