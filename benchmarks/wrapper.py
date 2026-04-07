"""Portable wrapper helpers for running benchmark presets from config files."""

from __future__ import annotations

import argparse
import dataclasses
import os
import pathlib
import sys
from typing import Final

from . import tcp_bench

WORKSPACE_ROOT_ENV: Final[str] = "WORKSPACE_ROOT"
DEFAULT_SERVER_BINARY: Final[str] = "${workspaceRoot}/target/release/pezhai-sevai"
PATH_KEYS: Final[frozenset[str]] = frozenset({"SERVER_BINARY", "JSON_OUTPUT"})
REQUIRED_KEYS: Final[frozenset[str]] = frozenset({"WORKLOAD", "INITIAL_KEY_COUNT"})
KEY_TO_FLAG: Final[dict[str, str]] = {
    "WORKLOAD": "--workload",
    "INITIAL_KEY_COUNT": "--initial-key-count",
    "WARMUP_SECONDS": "--warmup-seconds",
    "MEASURE_SECONDS": "--measure-seconds",
    "CONNECTIONS": "--connections",
    "INFLIGHT_PER_CONNECTION": "--inflight-per-connection",
    "PRELOAD_CONNECTIONS": "--preload-connections",
    "PRELOAD_INFLIGHT_PER_CONNECTION": "--preload-inflight-per-connection",
    "KEY_SIZE_RANGE": "--key-size-range",
    "VALUE_SIZE_RANGE": "--value-size-range",
    "SCAN_SPAN_KEYS": "--scan-span-keys",
    "SCAN_PAGE_MAX_RECORDS": "--scan-page-max-records",
    "SCAN_PAGE_MAX_BYTES": "--scan-page-max-bytes",
    "MIXED_PROFILE": "--mixed-profile",
    "MIXED_RATIOS": "--mixed-ratios",
    "JSON_OUTPUT": "--json-output",
    "SEED": "--seed",
    "STARTUP_TIMEOUT_SECONDS": "--startup-timeout-seconds",
    "SERVER_BINARY": "--server-binary",
}
ALLOWED_KEYS: Final[frozenset[str]] = frozenset(KEY_TO_FLAG)


class WrapperError(Exception):
    """Raised when the shell wrapper inputs are malformed or incomplete."""


@dataclasses.dataclass(frozen=True)
class WrapperConfig:
    """Validated wrapper inputs ready to map into benchmark CLI arguments."""

    workspace_root: pathlib.Path
    bench_config: pathlib.Path
    server_config: pathlib.Path
    values: dict[str, str]

    def benchmark_argv(self) -> list[str]:
        """Builds the argv that will be forwarded to `python -m benchmarks`."""
        argv = [
            "--server-binary",
            self.values["SERVER_BINARY"],
            "--server-config",
            str(self.server_config),
            "--workload",
            self.values["WORKLOAD"],
            "--initial-key-count",
            self.values["INITIAL_KEY_COUNT"],
        ]

        for key in (
            "WARMUP_SECONDS",
            "MEASURE_SECONDS",
            "CONNECTIONS",
            "INFLIGHT_PER_CONNECTION",
            "PRELOAD_CONNECTIONS",
            "PRELOAD_INFLIGHT_PER_CONNECTION",
            "KEY_SIZE_RANGE",
            "VALUE_SIZE_RANGE",
            "SCAN_SPAN_KEYS",
            "SCAN_PAGE_MAX_RECORDS",
            "SCAN_PAGE_MAX_BYTES",
            "MIXED_PROFILE",
            "MIXED_RATIOS",
            "JSON_OUTPUT",
            "SEED",
            "STARTUP_TIMEOUT_SECONDS",
        ):
            value = self.values.get(key)
            if value is None:
                continue
            argv.extend([KEY_TO_FLAG[key], value])

        return argv


def resolve_workspace_root() -> pathlib.Path:
    """Resolves the repository root from the wrapper environment or module path."""
    configured = os.environ.get(WORKSPACE_ROOT_ENV)
    if configured:
        return pathlib.Path(configured).expanduser().resolve()
    return pathlib.Path(__file__).resolve().parents[1]


def parse_bench_config_text(text: str, *, path: pathlib.Path) -> dict[str, str]:
    """Parses restricted `KEY=VALUE` benchmark config text."""
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if "=" not in raw_line:
            raise WrapperError(
                f"{path}:{line_number}: bench config lines must use KEY=VALUE"
            )

        key, value = raw_line.split("=", 1)
        if not key or key != key.strip() or not key.isupper():
            raise WrapperError(
                f"{path}:{line_number}: malformed config key `{key}`"
            )
        if key not in ALLOWED_KEYS:
            raise WrapperError(f"{path}:{line_number}: unknown bench config key `{key}`")
        if value.startswith(" ") or value.endswith(" "):
            raise WrapperError(
                f"{path}:{line_number}: values must not contain spaces around `=`"
            )
        if not value:
            raise WrapperError(f"{path}:{line_number}: `{key}` must not be empty")
        if key in values:
            raise WrapperError(f"{path}:{line_number}: duplicate bench config key `{key}`")
        values[key] = value

    for required_key in sorted(REQUIRED_KEYS):
        if required_key not in values:
            raise WrapperError(f"{path}: missing required bench config key `{required_key}`")

    mixed_profile = values.get("MIXED_PROFILE")
    mixed_ratios = values.get("MIXED_RATIOS")
    workload = values["WORKLOAD"]
    if mixed_profile and mixed_ratios:
        raise WrapperError(
            f"{path}: MIXED_PROFILE and MIXED_RATIOS are mutually exclusive"
        )
    if workload != "mixed" and (mixed_profile or mixed_ratios):
        raise WrapperError(
            f"{path}: MIXED_PROFILE and MIXED_RATIOS are only valid when WORKLOAD=mixed"
        )
    return values


def expand_workspace_root(value: str, workspace_root: pathlib.Path) -> str:
    """Expands the literal `${workspaceRoot}` token inside a config value."""
    return value.replace("${workspaceRoot}", str(workspace_root))


def resolve_wrapper_config(
    bench_config: pathlib.Path,
    server_config: pathlib.Path,
    workspace_root: pathlib.Path,
    *,
    validate_runtime: bool = True,
) -> WrapperConfig:
    """Loads, validates, and expands both wrapper config files."""
    if not bench_config.exists():
        raise WrapperError(f"bench config does not exist: {bench_config}")
    if not bench_config.is_file():
        raise WrapperError(f"bench config is not a file: {bench_config}")
    if not server_config.exists():
        raise WrapperError(f"server config does not exist: {server_config}")
    if not server_config.is_file():
        raise WrapperError(f"server config is not a file: {server_config}")

    try:
        bench_text = bench_config.read_text(encoding="utf-8")
    except OSError as error:
        raise WrapperError(f"failed to read bench config `{bench_config}`: {error}") from error

    values = parse_bench_config_text(bench_text, path=bench_config)
    values = dict(values)
    values.setdefault("SERVER_BINARY", DEFAULT_SERVER_BINARY)
    for path_key in PATH_KEYS:
        if path_key in values:
            values[path_key] = expand_workspace_root(values[path_key], workspace_root)

    server_binary = pathlib.Path(values["SERVER_BINARY"]).expanduser().resolve()
    values["SERVER_BINARY"] = str(server_binary)
    if "JSON_OUTPUT" in values:
        values["JSON_OUTPUT"] = str(
            pathlib.Path(values["JSON_OUTPUT"]).expanduser().resolve()
        )

    if validate_runtime:
        validate_prerequisites(server_binary, server_config)
    return WrapperConfig(
        workspace_root=workspace_root,
        bench_config=bench_config.resolve(),
        server_config=server_config.resolve(),
        values=values,
    )


def validate_prerequisites(
    server_binary: pathlib.Path,
    server_config: pathlib.Path,
) -> None:
    """Validates the runtime prerequisites expected by the wrapper."""
    try:
        tcp_bench.ensure_protobuf_runtime()
        tcp_bench.resolve_protoc()
        tcp_bench.load_server_config_template_text(server_config)
    except tcp_bench.CliError as error:
        raise WrapperError(str(error)) from error

    if not server_binary.exists():
        raise WrapperError(f"configured server binary does not exist: {server_binary}")
    if not os.access(server_binary, os.X_OK):
        raise WrapperError(f"configured server binary is not executable: {server_binary}")


def setup_instructions(workspace_root: pathlib.Path) -> str:
    """Returns the prerequisite commands printed by the wrapper on validation errors."""
    return (
        "Setup commands:\n"
        f"  python3 -m venv {workspace_root / '.venv-bench'}\n"
        f"  {workspace_root / '.venv-bench' / 'bin' / 'python'} -m pip install -r "
        f"{workspace_root / 'requirements-bench.txt'}\n"
        f"  cargo build --release -p pezhai-sevai"
    )


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parses the wrapper's two positional config file paths."""
    parser = argparse.ArgumentParser(
        prog="benchmarks/run-benchmark.sh",
        description="Portable wrapper around the Python benchmark runner.",
    )
    parser.add_argument("bench_config", help="Path to the KEY=VALUE benchmark config.")
    parser.add_argument("server_config", help="Path to the base pezhai-sevai TOML config.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Validates wrapper inputs, then forwards to the Python benchmark runner."""
    parsed = parse_args(argv)
    workspace_root = resolve_workspace_root()
    try:
        config = resolve_wrapper_config(
            pathlib.Path(parsed.bench_config).expanduser(),
            pathlib.Path(parsed.server_config).expanduser(),
            workspace_root,
        )
    except WrapperError as error:
        print(f"error: {error}", file=sys.stderr)
        print(setup_instructions(workspace_root), file=sys.stderr)
        return 2

    return tcp_bench.main(config.benchmark_argv())


if __name__ == "__main__":
    raise SystemExit(main())
