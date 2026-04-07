"""Tests for the supported `pezhai-sevai` Python TCP benchmark workflow."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
import venv
from unittest import mock

from benchmarks import tcp_bench
from benchmarks import wrapper

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER_BINARY = REPO_ROOT / "target" / "debug" / "pezhai-sevai"
RELEASE_SERVER_BINARY = REPO_ROOT / "target" / "release" / "pezhai-sevai"
WRAPPER_SCRIPT = REPO_ROOT / "benchmarks" / "run-benchmark.sh"
SERVER_TEMPLATE = REPO_ROOT / "benchmarks" / "server" / "default.toml"
PRESET_CONFIG_DIR = REPO_ROOT / "benchmarks" / "configs"
SHORT_WARMUP_SECONDS = "0.15"
SHORT_MEASURE_SECONDS = "0.25"
BENCHMARK_PYTHON: str | None = None


def ensure_benchmark_python() -> None:
    """Builds the server binaries and one benchmark venv shared by the test module."""
    global BENCHMARK_PYTHON
    if BENCHMARK_PYTHON is not None:
        return

    subprocess.run(
        ["cargo", "build", "-p", "pezhai-sevai"],
        cwd=REPO_ROOT,
        check=True,
    )
    subprocess.run(
        ["cargo", "build", "--release", "-p", "pezhai-sevai"],
        cwd=REPO_ROOT,
        check=True,
    )
    venv_dir = pathlib.Path(tempfile.mkdtemp(prefix="pezhai-bench-test-venv-"))
    venv.EnvBuilder(with_pip=True).create(venv_dir)
    if sys.platform == "win32":
        BENCHMARK_PYTHON = str(venv_dir / "Scripts" / "python.exe")
    else:
        BENCHMARK_PYTHON = str(venv_dir / "bin" / "python")
    subprocess.run(
        [BENCHMARK_PYTHON, "-m", "pip", "install", "-r", "requirements-bench.txt"],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def run_cli(
    *args: str,
    python_executable: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Runs the benchmark CLI in the repository root and captures its output."""
    merged_env = dict(os.environ)
    merged_env["PYTHONPATH"] = str(REPO_ROOT)
    if env is not None:
        merged_env.update(env)
    command = [python_executable or BENCHMARK_PYTHON or sys.executable, "-m", "benchmarks", *args]
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=merged_env,
        capture_output=True,
        text=True,
        check=False,
    )


def run_wrapper(
    bench_config: pathlib.Path,
    server_config: pathlib.Path,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Runs the portable shell wrapper with the benchmark venv first on PATH."""
    merged_env = dict(os.environ)
    python_dir = str(pathlib.Path(BENCHMARK_PYTHON).parent)
    merged_env["PATH"] = python_dir + os.pathsep + merged_env["PATH"]
    merged_env["PYTHON_BIN"] = BENCHMARK_PYTHON or sys.executable
    if env is not None:
        merged_env.update(env)
    return subprocess.run(
        ["sh", str(WRAPPER_SCRIPT), str(bench_config), str(server_config)],
        cwd=REPO_ROOT,
        env=merged_env,
        capture_output=True,
        text=True,
        check=False,
    )


def base_args(*workloads: str, json_output: pathlib.Path | None = None) -> list[str]:
    """Builds the common short-duration CLI arguments used by integration tests."""
    args = [
        "--server-binary",
        str(SERVER_BINARY),
        "--initial-key-count",
        "16",
        "--warmup-seconds",
        SHORT_WARMUP_SECONDS,
        "--measure-seconds",
        SHORT_MEASURE_SECONDS,
        "--connections",
        "2",
        "--inflight-per-connection",
        "2",
        "--preload-connections",
        "2",
        "--preload-inflight-per-connection",
        "2",
        "--scan-span-keys",
        "8",
        "--scan-page-max-records",
        "4",
        "--scan-page-max-bytes",
        "4096",
        "--seed",
        "11",
    ]
    for workload in workloads:
        args.extend(["--workload", workload])
    if json_output is not None:
        args.extend(["--json-output", str(json_output)])
    return args


class BenchmarkWorkflowTest(unittest.TestCase):
    """End-to-end smoke and validation coverage for the benchmark CLI."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_benchmark_python()

    def test_parse_size_range_rejects_bad_input(self) -> None:
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.parse_size_range("5", allow_zero=False, label="key")
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.parse_size_range("8:4", allow_zero=False, label="key")
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.parse_size_range("0:4", allow_zero=False, label="key")

    def test_parse_mixed_ratios_rejects_invalid_shapes(self) -> None:
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.parse_mixed_ratios("put=1,delete=1,get=1")
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.parse_mixed_ratios("put=1,delete=1,get=one,scan=1")
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.parse_mixed_ratios("put=0,delete=0,get=0,scan=0")

    def test_smoke_runs_every_workload(self) -> None:
        workloads = ["pure-put", "pure-delete", "pure-get", "pure-scan", "mixed"]
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = pathlib.Path(temp_dir) / "smoke.json"
            completed = run_cli(*base_args(*workloads, json_output=json_path))
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertEqual([entry["workload"] for entry in payload["workloads"]], workloads)
        for workload in payload["workloads"]:
            self.assertGreater(workload["tps"], 0)
            self.assertEqual(workload["effective_config"]["server_binary"], str(SERVER_BINARY))
            self.assertIn("p99.9", workload["latency_ms"])
            self.assertIn("listen_addr", workload["metadata"])
            self.assertIn("server_pid", workload["metadata"])

    def test_json_output_includes_required_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = pathlib.Path(temp_dir) / "result.json"
            completed = run_cli(*base_args("pure-get", json_output=json_path))
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["tool"], "kalanjiyam-pezhai-sevai-bench")
        workload = payload["workloads"][0]
        self.assertEqual(workload["workload"], "pure-get")
        self.assertIn("tps", workload)
        self.assertEqual(
            sorted(workload["latency_ms"]),
            ["p50", "p75", "p90", "p95", "p99", "p99.9"],
        )
        self.assertEqual(workload["effective_config"]["initial_key_count"], 16)
        self.assertGreaterEqual(workload["operation_counts"]["Get"], 1)

    def test_multiple_workloads_run_with_fresh_server_instances(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = pathlib.Path(temp_dir) / "isolated.json"
            completed = run_cli(*base_args("pure-put", "pure-get", json_output=json_path))
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        first, second = payload["workloads"]
        self.assertNotEqual(first["metadata"]["listen_addr"], second["metadata"]["listen_addr"])
        self.assertNotEqual(first["metadata"]["server_pid"], second["metadata"]["server_pid"])
        self.assertEqual(first["metadata"]["preload_put_count"], 16)
        self.assertEqual(second["metadata"]["preload_put_count"], 16)

    def test_pure_scan_reports_rpc_level_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = pathlib.Path(temp_dir) / "scan.json"
            completed = run_cli(
                *base_args("pure-scan", json_output=json_path),
                "--scan-page-max-records",
                "1",
                "--scan-page-max-bytes",
                "256",
            )
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        workload = payload["workloads"][0]
        self.assertGreaterEqual(workload["operation_counts"]["ScanStart"], 1)
        self.assertGreaterEqual(workload["operation_counts"]["ScanFetchNext"], 1)
        self.assertEqual(
            workload["metadata"]["workload_metadata"]["last_scan_range"]["target_span_keys"],
            8,
        )

    def test_scan_fetch_cap_is_enforced(self) -> None:
        completed = run_cli(
            *base_args("pure-scan"),
            "--initial-key-count",
            "256",
            "--scan-span-keys",
            "200",
            "--scan-page-max-records",
            "1",
            "--scan-page-max-bytes",
            "4096",
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("ScanFetchNext cap", completed.stderr)

    def test_pure_delete_maintains_steady_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = pathlib.Path(temp_dir) / "delete.json"
            completed = run_cli(
                *base_args("pure-delete", json_output=json_path),
                "--measure-seconds",
                "0.45",
            )
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        workload = payload["workloads"][0]
        self.assertEqual(workload["metadata"]["initial_existing_key_count"], 16)
        self.assertEqual(workload["metadata"]["final_existing_key_count"], 16)
        self.assertGreater(
            workload["metadata"]["workload_metadata"]["steady_state_replenishment_puts"],
            0,
        )

    def test_cli_rejects_missing_server_binary(self) -> None:
        completed = run_cli("--workload", "pure-get", "--initial-key-count", "1")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("--server-binary", completed.stderr)

    def test_cli_rejects_malformed_ranges(self) -> None:
        completed = run_cli(
            "--server-binary",
            str(SERVER_BINARY),
            "--workload",
            "pure-get",
            "--initial-key-count",
            "1",
            "--key-size-range",
            "9",
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("size range", completed.stderr)

    def test_cli_rejects_invalid_mixed_ratio_string(self) -> None:
        completed = run_cli(
            "--server-binary",
            str(SERVER_BINARY),
            "--workload",
            "mixed",
            "--initial-key-count",
            "1",
            "--mixed-ratios",
            "put=1,delete=1,get=1",
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("mixed ratios", completed.stderr)

    def test_cli_rejects_mixed_profile_and_ratio_conflict(self) -> None:
        completed = run_cli(
            "--server-binary",
            str(SERVER_BINARY),
            "--workload",
            "mixed",
            "--initial-key-count",
            "1",
            "--mixed-profile",
            "balanced",
            "--mixed-ratios",
            "put=1,delete=1,get=1,scan=1",
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("not both", completed.stderr)

    def test_cli_rejects_missing_protoc(self) -> None:
        env = os.environ.copy()
        env["PATH"] = ""
        completed = run_cli(*base_args("pure-get"), env=env)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("missing `protoc`", completed.stderr)

    def test_cli_rejects_missing_python_protobuf_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            venv_dir = pathlib.Path(temp_dir) / "clean-venv"
            venv.EnvBuilder(with_pip=False).create(venv_dir)
            if sys.platform == "win32":
                python_executable = venv_dir / "Scripts" / "python.exe"
            else:
                python_executable = venv_dir / "bin" / "python"
            completed = run_cli(
                *base_args("pure-get"),
                python_executable=str(python_executable),
            )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("missing Python protobuf runtime", completed.stderr)

    def test_cli_accepts_server_config_template(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = pathlib.Path(temp_dir) / "result.json"
            server_config = pathlib.Path(temp_dir) / "server.toml"
            server_config.write_text(
                "[engine]\n"
                'sync_mode = "manual"\n'
                "page_size_bytes = 8192\n"
                "\n"
                "[wal]\n"
                "segment_bytes = 2048\n"
                "\n"
                "[sevai]\n"
                'listen_addr = "127.0.0.1:7000"\n',
                encoding="utf-8",
            )
            completed = run_cli(
                *base_args("pure-get", json_output=json_path),
                "--server-config",
                str(server_config),
            )
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["workloads"][0]["workload"], "pure-get")

    def test_cli_prints_populate_and_measure_progress(self) -> None:
        completed = run_cli(
            *base_args("pure-get"),
            "--warmup-seconds",
            "1.1",
            "--measure-seconds",
            "1.1",
        )
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("populate:", completed.stdout)
        self.assertIn("measure:", completed.stdout)


class ServerConfigTemplateTest(unittest.TestCase):
    """Focused tests for the server TOML template merge path."""

    def test_replace_listen_addr_rewrites_only_sevai_value(self) -> None:
        original = (
            "[engine]\n"
            'sync_mode = "manual"\n'
            "page_size_bytes = 8192\n"
            "\n"
            "[sevai]\n"
            'listen_addr = "127.0.0.1:7000"\n'
            "\n"
            "[lsm]\n"
            "max_levels = 9\n"
        )

        rendered = tcp_bench.replace_listen_addr_in_server_config(original, "127.0.0.1:9000")

        self.assertIn('sync_mode = "manual"', rendered)
        self.assertIn("page_size_bytes = 8192", rendered)
        self.assertIn("max_levels = 9", rendered)
        self.assertIn('listen_addr = "127.0.0.1:9000"', rendered)
        self.assertNotIn('listen_addr = "127.0.0.1:7000"', rendered)

    def test_replace_listen_addr_requires_sevai_table(self) -> None:
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.replace_listen_addr_in_server_config(
                "[engine]\npage_size_bytes = 4096\n",
                "127.0.0.1:9000",
            )

    def test_replace_listen_addr_requires_listen_addr_key(self) -> None:
        with self.assertRaises(tcp_bench.CliError):
            tcp_bench.replace_listen_addr_in_server_config(
                "[sevai]\nmax_connections = 3\n",
                "127.0.0.1:9000",
            )


class WrapperWorkflowTest(unittest.TestCase):
    """Coverage for the portable shell wrapper and preset config files."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_benchmark_python()

    def test_wrapper_smoke_runs_short_benchmark(self) -> None:
        output_path = REPO_ROOT / "target" / "wrapper-smoke.json"
        output_path.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory() as temp_dir:
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "# comment\n"
                "\n"
                "WORKLOAD=pure-get\n"
                "INITIAL_KEY_COUNT=16\n"
                "WARMUP_SECONDS=0.15\n"
                "MEASURE_SECONDS=0.25\n"
                "SERVER_BINARY=${workspaceRoot}/target/debug/pezhai-sevai\n"
                "JSON_OUTPUT=${workspaceRoot}/target/wrapper-smoke.json\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("populate:", completed.stdout)
        self.assertIn("measure:", completed.stdout)
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["workloads"][0]["workload"], "pure-get")
        output_path.unlink(missing_ok=True)

    def test_wrapper_defaults_server_binary_to_release(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / "default-release.json"
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD=pure-get\n"
                "INITIAL_KEY_COUNT=16\n"
                "WARMUP_SECONDS=0.15\n"
                "MEASURE_SECONDS=0.25\n"
                f"JSON_OUTPUT={output_path}\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(
            payload["workloads"][0]["effective_config"]["server_binary"],
            str(RELEASE_SERVER_BINARY),
        )

    def test_wrapper_rejects_malformed_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD pure-get\nINITIAL_KEY_COUNT=16\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("KEY=VALUE", completed.stderr)

    def test_wrapper_rejects_unknown_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD=pure-get\nINITIAL_KEY_COUNT=16\nSURPRISE=1\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("unknown bench config key", completed.stderr)

    def test_wrapper_rejects_spaces_around_equals(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD =pure-get\nINITIAL_KEY_COUNT=16\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("malformed config key", completed.stderr)

    def test_wrapper_rejects_mixed_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD=mixed\n"
                "INITIAL_KEY_COUNT=16\n"
                "MIXED_PROFILE=balanced\n"
                "MIXED_RATIOS=put=1,delete=1,get=1,scan=1\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("mutually exclusive", completed.stderr)

    def test_wrapper_expands_workspace_root_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / "wrapper-expanded.json"
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD=pure-get\n"
                "INITIAL_KEY_COUNT=16\n"
                "WARMUP_SECONDS=0.15\n"
                "MEASURE_SECONDS=0.25\n"
                "SERVER_BINARY=${workspaceRoot}/target/debug/pezhai-sevai\n"
                f"JSON_OUTPUT={output_path}\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)
            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(
            payload["workloads"][0]["effective_config"]["server_binary"],
            str(SERVER_BINARY),
        )

    def test_wrapper_rejects_mixed_settings_for_non_mixed_workload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bench_config = pathlib.Path(temp_dir) / "bench.conf"
            bench_config.write_text(
                "WORKLOAD=pure-get\nINITIAL_KEY_COUNT=16\nMIXED_PROFILE=balanced\n",
                encoding="utf-8",
            )
            completed = run_wrapper(bench_config, SERVER_TEMPLATE)

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("only valid when WORKLOAD=mixed", completed.stderr)

    def test_all_preset_configs_parse_and_map_to_valid_keys(self) -> None:
        preset_paths = sorted(PRESET_CONFIG_DIR.glob("*.conf"))
        self.assertTrue(preset_paths)
        for preset_path in preset_paths:
            parsed = wrapper.parse_bench_config_text(
                preset_path.read_text(encoding="utf-8"),
                path=preset_path,
            )
            self.assertIn(parsed["WORKLOAD"], tcp_bench.WORKLOAD_CHOICES)
            self.assertEqual(parsed["INITIAL_KEY_COUNT"], "1000000")
            self.assertEqual(parsed["WARMUP_SECONDS"], "5")
            self.assertEqual(parsed["MEASURE_SECONDS"], "10")
            resolved = wrapper.resolve_wrapper_config(
                preset_path,
                SERVER_TEMPLATE,
                REPO_ROOT,
                validate_runtime=False,
            )
            argv = resolved.benchmark_argv()
            self.assertIn("--server-config", argv)
            self.assertIn(str(SERVER_TEMPLATE.resolve()), argv)
            self.assertIn("--server-binary", argv)
            self.assertIn(str(RELEASE_SERVER_BINARY.resolve()), argv)


class WrapperUnitTest(unittest.TestCase):
    """In-process coverage for the wrapper module's parser and entrypoint logic."""

    def test_parse_bench_config_accepts_comments_and_blank_lines(self) -> None:
        values = wrapper.parse_bench_config_text(
            "# comment\n\nWORKLOAD=pure-get\nINITIAL_KEY_COUNT=16\n",
            path=pathlib.Path("bench.conf"),
        )
        self.assertEqual(values["WORKLOAD"], "pure-get")
        self.assertEqual(values["INITIAL_KEY_COUNT"], "16")

    def test_parse_bench_config_rejects_duplicate_keys(self) -> None:
        with self.assertRaises(wrapper.WrapperError):
            wrapper.parse_bench_config_text(
                "WORKLOAD=pure-get\nWORKLOAD=pure-put\nINITIAL_KEY_COUNT=16\n",
                path=pathlib.Path("bench.conf"),
            )

    def test_expand_workspace_root_replaces_literal_token(self) -> None:
        expanded = wrapper.expand_workspace_root(
            "${workspaceRoot}/target/debug/pezhai-sevai",
            REPO_ROOT,
        )
        self.assertEqual(expanded, f"{REPO_ROOT}/target/debug/pezhai-sevai")

    def test_resolve_workspace_root_prefers_environment(self) -> None:
        with mock.patch.dict(os.environ, {"WORKSPACE_ROOT": "/tmp/workspace-root"}):
            self.assertEqual(
                wrapper.resolve_workspace_root(),
                pathlib.Path("/tmp/workspace-root").resolve(),
            )

    def test_resolve_wrapper_config_can_skip_runtime_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = pathlib.Path(temp_dir)
            server_binary = temp_root / "target" / "release" / "pezhai-sevai"
            server_binary.parent.mkdir(parents=True)
            server_binary.write_text("#!/bin/sh\n", encoding="utf-8")
            server_binary.chmod(0o755)
            bench_config = temp_root / "bench.conf"
            bench_config.write_text(
                "WORKLOAD=pure-get\nINITIAL_KEY_COUNT=16\n",
                encoding="utf-8",
            )
            server_config = temp_root / "server.toml"
            server_config.write_text(
                '[sevai]\nlisten_addr = "127.0.0.1:7000"\n',
                encoding="utf-8",
            )

            resolved = wrapper.resolve_wrapper_config(
                bench_config,
                server_config,
                temp_root,
                validate_runtime=False,
            )

        self.assertEqual(resolved.values["SERVER_BINARY"], str(server_binary.resolve()))

    def test_setup_instructions_reference_expected_commands(self) -> None:
        instructions = wrapper.setup_instructions(REPO_ROOT)
        self.assertIn("python3 -m venv", instructions)
        self.assertIn("requirements-bench.txt", instructions)
        self.assertIn("cargo build --release -p pezhai-sevai", instructions)

    def test_wrapper_main_prints_setup_hints_on_validation_error(self) -> None:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            exit_code = wrapper.main(["/definitely/missing.conf", str(SERVER_TEMPLATE)])
        self.assertEqual(exit_code, 2)
        self.assertIn("Setup commands:", stderr.getvalue())

    def test_wrapper_main_forwards_benchmark_argv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = pathlib.Path(temp_dir)
            resolved = wrapper.WrapperConfig(
                workspace_root=REPO_ROOT,
                bench_config=temp_root / "bench.conf",
                server_config=SERVER_TEMPLATE.resolve(),
                values={
                    "SERVER_BINARY": str(SERVER_BINARY),
                    "WORKLOAD": "pure-get",
                    "INITIAL_KEY_COUNT": "16",
                    "JSON_OUTPUT": str(temp_root / "result.json"),
                },
            )
            with mock.patch.object(wrapper, "resolve_wrapper_config", return_value=resolved):
                with mock.patch.object(tcp_bench, "main", return_value=0) as benchmark_main:
                    exit_code = wrapper.main(
                        [str(temp_root / "bench.conf"), str(SERVER_TEMPLATE)]
                    )

        self.assertEqual(exit_code, 0)
        benchmark_main.assert_called_once_with(resolved.benchmark_argv())

    def test_progress_formatters_include_expected_fields(self) -> None:
        populate = tcp_bench.format_populate_progress(
            tcp_bench.PreloadProgress(total_operations=10, completed_operations=5),
            elapsed_seconds=2.0,
        )
        measure = tcp_bench.format_measurement_progress(
            workload_name="pure-get",
            second=3,
            tps=123.4,
            latency_ms={"p50": 1.1, "p95": 2.2, "p99": 3.3},
        )
        self.assertIn("populate:", populate)
        self.assertIn("5/10", populate)
        self.assertIn("avg_tps=2.5", populate)
        self.assertIn("measure:", measure)
        self.assertIn("workload=pure-get", measure)
        self.assertIn("tps=123.4", measure)


class ScanPlannerTest(unittest.TestCase):
    """Focused tests for the scan-range planner used by the benchmark."""

    def test_scan_bounds_use_approximate_key_span(self) -> None:
        keys = [bytes([value]) for value in range(1, 10)]
        pool = tcp_bench.KeyPool(keys)
        start_key, end_key = asyncio.run(pool.choose_scan_bounds(random_source(), 3))
        self.assertIn(start_key, keys)
        if end_key is not None:
            self.assertGreaterEqual(keys.index(end_key) - keys.index(start_key), 1)


def random_source() -> tcp_bench.random.Random:
    """Builds a deterministic RNG without sharing test-global state."""
    return tcp_bench.random.Random(7)
