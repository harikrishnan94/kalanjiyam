"""Async TCP benchmark runner for the `pezhai-sevai` server."""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PROTO_DIR = REPO_ROOT / "crates" / "pezhai-sevai" / "proto"
PROTO_FILE = PROTO_DIR / "sevai.proto"
MAX_KEY_BYTES = 1024
MAX_SCAN_FETCH_CALLS = 100
READY_POLL_INTERVAL_SECONDS = 0.05
DEFAULT_WARMUP_SECONDS = 1.0
DEFAULT_MEASURE_SECONDS = 3.0
DEFAULT_CONNECTIONS = 8
DEFAULT_INFLIGHT_PER_CONNECTION = 8
DEFAULT_PRELOAD_CONNECTIONS = 4
DEFAULT_PRELOAD_INFLIGHT_PER_CONNECTION = 16
DEFAULT_SCAN_SPAN_KEYS = 128
DEFAULT_SCAN_PAGE_MAX_RECORDS = 32
DEFAULT_SCAN_PAGE_MAX_BYTES = 64 * 1024
OK_STATUS_CODE = "STATUS_CODE_OK"
MIXED_PROFILE_BALANCED = "balanced"
WORKLOAD_CHOICES = (
    "pure-put",
    "pure-delete",
    "pure-get",
    "pure-scan",
    "mixed",
)
RPC_NAME_BY_WORKLOAD = {
    "pure-put": "Put",
    "pure-delete": "Delete",
    "pure-get": "Get",
    "pure-scan": "ScanStart",
    "mixed": "mixed",
}
MIXED_PROFILES = {
    MIXED_PROFILE_BALANCED: {
        "put": 0.25,
        "delete": 0.25,
        "get": 0.25,
        "scan": 0.25,
    },
    "read-heavy": {
        "put": 0.10,
        "delete": 0.10,
        "get": 0.65,
        "scan": 0.15,
    },
    "write-heavy": {
        "put": 0.50,
        "delete": 0.20,
        "get": 0.20,
        "scan": 0.10,
    },
    "delete-heavy": {
        "put": 0.20,
        "delete": 0.45,
        "get": 0.20,
        "scan": 0.15,
    },
}

_PROTO_TEMP_DIR: tempfile.TemporaryDirectory[str] | None = None
_WIRE_MODULE: Any | None = None


class BenchmarkError(Exception):
    """Base exception for benchmark failures."""


class CliError(BenchmarkError):
    """Raised for invalid CLI inputs or missing local prerequisites."""


class WorkloadError(BenchmarkError):
    """Raised when one isolated workload cannot run to completion."""


@dataclasses.dataclass(frozen=True)
class SizeRange:
    """Inclusive size range used for benchmark key or value generation."""

    minimum: int
    maximum: int

    def sample(self, rng: random.Random) -> int:
        """Samples one size from the inclusive range."""
        if self.minimum == self.maximum:
            return self.minimum
        return rng.randint(self.minimum, self.maximum)

    def as_text(self) -> str:
        """Formats the range in the same `min:max` shape accepted by the CLI."""
        return f"{self.minimum}:{self.maximum}"


@dataclasses.dataclass(frozen=True)
class ConnectionShape:
    """Connection-count plus in-flight pipeline depth for one benchmark phase."""

    connections: int
    inflight_per_connection: int


@dataclasses.dataclass(frozen=True)
class ScanShape:
    """Scan planning knobs shared across pure-scan and mixed workloads."""

    span_keys: int
    max_records_per_page: int
    max_bytes_per_page: int


@dataclasses.dataclass(frozen=True)
class MixedShape:
    """Normalized mixed-workload operation ratios plus the source selection."""

    ratios: dict[str, float]
    source: str


@dataclasses.dataclass(frozen=True)
class BenchmarkConfig:
    """User-visible benchmark configuration after CLI validation."""

    server_binary: pathlib.Path
    workloads: tuple[str, ...]
    initial_key_count: int
    warmup_seconds: float
    measure_seconds: float
    key_size_range: SizeRange
    value_size_range: SizeRange
    measured_load: ConnectionShape
    preload_load: ConnectionShape
    scan_shape: ScanShape
    mixed_shape: MixedShape
    server_config: pathlib.Path | None
    json_output: pathlib.Path | None
    seed: int
    startup_timeout_seconds: float

    def shared_effective_config(self) -> dict[str, Any]:
        """Builds the workload-agnostic config payload emitted in JSON."""
        return {
            "server_binary": str(self.server_binary),
            "workloads": list(self.workloads),
            "initial_key_count": self.initial_key_count,
            "warmup_seconds": self.warmup_seconds,
            "measure_seconds": self.measure_seconds,
            "key_size_range": self.key_size_range.as_text(),
            "value_size_range": self.value_size_range.as_text(),
            "connections": self.measured_load.connections,
            "inflight_per_connection": self.measured_load.inflight_per_connection,
            "preload_connections": self.preload_load.connections,
            "preload_inflight_per_connection": self.preload_load.inflight_per_connection,
            "scan_span_keys": self.scan_shape.span_keys,
            "scan_page_max_records": self.scan_shape.max_records_per_page,
            "scan_page_max_bytes": self.scan_shape.max_bytes_per_page,
            "scan_fetch_next_cap": MAX_SCAN_FETCH_CALLS,
            "mixed_source": self.mixed_shape.source,
            "mixed_ratios": dict(self.mixed_shape.ratios),
            "seed": self.seed,
            "startup_timeout_seconds": self.startup_timeout_seconds,
        }


@dataclasses.dataclass(frozen=True)
class ReservedPutKey:
    """One reserved key chosen for a `Put`, including whether it fills a missing slot."""

    key: bytes
    from_missing: bool


@dataclasses.dataclass
class PhaseState:
    """Mutable benchmark phase visible to every measured worker."""

    phase: str = "warmup"


@dataclasses.dataclass
class PreloadProgress:
    """Tracks preload completion for periodic stdout progress reporting."""

    total_operations: int
    completed_operations: int = 0


@dataclasses.dataclass(frozen=True)
class MeasurementSnapshot:
    """One point-in-time view of measured RPC throughput and latency."""

    total_operations: int
    latency_ms: dict[str, float]

    @property
    def has_samples(self) -> bool:
        """Returns whether at least one measured RPC has completed."""
        return self.total_operations > 0


class KeyPool:
    """Tracks the benchmark's current view of existing keys and missing slots."""

    def __init__(self, all_keys_sorted: list[bytes]) -> None:
        self._all_keys_sorted = tuple(all_keys_sorted)
        self._order_index = {key: index for index, key in enumerate(self._all_keys_sorted)}
        self._existing_keys = list(self._all_keys_sorted)
        self._existing_index = {key: index for index, key in enumerate(self._existing_keys)}
        self._missing_keys: list[bytes] = []
        self._missing_index: dict[bytes, int] = {}
        self._reserved_keys: set[bytes] = set()
        self._lock = asyncio.Lock()

    async def existing_count(self) -> int:
        """Returns the benchmark's current estimated count of existing keys."""
        async with self._lock:
            return len(self._existing_keys)

    async def reserve_existing_key(self, rng: random.Random) -> bytes | None:
        """Reserves one existing key for a delete-target selection."""
        async with self._lock:
            return self._reserve_key_from(self._existing_keys, rng)

    async def choose_existing_key(self, rng: random.Random) -> bytes | None:
        """Reads one existing key without reserving it for mutation."""
        async with self._lock:
            if not self._existing_keys:
                return None
            return self._existing_keys[rng.randrange(len(self._existing_keys))]

    async def reserve_put_key(self, rng: random.Random, prefer_missing: bool) -> ReservedPutKey:
        """Reserves a key for a `Put`, filling missing keys first when requested."""
        async with self._lock:
            if prefer_missing:
                reserved = self._reserve_key_from(self._missing_keys, rng)
                if reserved is not None:
                    return ReservedPutKey(key=reserved, from_missing=True)

            reserved = self._reserve_key_from(self._existing_keys, rng)
            if reserved is not None:
                return ReservedPutKey(key=reserved, from_missing=False)

            reserved = self._reserve_key_from(self._missing_keys, rng)
            if reserved is not None:
                return ReservedPutKey(key=reserved, from_missing=True)

        raise WorkloadError("no keys are available for Put selection")

    async def mark_delete_success(self, key: bytes) -> None:
        """Moves a successfully deleted key into the missing-key set."""
        async with self._lock:
            if key in self._existing_index:
                self._swap_remove(self._existing_keys, self._existing_index, key)
                self._append(self._missing_keys, self._missing_index, key)
            self._reserved_keys.discard(key)

    async def mark_put_success(self, key: bytes, from_missing: bool) -> None:
        """Moves a successfully inserted key back into the existing-key set."""
        async with self._lock:
            if from_missing and key in self._missing_index:
                self._swap_remove(self._missing_keys, self._missing_index, key)
                self._append(self._existing_keys, self._existing_index, key)
            self._reserved_keys.discard(key)

    async def release_reservation(self, key: bytes) -> None:
        """Drops a temporary reservation when a request did not commit visible state."""
        async with self._lock:
            self._reserved_keys.discard(key)

    async def choose_scan_bounds(
        self,
        rng: random.Random,
        span_keys: int,
    ) -> tuple[bytes, bytes | None]:
        """Chooses one bounded scan range from the ordered keyspace."""
        async with self._lock:
            if not self._existing_keys:
                raise WorkloadError("pure-scan requires at least one existing key")
            anchor = self._existing_keys[rng.randrange(len(self._existing_keys))]
            start_index = self._order_index[anchor]
            end_index = min(start_index + span_keys, len(self._all_keys_sorted))
            start_key = self._all_keys_sorted[start_index]
            end_key = (
                None
                if end_index >= len(self._all_keys_sorted)
                else self._all_keys_sorted[end_index]
            )
            return start_key, end_key

    @staticmethod
    def _append(items: list[bytes], index_by_key: dict[bytes, int], key: bytes) -> None:
        index_by_key[key] = len(items)
        items.append(key)

    @staticmethod
    def _swap_remove(items: list[bytes], index_by_key: dict[bytes, int], key: bytes) -> None:
        remove_index = index_by_key.pop(key)
        last_key = items.pop()
        if remove_index < len(items):
            items[remove_index] = last_key
            index_by_key[last_key] = remove_index

    def _reserve_key_from(self, items: list[bytes], rng: random.Random) -> bytes | None:
        if not items:
            return None

        sample_budget = min(len(items), 64)
        for _ in range(sample_budget):
            key = items[rng.randrange(len(items))]
            if key not in self._reserved_keys:
                self._reserved_keys.add(key)
                return key

        for key in items:
            if key not in self._reserved_keys:
                self._reserved_keys.add(key)
                return key
        return None


class MeasurementCollector:
    """Collects RPC-level throughput, latency, and status metrics."""

    def __init__(self) -> None:
        self._latencies_ns: list[int] = []
        self._latencies_by_operation: dict[str, list[int]] = collections.defaultdict(list)
        self._operation_counts: collections.Counter[str] = collections.Counter()
        self._status_counts: collections.Counter[str] = collections.Counter()
        self._status_by_operation: dict[str, collections.Counter[str]] = collections.defaultdict(
            collections.Counter
        )
        self._status_messages: collections.Counter[str] = collections.Counter()

    def record(self, operation: str, response: Any, latency_ns: int, wire: Any) -> None:
        """Records one measured RPC response."""
        status_name = wire.StatusCode.Name(response.status.code)
        self._latencies_ns.append(latency_ns)
        self._latencies_by_operation[operation].append(latency_ns)
        self._operation_counts[operation] += 1
        self._status_counts[status_name] += 1
        self._status_by_operation[operation][status_name] += 1
        if response.status.message:
            bucket = f"{status_name}:{response.status.message}"
            self._status_messages[bucket] += 1

    def snapshot(self) -> MeasurementSnapshot:
        """Returns one point-in-time measurement summary for live progress output."""
        latencies_ns = list(self._latencies_ns)
        latency_ms = progress_latency_map(latencies_ns)
        return MeasurementSnapshot(
            total_operations=len(latencies_ns),
            latency_ms=latency_ms,
        )

    def snapshot_since(self, start_index: int) -> tuple[int, MeasurementSnapshot]:
        """Returns one interval snapshot plus the next latency index to resume from."""
        latencies_ns = self._latencies_ns[start_index:]
        next_index = len(self._latencies_ns)
        return next_index, MeasurementSnapshot(
            total_operations=len(latencies_ns),
            latency_ms=progress_latency_map(latencies_ns),
        )

    def build_result(
        self,
        workload_name: str,
        config: BenchmarkConfig,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Converts the collected metrics into the persisted result schema."""
        if not self._latencies_ns:
            raise WorkloadError(f"{workload_name} did not complete any measured RPCs")

        duration = config.measure_seconds
        total_operations = len(self._latencies_ns)
        return {
            "workload": workload_name,
            "primary_rpc": RPC_NAME_BY_WORKLOAD[workload_name],
            "tps": total_operations / duration,
            "latency_ms": percentile_map(self._latencies_ns),
            "effective_config": config.shared_effective_config(),
            "operation_counts": dict(sorted(self._operation_counts.items())),
            "status_breakdown": dict(sorted(self._status_counts.items())),
            "status_breakdown_by_operation": {
                operation: dict(sorted(counts.items()))
                for operation, counts in sorted(self._status_by_operation.items())
            },
            "operation_latency_ms": {
                operation: percentile_map(latencies)
                for operation, latencies in sorted(self._latencies_by_operation.items())
            },
            "status_messages": dict(sorted(self._status_messages.items())),
            "metadata": metadata,
        }


class AsyncRpcClient:
    """Pipelined TCP client that preserves one monotonic `request_id` sequence per connection."""

    def __init__(self, wire: Any, host: str, port: int, client_id: str) -> None:
        self._wire = wire
        self._host = host
        self._port = port
        self._client_id = client_id
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._next_request_id = 1
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._closed = False

    async def connect(self) -> None:
        """Opens the TCP stream and starts the background response reader."""
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._reader_task = asyncio.create_task(self._read_responses())

    async def close(self) -> None:
        """Closes the TCP stream and resolves all pending callers."""
        if self._closed:
            return
        self._closed = True

        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()

        if self._reader_task is not None:
            try:
                await self._reader_task
            except asyncio.IncompleteReadError:
                pass
            except ConnectionError:
                pass

        if self._pending:
            error = ConnectionError("client closed before all responses were received")
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()

    async def call(self, method_name: str, payload: Any, cancel_token: str | None = None) -> Any:
        """Sends one request envelope and waits for the matching terminal response."""
        if self._writer is None:
            raise WorkloadError("client connection is not open")

        loop = asyncio.get_running_loop()
        async with self._write_lock:
            request_id = self._next_request_id
            self._next_request_id += 1

            future: asyncio.Future[Any] = loop.create_future()
            self._pending[request_id] = future

            envelope = self._wire.RequestEnvelope()
            envelope.client_id = self._client_id
            envelope.request_id = request_id
            if cancel_token is not None:
                envelope.cancel_token = cancel_token
            getattr(envelope, method_name).CopyFrom(payload)

            payload_bytes = envelope.SerializeToString()
            frame = len(payload_bytes).to_bytes(4, "big") + payload_bytes
            self._writer.write(frame)
            await self._writer.drain()

        return await future

    async def _read_responses(self) -> None:
        """Reads framed protobuf responses until EOF, resolving pending waiters in order."""
        assert self._reader is not None
        try:
            while True:
                prefix = await self._reader.readexactly(4)
                frame_len = int.from_bytes(prefix, "big")
                payload = await self._reader.readexactly(frame_len)
                envelope = self._wire.ResponseEnvelope()
                envelope.ParseFromString(payload)
                future = self._pending.pop(envelope.request_id, None)
                if future is not None and not future.done():
                    future.set_result(envelope)
        except asyncio.IncompleteReadError as error:
            if self._closed and not error.partial:
                return
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()
            raise
        except Exception as error:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()
            raise


@dataclasses.dataclass
class ServerRun:
    """Process and address details for one isolated benchmark server instance."""

    process: asyncio.subprocess.Process
    host: str
    port: int
    config_path: pathlib.Path

    @property
    def listen_addr(self) -> str:
        """Formats the bound listen address for logs and JSON output."""
        return f"{self.host}:{self.port}"


class ServerHarness:
    """Starts and stops a fresh `pezhai-sevai` process for one workload."""

    def __init__(self, config: BenchmarkConfig, wire: Any) -> None:
        self._config = config
        self._wire = wire
        self._run: ServerRun | None = None

    async def start(self) -> ServerRun:
        """Creates a temp config, launches the binary, and waits for protocol readiness."""
        host, port = reserve_listen_addr()
        config_path = write_temp_server_config(host, port, self._config.server_config)
        process = await asyncio.create_subprocess_exec(
            str(self._config.server_binary),
            "--config",
            str(config_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        run = ServerRun(process=process, host=host, port=port, config_path=config_path)
        await self._wait_until_ready(run)
        self._run = run
        return run

    async def stop(self) -> dict[str, str]:
        """Requests a graceful shutdown and returns captured process output."""
        if self._run is None:
            return {"stdout": "", "stderr": ""}

        process = self._run.process
        if process.returncode is None:
            try:
                process.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), timeout=5.0)
        except asyncio.TimeoutError:
            process.kill()
            stdout_bytes, stderr_bytes = await process.communicate()

        try:
            self._run.config_path.unlink(missing_ok=True)
        finally:
            self._run = None

        return {
            "stdout": stdout_bytes.decode("utf-8", errors="replace"),
            "stderr": stderr_bytes.decode("utf-8", errors="replace"),
        }

    async def _wait_until_ready(self, run: ServerRun) -> None:
        """Polls the server listener until a `Stats` probe completes successfully."""
        deadline = time.perf_counter() + self._config.startup_timeout_seconds
        last_error: Exception | None = None
        while time.perf_counter() < deadline:
            if run.process.returncode is not None:
                stdout_bytes, stderr_bytes = await run.process.communicate()
                raise WorkloadError(
                    "server exited before becoming ready\n"
                    f"stdout:\n{stdout_bytes.decode('utf-8', errors='replace')}\n"
                    f"stderr:\n{stderr_bytes.decode('utf-8', errors='replace')}"
                )

            try:
                client = AsyncRpcClient(self._wire, run.host, run.port, "readiness-probe")
                await client.connect()
                response = await client.call("stats", self._wire.StatsRequest())
                await client.close()
                if self._wire.StatusCode.Name(response.status.code) != OK_STATUS_CODE:
                    raise WorkloadError(
                        "server listener opened but readiness probe failed with "
                        f"{self._wire.StatusCode.Name(response.status.code)}"
                    )
                return
            except Exception as error:
                last_error = error
                await asyncio.sleep(READY_POLL_INTERVAL_SECONDS)

        raise WorkloadError(f"server never became ready: {last_error}")


async def run_benchmark(config: BenchmarkConfig) -> dict[str, Any]:
    """Runs every selected workload in isolation and returns the persisted JSON payload."""
    wire = load_wire_module()
    results = []
    for index, workload_name in enumerate(config.workloads):
        workload_seed = config.seed + index
        workload_rng = random.Random(workload_seed)
        initial_records = build_initial_records(config, workload_rng)
        key_pool = KeyPool(sorted(key for key, _value in initial_records))
        harness = ServerHarness(config, wire)
        run = await harness.start()
        shutdown_output: dict[str, str] | None = None
        try:
            preload_duration = await preload_initial_keys(config, wire, run, initial_records)
            result = await run_workload(
                config=config,
                wire=wire,
                run=run,
                key_pool=key_pool,
                workload_name=workload_name,
                seed=workload_seed,
                preload_duration=preload_duration,
            )
            results.append(result)
        finally:
            shutdown_output = await harness.stop()
            if results:
                results[-1]["metadata"]["server_stdout"] = shutdown_output["stdout"]
                results[-1]["metadata"]["server_stderr"] = shutdown_output["stderr"]

    return {
        "tool": "kalanjiyam-pezhai-sevai-bench",
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "python_version": sys.version.split()[0],
        "workloads": results,
    }


async def run_workload(
    *,
    config: BenchmarkConfig,
    wire: Any,
    run: ServerRun,
    key_pool: KeyPool,
    workload_name: str,
    seed: int,
    preload_duration: float,
) -> dict[str, Any]:
    """Runs one isolated measured workload against a fresh server instance."""
    collector = MeasurementCollector()
    phase_state = PhaseState()
    measurement_report_task: asyncio.Task[None] | None = None
    clients = await open_clients(
        wire=wire,
        host=run.host,
        port=run.port,
        connection_shape=config.measured_load,
        client_prefix=f"{workload_name}-{seed}",
    )
    metadata = {
        "seed": seed,
        "server_pid": run.process.pid,
        "listen_addr": run.listen_addr,
        "preload_duration_seconds": preload_duration,
        "preload_put_count": config.initial_key_count,
        "scan_fetch_next_cap": MAX_SCAN_FETCH_CALLS,
        "initial_existing_key_count": config.initial_key_count,
        "workload_metadata": {},
    }
    metadata["workload_metadata"]["requested_connections"] = config.measured_load.connections
    metadata["workload_metadata"]["requested_inflight_per_connection"] = (
        config.measured_load.inflight_per_connection
    )
    metadata["workload_metadata"]["actual_connection_count"] = len(clients)

    stop_event = asyncio.Event()
    workers = []
    for client_index, client in enumerate(clients):
        worker_seed = seed * 10_000 + client_index
        workers.append(
            asyncio.create_task(
                measured_worker(
                    config=config,
                    wire=wire,
                    client=client,
                    key_pool=key_pool,
                    collector=collector,
                    workload_name=workload_name,
                    phase_state=phase_state,
                    stop_event=stop_event,
                    rng=random.Random(worker_seed),
                    metadata=metadata["workload_metadata"],
                )
            )
        )

    try:
        await asyncio.sleep(config.warmup_seconds)
        phase_state.phase = "measure"
        measurement_report_task = asyncio.create_task(
            report_measurement_progress(workload_name, collector)
        )
        await asyncio.sleep(config.measure_seconds)
        phase_state.phase = "done"
        stop_event.set()
        await asyncio.gather(*workers)
        metadata["final_existing_key_count"] = await key_pool.existing_count()
    finally:
        if measurement_report_task is not None:
            measurement_report_task.cancel()
            try:
                await measurement_report_task
            except asyncio.CancelledError:
                pass
        await close_clients(clients)

    return collector.build_result(workload_name, config, metadata)


async def measured_worker(
    *,
    config: BenchmarkConfig,
    wire: Any,
    client: AsyncRpcClient,
    key_pool: KeyPool,
    collector: MeasurementCollector,
    workload_name: str,
    phase_state: PhaseState,
    stop_event: asyncio.Event,
    rng: random.Random,
    metadata: dict[str, Any],
) -> None:
    """Maintains one in-flight measured request slot for the selected workload."""
    while True:
        phase = phase_state.phase
        if phase == "done" or stop_event.is_set():
            return

        if workload_name == "pure-put":
            await perform_put_cycle(
                config=config,
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                prefer_missing=True,
            )
            continue

        if workload_name == "pure-get":
            await perform_get_cycle(
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
            )
            continue

        if workload_name == "pure-delete":
            await perform_delete_cycle(
                config=config,
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                replenish=True,
                metadata=metadata,
            )
            continue

        if workload_name == "pure-scan":
            await perform_scan_cycle(
                config=config,
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                metadata=metadata,
            )
            continue

        operation = choose_mixed_operation(config.mixed_shape.ratios, rng)
        if operation == "put":
            await perform_put_cycle(
                config=config,
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                prefer_missing=True,
            )
        elif operation == "delete":
            deleted = await perform_delete_cycle(
                config=config,
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                replenish=False,
                metadata=metadata,
                fail_if_empty=False,
            )
            if not deleted:
                metadata["mixed_replenishment_put_overrides"] = (
                    metadata.get("mixed_replenishment_put_overrides", 0) + 1
                )
                await perform_put_cycle(
                    config=config,
                    wire=wire,
                    client=client,
                    key_pool=key_pool,
                    collector=collector,
                    phase=phase,
                    rng=rng,
                    prefer_missing=True,
                )
        elif operation == "get":
            got_value = await perform_get_cycle(
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                fail_if_empty=False,
            )
            if not got_value:
                metadata["mixed_replenishment_put_overrides"] = (
                    metadata.get("mixed_replenishment_put_overrides", 0) + 1
                )
                await perform_put_cycle(
                    config=config,
                    wire=wire,
                    client=client,
                    key_pool=key_pool,
                    collector=collector,
                    phase=phase,
                    rng=rng,
                    prefer_missing=True,
                )
        else:
            scanned = await perform_scan_cycle(
                config=config,
                wire=wire,
                client=client,
                key_pool=key_pool,
                collector=collector,
                phase=phase,
                rng=rng,
                metadata=metadata,
                fail_if_empty=False,
            )
            if not scanned:
                metadata["mixed_replenishment_put_overrides"] = (
                    metadata.get("mixed_replenishment_put_overrides", 0) + 1
                )
                await perform_put_cycle(
                    config=config,
                    wire=wire,
                    client=client,
                    key_pool=key_pool,
                    collector=collector,
                    phase=phase,
                    rng=rng,
                    prefer_missing=True,
                )


async def perform_put_cycle(
    *,
    config: BenchmarkConfig,
    wire: Any,
    client: AsyncRpcClient,
    key_pool: KeyPool,
    collector: MeasurementCollector,
    phase: str,
    rng: random.Random,
    prefer_missing: bool,
) -> None:
    """Executes one measured `Put` and updates the key pool on success."""
    reserved = await key_pool.reserve_put_key(rng, prefer_missing=prefer_missing)
    payload = wire.PutRequest()
    payload.key = reserved.key
    payload.value = generate_value(config.value_size_range, rng)
    response = await timed_call(
        collector=collector,
        wire=wire,
        client=client,
        operation="Put",
        method_name="put",
        payload=payload,
        phase=phase,
    )
    if wire.StatusCode.Name(response.status.code) == OK_STATUS_CODE:
        await key_pool.mark_put_success(reserved.key, reserved.from_missing)
    else:
        await key_pool.release_reservation(reserved.key)


async def perform_get_cycle(
    *,
    wire: Any,
    client: AsyncRpcClient,
    key_pool: KeyPool,
    collector: MeasurementCollector,
    phase: str,
    rng: random.Random,
    fail_if_empty: bool = True,
) -> bool:
    """Executes one measured `Get` against a currently existing key."""
    key = await key_pool.choose_existing_key(rng)
    if key is None:
        if fail_if_empty:
            raise WorkloadError("Get requires at least one existing key")
        return False
    payload = wire.GetRequest()
    payload.key = key
    await timed_call(
        collector=collector,
        wire=wire,
        client=client,
        operation="Get",
        method_name="get",
        payload=payload,
        phase=phase,
    )
    return True


async def perform_delete_cycle(
    *,
    config: BenchmarkConfig,
    wire: Any,
    client: AsyncRpcClient,
    key_pool: KeyPool,
    collector: MeasurementCollector,
    phase: str,
    rng: random.Random,
    replenish: bool,
    metadata: dict[str, Any],
    fail_if_empty: bool = True,
) -> bool:
    """Executes one measured `Delete`, with optional steady-state replenishment `Put`s."""
    key = await key_pool.reserve_existing_key(rng)
    if key is None:
        if fail_if_empty:
            raise WorkloadError("Delete requires at least one existing key")
        return False
    payload = wire.DeleteRequest()
    payload.key = key
    response = await timed_call(
        collector=collector,
        wire=wire,
        client=client,
        operation="Delete",
        method_name="delete",
        payload=payload,
        phase=phase,
    )
    if wire.StatusCode.Name(response.status.code) != OK_STATUS_CODE:
        await key_pool.release_reservation(key)
        return True

    await key_pool.mark_delete_success(key)
    if not replenish:
        return True

    metadata["steady_state_replenishment_puts"] = (
        metadata.get("steady_state_replenishment_puts", 0) + 1
    )
    restore_payload = wire.PutRequest()
    restore_payload.key = key
    restore_payload.value = generate_value(config.value_size_range, rng)
    restore_response = await timed_call(
        collector=collector,
        wire=wire,
        client=client,
        operation="Put",
        method_name="put",
        payload=restore_payload,
        phase=phase,
    )
    if wire.StatusCode.Name(restore_response.status.code) == OK_STATUS_CODE:
        await key_pool.mark_put_success(key, from_missing=True)
    return True


async def perform_scan_cycle(
    *,
    config: BenchmarkConfig,
    wire: Any,
    client: AsyncRpcClient,
    key_pool: KeyPool,
    collector: MeasurementCollector,
    phase: str,
    rng: random.Random,
    metadata: dict[str, Any],
    fail_if_empty: bool = True,
) -> bool:
    """Executes one full paged scan session while counting every RPC individually."""
    if not fail_if_empty and await key_pool.existing_count() == 0:
        return False
    start_key, end_key = await key_pool.choose_scan_bounds(rng, config.scan_shape.span_keys)
    request = wire.ScanStartRequest()
    request.start_bound.CopyFrom(make_finite_bound(wire, start_key))
    if end_key is None:
        request.end_bound.CopyFrom(make_pos_inf_bound(wire))
    else:
        request.end_bound.CopyFrom(make_finite_bound(wire, end_key))
    request.max_records_per_page = config.scan_shape.max_records_per_page
    request.max_bytes_per_page = config.scan_shape.max_bytes_per_page

    metadata["scan_sessions_started"] = metadata.get("scan_sessions_started", 0) + 1
    metadata["last_scan_range"] = {
        "start_bound_hex": start_key.hex(),
        "end_bound_hex": None if end_key is None else end_key.hex(),
        "target_span_keys": config.scan_shape.span_keys,
    }

    response = await timed_call(
        collector=collector,
        wire=wire,
        client=client,
        operation="ScanStart",
        method_name="scan_start",
        payload=request,
        phase=phase,
    )
    if wire.StatusCode.Name(response.status.code) != OK_STATUS_CODE:
        return True

    if response.WhichOneof("payload") != "scan_start":
        raise WorkloadError("ScanStart returned an unexpected payload type")
    scan_id = response.scan_start.scan_id

    for fetch_count in range(1, MAX_SCAN_FETCH_CALLS + 1):
        fetch_request = wire.ScanFetchNextRequest()
        fetch_request.scan_id = scan_id
        fetch_response = await timed_call(
            collector=collector,
            wire=wire,
            client=client,
            operation="ScanFetchNext",
            method_name="scan_fetch_next",
            payload=fetch_request,
            phase=phase,
        )
        if wire.StatusCode.Name(fetch_response.status.code) != OK_STATUS_CODE:
            return True
        if fetch_response.WhichOneof("payload") != "scan_fetch_next":
            raise WorkloadError("ScanFetchNext returned an unexpected payload type")
        if fetch_response.scan_fetch_next.eof:
            metadata["scan_fetch_next_rpc_count"] = (
                metadata.get("scan_fetch_next_rpc_count", 0) + fetch_count
            )
            return True

    metadata["scan_fetch_cap_hits"] = metadata.get("scan_fetch_cap_hits", 0) + 1
    raise WorkloadError(
        f"scan session reached the {MAX_SCAN_FETCH_CALLS}-RPC ScanFetchNext cap before EOF"
    )


async def timed_call(
    *,
    collector: MeasurementCollector,
    wire: Any,
    client: AsyncRpcClient,
    operation: str,
    method_name: str,
    payload: Any,
    phase: str,
) -> Any:
    """Times one RPC and records it only when the benchmark is in the measure phase."""
    started = time.perf_counter_ns()
    try:
        response = await client.call(method_name, payload)
    except Exception as error:
        raise WorkloadError(f"{operation} failed with a transport error: {error}") from error

    if phase == "measure":
        collector.record(operation, response, time.perf_counter_ns() - started, wire)
    return response


async def report_populate_progress(progress: PreloadProgress, started: float) -> None:
    """Prints one preload progress line per second while initial keys are loading."""
    while True:
        await asyncio.sleep(1.0)
        print(
            format_populate_progress(
                progress,
                elapsed_seconds=time.perf_counter() - started,
            ),
            flush=True,
        )


async def report_measurement_progress(
    workload_name: str,
    collector: MeasurementCollector,
) -> None:
    """Prints one live measurement line per second while the measure window is active."""
    last_index = 0
    second = 0
    print(
        format_measurement_progress(
            workload_name=workload_name,
            second=second,
            tps=0.0,
            latency_ms=progress_latency_map([]),
        ),
        flush=True,
    )
    while True:
        started = time.perf_counter()
        await asyncio.sleep(1.0)
        second += 1
        interval_seconds = max(time.perf_counter() - started, 1e-9)
        last_index, snapshot = collector.snapshot_since(last_index)
        print(
            format_measurement_progress(
                workload_name=workload_name,
                second=second,
                tps=snapshot.total_operations / interval_seconds,
                latency_ms=snapshot.latency_ms,
            ),
            flush=True,
        )


async def preload_initial_keys(
    config: BenchmarkConfig,
    wire: Any,
    run: ServerRun,
    initial_records: list[tuple[bytes, bytes]],
) -> float:
    """Loads the requested initial key/value pairs before warmup begins."""
    clients = await open_clients(
        wire=wire,
        host=run.host,
        port=run.port,
        connection_shape=config.preload_load,
        client_prefix="preload",
    )
    pending = collections.deque(initial_records)
    lock = asyncio.Lock()
    progress = PreloadProgress(total_operations=len(initial_records))
    started = time.perf_counter()
    print(format_populate_progress(progress, elapsed_seconds=0.0), flush=True)
    progress_task = asyncio.create_task(report_populate_progress(progress, started))

    async def preload_worker(client: AsyncRpcClient) -> None:
        while True:
            async with lock:
                if not pending:
                    return
                key, value = pending.popleft()

            payload = wire.PutRequest()
            payload.key = key
            payload.value = value
            response = await client.call("put", payload)
            if wire.StatusCode.Name(response.status.code) != OK_STATUS_CODE:
                raise WorkloadError(
                    "preload Put failed with "
                    f"{wire.StatusCode.Name(response.status.code)}: {response.status.message}"
                )
            progress.completed_operations += 1

    tasks = [asyncio.create_task(preload_worker(client)) for client in clients]

    try:
        await asyncio.gather(*tasks)
    finally:
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass
        await close_clients(clients)
    elapsed_seconds = time.perf_counter() - started
    print(
        format_populate_progress(progress, elapsed_seconds=elapsed_seconds),
        flush=True,
    )
    return elapsed_seconds


async def open_clients(
    *,
    wire: Any,
    host: str,
    port: int,
    connection_shape: ConnectionShape,
    client_prefix: str,
) -> list[AsyncRpcClient]:
    """Creates one connection per logical in-flight lane.

    The current TCP adapter forwards same-connection requests into the owner actor from
    independently scheduled tasks, so preserving monotonic `request_id` ordering requires one
    logical request lane per TCP connection.
    """
    total_connections = connection_shape.connections * connection_shape.inflight_per_connection
    clients = [
        AsyncRpcClient(wire, host, port, f"{client_prefix}-conn-{index}")
        for index in range(total_connections)
    ]
    await asyncio.gather(*(client.connect() for client in clients))
    return clients


async def close_clients(clients: list[AsyncRpcClient]) -> None:
    """Closes every benchmark client connection."""
    await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)


def build_initial_records(
    config: BenchmarkConfig,
    rng: random.Random,
) -> list[tuple[bytes, bytes]]:
    """Generates the fixed logical key universe used by preload and steady-state writes."""
    if config.initial_key_count < 1:
        raise CliError("--initial-key-count must be at least 1")

    records = []
    seen: set[bytes] = set()
    for index in range(config.initial_key_count):
        key = generate_unique_key(index, config.key_size_range.sample(rng))
        if key in seen:
            raise CliError("generated duplicate keys; widen --key-size-range and retry")
        seen.add(key)
        records.append((key, generate_value(config.value_size_range, rng)))
    return records


def generate_unique_key(key_id: int, key_size: int) -> bytes:
    """Builds a deterministic unique key from the logical key identifier."""
    if key_size < 1:
        raise CliError("key sizes must be at least 1 byte")
    if key_size > MAX_KEY_BYTES:
        raise CliError(f"key sizes must be at most {MAX_KEY_BYTES} bytes")

    seed = key_id.to_bytes(8, "big", signed=False)
    output = bytearray()
    counter = 0
    while len(output) < key_size:
        digest = hashlib.sha256(seed + counter.to_bytes(4, "big", signed=False)).digest()
        output.extend(digest)
        counter += 1
    return bytes(output[:key_size])


def generate_value(value_size_range: SizeRange, rng: random.Random) -> bytes:
    """Samples one random value payload for a `Put`."""
    size = value_size_range.sample(rng)
    return rng.randbytes(size)


def make_finite_bound(wire: Any, key: bytes) -> Any:
    """Encodes one finite scan bound."""
    bound = wire.Bound()
    bound.finite = key
    return bound


def make_pos_inf_bound(wire: Any) -> Any:
    """Encodes one positive-infinity scan bound."""
    bound = wire.Bound()
    bound.pos_inf.SetInParent()
    return bound


def reserve_listen_addr() -> tuple[str, int]:
    """Reserves one ephemeral localhost listen address for a fresh server process."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        host, port = listener.getsockname()
    return str(host), int(port)


def write_temp_server_config(
    host: str,
    port: int,
    server_config: pathlib.Path | None = None,
) -> pathlib.Path:
    """Writes the temporary config consumed by one isolated server process."""
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        prefix="pezhai-sevai-bench-",
        suffix=".toml",
        delete=False,
    )
    with handle:
        handle.write(render_server_config(host, port, server_config))
    return pathlib.Path(handle.name)


def render_server_config(
    host: str,
    port: int,
    server_config: pathlib.Path | None = None,
) -> str:
    """Builds one server config body from defaults or a template TOML file."""
    listen_addr = f"{host}:{port}"
    if server_config is None:
        return (
            "[engine]\n"
            'sync_mode = "per_write"\n'
            "\n"
            "[sevai]\n"
            f'listen_addr = "{listen_addr}"\n'
        )

    config_text = load_server_config_template_text(server_config)
    return replace_listen_addr_in_server_config(config_text, listen_addr)


def load_server_config_template_text(server_config: pathlib.Path) -> str:
    """Loads and validates one server TOML template used as benchmark input."""
    try:
        config_text = server_config.read_text(encoding="utf-8")
    except OSError as error:
        raise CliError(f"failed to read --server-config {server_config}: {error}") from error

    try:
        parsed = tomllib.loads(config_text)
    except tomllib.TOMLDecodeError as error:
        raise CliError(f"failed to parse --server-config {server_config}: {error}") from error

    sevai_config = parsed.get("sevai")
    if not isinstance(sevai_config, dict):
        raise CliError("--server-config must contain a [sevai] table")
    if "listen_addr" not in sevai_config:
        raise CliError("--server-config must define sevai.listen_addr")
    return config_text


def replace_listen_addr_in_server_config(config_text: str, listen_addr: str) -> str:
    """Rewrites only `sevai.listen_addr` in a parsed TOML template."""
    output_lines = []
    in_sevai_table = False
    found_sevai_table = False
    replaced_listen_addr = False

    for line in config_text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_sevai_table = stripped == "[sevai]"
            found_sevai_table = found_sevai_table or in_sevai_table

        if in_sevai_table and "=" in line:
            key, _separator, _value = line.partition("=")
            if key.strip() == "listen_addr":
                indentation = key[: len(key) - len(key.lstrip())]
                newline = "\n" if line.endswith("\n") else ""
                output_lines.append(f'{indentation}listen_addr = "{listen_addr}"{newline}')
                replaced_listen_addr = True
                continue

        output_lines.append(line)

    if not found_sevai_table:
        raise CliError("--server-config must contain a [sevai] table")
    if not replaced_listen_addr:
        raise CliError("--server-config must define sevai.listen_addr")

    return "".join(output_lines)


def load_wire_module() -> Any:
    """Generates the Python protobuf bindings at runtime and imports them once."""
    global _PROTO_TEMP_DIR, _WIRE_MODULE
    if _WIRE_MODULE is not None:
        return _WIRE_MODULE

    ensure_protobuf_runtime()
    protoc = resolve_protoc()
    _PROTO_TEMP_DIR = tempfile.TemporaryDirectory(prefix="kalanjiyam-bench-proto-")
    output_dir = pathlib.Path(_PROTO_TEMP_DIR.name)
    command = [
        protoc,
        f"--proto_path={PROTO_DIR}",
        f"--python_out={output_dir}",
        PROTO_FILE.name,
    ]
    completed = subprocess.run(
        command,
        cwd=PROTO_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise CliError(
            "runtime protoc generation failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    module_path = output_dir / "sevai_pb2.py"
    spec = importlib.util.spec_from_file_location("kalanjiyam_bench_sevai_pb2", module_path)
    if spec is None or spec.loader is None:
        raise CliError(f"failed to import generated protobuf module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _WIRE_MODULE = module
    return module


def ensure_protobuf_runtime() -> None:
    """Fails fast when the Python protobuf runtime is unavailable."""
    try:
        import google.protobuf  # noqa: F401
    except ModuleNotFoundError as error:
        raise CliError(
            "missing Python protobuf runtime; install requirements-bench.txt before running the "
            "benchmark"
        ) from error


def resolve_protoc() -> str:
    """Finds the local `protoc` binary used for runtime Python binding generation."""
    configured = os.environ.get("PROTOC")
    if configured:
        if pathlib.Path(configured).exists():
            return configured
        located = shutil.which(configured)
        if located is not None:
            return located
        raise CliError(f"PROTOC points to `{configured}`, but that binary is not available")

    located = shutil.which("protoc")
    if located is None:
        raise CliError("missing `protoc`; install it or set PROTOC to a working binary")
    return located


def percentile_map(latencies_ns: list[int]) -> dict[str, float]:
    """Builds the required percentile summary in milliseconds."""
    sorted_latencies = sorted(latencies_ns)
    return {
        "p50": percentile_ms(sorted_latencies, 50.0),
        "p75": percentile_ms(sorted_latencies, 75.0),
        "p90": percentile_ms(sorted_latencies, 90.0),
        "p95": percentile_ms(sorted_latencies, 95.0),
        "p99": percentile_ms(sorted_latencies, 99.0),
        "p99.9": percentile_ms(sorted_latencies, 99.9),
    }


def percentile_ms(sorted_latencies_ns: list[int], percentile: float) -> float:
    """Computes a nearest-rank percentile and formats it in milliseconds."""
    if not sorted_latencies_ns:
        return 0.0
    rank = max(1, math.ceil((percentile / 100.0) * len(sorted_latencies_ns)))
    value_ns = sorted_latencies_ns[rank - 1]
    return round(value_ns / 1_000_000.0, 6)


def choose_mixed_operation(ratios: dict[str, float], rng: random.Random) -> str:
    """Samples one logical mixed-workload operation from normalized ratios."""
    threshold = rng.random()
    running = 0.0
    last_name = "get"
    for name, value in ratios.items():
        running += value
        last_name = name
        if threshold <= running:
            return name
    return last_name


def parse_args(argv: list[str] | None) -> BenchmarkConfig:
    """Parses and validates the benchmark CLI."""
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks",
        description=("Async TCP benchmark for the pezhai-sevai server."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--server-binary",
        required=True,
        help="Path to the compiled pezhai-sevai binary.",
    )
    parser.add_argument(
        "--server-config",
        help="Optional server TOML template whose sevai.listen_addr will be overridden per run.",
    )
    parser.add_argument(
        "--workload",
        action="append",
        required=True,
        choices=WORKLOAD_CHOICES,
        help="One workload to run. Repeat the flag to run multiple workloads in isolation.",
    )
    parser.add_argument(
        "--initial-key-count",
        required=True,
        type=int,
        help="Number of keys to preload.",
    )
    parser.add_argument("--warmup-seconds", type=float, default=DEFAULT_WARMUP_SECONDS)
    parser.add_argument("--measure-seconds", type=float, default=DEFAULT_MEASURE_SECONDS)
    parser.add_argument("--connections", type=int, default=DEFAULT_CONNECTIONS)
    parser.add_argument(
        "--inflight-per-connection",
        type=int,
        default=DEFAULT_INFLIGHT_PER_CONNECTION,
    )
    parser.add_argument("--preload-connections", type=int, default=DEFAULT_PRELOAD_CONNECTIONS)
    parser.add_argument(
        "--preload-inflight-per-connection",
        type=int,
        default=DEFAULT_PRELOAD_INFLIGHT_PER_CONNECTION,
    )
    parser.add_argument(
        "--key-size-range",
        default="128:128",
        help="Inclusive key-size range as min:max.",
    )
    parser.add_argument(
        "--value-size-range",
        default="100:1024",
        help="Inclusive value-size range as min:max.",
    )
    parser.add_argument("--scan-span-keys", type=int, default=DEFAULT_SCAN_SPAN_KEYS)
    parser.add_argument("--scan-page-max-records", type=int, default=DEFAULT_SCAN_PAGE_MAX_RECORDS)
    parser.add_argument("--scan-page-max-bytes", type=int, default=DEFAULT_SCAN_PAGE_MAX_BYTES)
    parser.add_argument("--mixed-profile", choices=sorted(MIXED_PROFILES))
    parser.add_argument("--mixed-ratios", help="Mixed ratios as put=..,delete=..,get=..,scan=..")
    parser.add_argument("--json-output", help="Optional path for the JSON results artifact.")
    parser.add_argument("--seed", type=int, default=1, help="Base RNG seed for workload planning.")
    parser.add_argument(
        "--startup-timeout-seconds",
        type=float,
        default=10.0,
        help="Seconds to wait for a fresh server process to become ready.",
    )

    parsed = parser.parse_args(argv)
    return build_config(parsed)


def build_config(parsed: argparse.Namespace) -> BenchmarkConfig:
    """Converts parsed CLI arguments into validated benchmark config."""
    server_binary = pathlib.Path(parsed.server_binary).expanduser().resolve()
    if not server_binary.exists():
        raise CliError(f"--server-binary does not exist: {server_binary}")
    if not os.access(server_binary, os.X_OK):
        raise CliError(f"--server-binary is not executable: {server_binary}")

    server_config = None
    if parsed.server_config is not None:
        server_config = pathlib.Path(parsed.server_config).expanduser().resolve()
        if not server_config.exists():
            raise CliError(f"--server-config does not exist: {server_config}")
        if not server_config.is_file():
            raise CliError(f"--server-config is not a file: {server_config}")
        load_server_config_template_text(server_config)

    workloads = tuple(parsed.workload)
    if not workloads:
        raise CliError("at least one --workload must be provided")
    if parsed.initial_key_count < 1:
        raise CliError("--initial-key-count must be at least 1")
    if parsed.warmup_seconds <= 0 or parsed.measure_seconds <= 0:
        raise CliError("warmup and measure windows must both be greater than zero")
    if parsed.connections < 1 or parsed.inflight_per_connection < 1:
        raise CliError("measured-load connection and in-flight counts must both be at least 1")
    if parsed.preload_connections < 1 or parsed.preload_inflight_per_connection < 1:
        raise CliError("preload connection and in-flight counts must both be at least 1")
    if parsed.scan_span_keys < 1:
        raise CliError("--scan-span-keys must be at least 1")
    if parsed.scan_page_max_records < 1 or parsed.scan_page_max_bytes < 1:
        raise CliError("scan page limits must both be at least 1")
    if parsed.startup_timeout_seconds <= 0:
        raise CliError("--startup-timeout-seconds must be greater than zero")

    key_size_range = parse_size_range(parsed.key_size_range, allow_zero=False, label="key")
    if key_size_range.maximum > MAX_KEY_BYTES:
        raise CliError(f"key sizes must be at most {MAX_KEY_BYTES} bytes")
    value_size_range = parse_size_range(parsed.value_size_range, allow_zero=True, label="value")

    if parsed.mixed_profile and parsed.mixed_ratios:
        raise CliError("mixed mode accepts either --mixed-profile or --mixed-ratios, not both")
    mixed_shape = resolve_mixed_shape(parsed.mixed_profile, parsed.mixed_ratios)

    json_output = None
    if parsed.json_output is not None:
        json_output = pathlib.Path(parsed.json_output).expanduser().resolve()

    return BenchmarkConfig(
        server_binary=server_binary,
        workloads=workloads,
        initial_key_count=parsed.initial_key_count,
        warmup_seconds=parsed.warmup_seconds,
        measure_seconds=parsed.measure_seconds,
        key_size_range=key_size_range,
        value_size_range=value_size_range,
        measured_load=ConnectionShape(
            connections=parsed.connections,
            inflight_per_connection=parsed.inflight_per_connection,
        ),
        preload_load=ConnectionShape(
            connections=parsed.preload_connections,
            inflight_per_connection=parsed.preload_inflight_per_connection,
        ),
        scan_shape=ScanShape(
            span_keys=parsed.scan_span_keys,
            max_records_per_page=parsed.scan_page_max_records,
            max_bytes_per_page=parsed.scan_page_max_bytes,
        ),
        mixed_shape=mixed_shape,
        server_config=server_config,
        json_output=json_output,
        seed=parsed.seed,
        startup_timeout_seconds=parsed.startup_timeout_seconds,
    )


def parse_size_range(text: str, *, allow_zero: bool, label: str) -> SizeRange:
    """Parses one inclusive `min:max` size range string."""
    pieces = text.split(":")
    if len(pieces) != 2:
        raise CliError(f"{label} size range must use the form min:max")
    try:
        minimum = int(pieces[0])
        maximum = int(pieces[1])
    except ValueError as error:
        raise CliError(f"{label} size range must contain integers") from error

    lower_bound = 0 if allow_zero else 1
    if minimum < lower_bound or maximum < lower_bound:
        qualifier = "non-negative" if allow_zero else "positive"
        raise CliError(f"{label} sizes must be {qualifier}")
    if minimum > maximum:
        raise CliError(f"{label} size range must satisfy min <= max")
    return SizeRange(minimum=minimum, maximum=maximum)


def resolve_mixed_shape(profile_name: str | None, ratio_text: str | None) -> MixedShape:
    """Resolves the effective mixed-workload ratios and records their source."""
    if ratio_text is not None:
        ratios = parse_mixed_ratios(ratio_text)
        return MixedShape(ratios=ratios, source="ratio-string")

    resolved_profile = profile_name or MIXED_PROFILE_BALANCED
    return MixedShape(
        ratios=normalize_ratios(MIXED_PROFILES[resolved_profile]),
        source=f"profile:{resolved_profile}",
    )


def parse_mixed_ratios(text: str) -> dict[str, float]:
    """Parses and normalizes one explicit mixed-workload ratio string."""
    pieces = [piece.strip() for piece in text.split(",") if piece.strip()]
    if not pieces:
        raise CliError("mixed ratios must not be empty")

    ratios: dict[str, float] = {}
    for piece in pieces:
        if "=" not in piece:
            raise CliError("mixed ratios must use key=value entries")
        name, raw_value = [part.strip() for part in piece.split("=", 1)]
        if name not in {"put", "delete", "get", "scan"}:
            raise CliError(f"unknown mixed-ratio operation `{name}`")
        try:
            value = float(raw_value)
        except ValueError as error:
            raise CliError(f"mixed ratio for `{name}` must be numeric") from error
        if value < 0:
            raise CliError(f"mixed ratio for `{name}` must be non-negative")
        ratios[name] = value

    required = {"put", "delete", "get", "scan"}
    if set(ratios) != required:
        missing = sorted(required - set(ratios))
        extra = sorted(set(ratios) - required)
        details = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if extra:
            details.append(f"unexpected {', '.join(extra)}")
        raise CliError(
            "mixed ratios must specify put, delete, get, and scan exactly once: "
            + "; ".join(details)
        )

    return normalize_ratios(ratios)


def normalize_ratios(ratios: dict[str, float]) -> dict[str, float]:
    """Normalizes a ratio dictionary into cumulative probability weights."""
    total = sum(ratios.values())
    if total <= 0:
        raise CliError("mixed ratios must include at least one positive value")
    return {name: value / total for name, value in ratios.items()}


def progress_latency_map(latencies_ns: list[int]) -> dict[str, float]:
    """Builds the smaller latency summary used by live stdout progress lines."""
    if not latencies_ns:
        return {
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }

    sorted_latencies = sorted(latencies_ns)
    return {
        "p50": percentile_ms(sorted_latencies, 50.0),
        "p95": percentile_ms(sorted_latencies, 95.0),
        "p99": percentile_ms(sorted_latencies, 99.0),
    }


def format_populate_progress(progress: PreloadProgress, elapsed_seconds: float) -> str:
    """Formats one preload progress line for stdout."""
    completed = progress.completed_operations
    total = progress.total_operations
    percent_complete = 100.0 * completed / total if total else 100.0
    average_tps = completed / elapsed_seconds if elapsed_seconds > 0 else 0.0
    return (
        "populate: "
        f"{completed}/{total} keys ({percent_complete:.2f}%) "
        f"elapsed={elapsed_seconds:.1f}s avg_tps={average_tps:.1f}"
    )


def format_measurement_progress(
    *,
    workload_name: str,
    second: int,
    tps: float,
    latency_ms: dict[str, float],
) -> str:
    """Formats one measurement progress line for stdout."""
    return (
        "measure: "
        f"workload={workload_name} "
        f"second={second} "
        f"tps={tps:.1f} "
        "latency_ms "
        f"p50={latency_ms['p50']:.6f} "
        f"p95={latency_ms['p95']:.6f} "
        f"p99={latency_ms['p99']:.6f}"
    )


def render_human_output(results: dict[str, Any]) -> str:
    """Formats the benchmark results for stdout."""
    lines = []
    for workload in results["workloads"]:
        latency = workload["latency_ms"]
        lines.extend(
            [
                f"workload: {workload['workload']}",
                f"  tps: {workload['tps']:.3f}",
                (
                    "  latency_ms:"
                    f" p50={latency['p50']:.6f}"
                    f" p75={latency['p75']:.6f}"
                    f" p90={latency['p90']:.6f}"
                    f" p95={latency['p95']:.6f}"
                    f" p99={latency['p99']:.6f}"
                    f" p99.9={latency['p99.9']:.6f}"
                ),
                "  operation_counts: "
                + ", ".join(
                    f"{key}={value}"
                    for key, value in workload["operation_counts"].items()
                ),
                "  status_breakdown: "
                + ", ".join(
                    f"{key}={value}"
                    for key, value in workload["status_breakdown"].items()
                ),
                "  listen_addr: " + workload["metadata"]["listen_addr"],
                "  server_pid: " + str(workload["metadata"]["server_pid"]),
            ]
        )
    return "\n".join(lines)


def write_json_output(path: pathlib.Path, results: dict[str, Any]) -> None:
    """Writes the optional JSON artifact to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint used by both `python -m benchmarks` and tests."""
    try:
        config = parse_args(argv)
        results = asyncio.run(run_benchmark(config))
    except CliError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except BenchmarkError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    print(render_human_output(results))
    if config.json_output is not None:
        write_json_output(config.json_output, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
