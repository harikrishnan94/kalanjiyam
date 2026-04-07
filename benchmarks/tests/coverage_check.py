"""Runs the benchmark tests under stdlib trace and reports measured coverage."""

from __future__ import annotations

import pathlib
import sys
import trace
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
TARGETS = [
    REPO_ROOT / "benchmarks" / "__init__.py",
    REPO_ROOT / "benchmarks" / "__main__.py",
    REPO_ROOT / "benchmarks" / "tcp_bench.py",
    REPO_ROOT / "benchmarks" / "wrapper.py",
]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def main() -> int:
    """Runs the benchmark unittest suite and reports coverage for benchmark package files."""
    loader = unittest.defaultTestLoader
    suite = loader.discover(str(REPO_ROOT / "benchmarks" / "tests"), pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    tracer = trace.Trace(count=True, trace=False, ignoredirs=[sys.prefix, sys.base_prefix])
    result = tracer.runfunc(runner.run, suite)
    if not result.wasSuccessful():
        return 1

    counts = tracer.results().counts
    total_covered = 0
    total_executable = 0
    for target in TARGETS:
        executable = trace._find_executable_linenos(str(target))
        covered = {
            lineno
            for (filename, lineno), count in counts.items()
            if pathlib.Path(filename).resolve() == target.resolve()
            and count > 0
            and lineno in executable
        }
        total_covered += len(covered)
        total_executable += len(executable)
        percent = 100.0 * len(covered) / len(executable)
        print(
            f"{target.relative_to(REPO_ROOT)}: "
            f"{len(covered)}/{len(executable)} lines covered ({percent:.2f}%)"
        )

    overall_percent = 100.0 * total_covered / total_executable
    print(
        f"overall benchmark package coverage: "
        f"{total_covered}/{total_executable} lines ({overall_percent:.2f}%)"
    )
    if total_covered != total_executable:
        print(
            "note: stdlib trace does not follow the subprocess-based CLI smoke runs used by this "
            "benchmark workflow, so this report is best-effort in-process coverage rather than a "
            "hard pass/fail gate."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
