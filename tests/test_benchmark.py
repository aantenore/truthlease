from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from truthlease.benchmark import run_benchmark


def test_benchmark_reports_exact_reuse() -> None:
    report = run_benchmark(nodes=24, partitions=4)
    assert report["correctness"]["exact_minimal_invalidation"] is True
    assert report["impacted_nodes"] == 6
    assert report["reused_nodes"] == 18
    assert report["work_reuse_ratio"] == 0.75


@pytest.mark.parametrize(
    ("nodes", "partitions"),
    [(0, 1), (1, 0), (2, 3)],
)
def test_benchmark_rejects_invalid_shapes(nodes: int, partitions: int) -> None:
    with pytest.raises(ValueError):
        run_benchmark(nodes=nodes, partitions=partitions)


def test_fence_benchmark_proves_all_protocol_invariants() -> None:
    completed = subprocess.run(
        [sys.executable, "benchmarks/benchmark_fence_correctness.py"],
        cwd=Path(__file__).parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    report = json.loads(completed.stdout)
    assert report["passed"] is True
    assert report["target_effect_count"] == 3
    assert report["unique_accepted_attempts"] == 3
    assert all(report["correctness"].values())
