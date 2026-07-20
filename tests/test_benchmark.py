from __future__ import annotations

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
