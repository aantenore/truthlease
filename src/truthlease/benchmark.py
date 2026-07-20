"""Reproducible invalidation benchmark over the real SQLite adapter."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Any

from truthlease.engine import TruthLease
from truthlease.models import Artifact, ArtifactDependency, DependencyCompleteness
from truthlease.storage.sqlite import SQLiteStore


def run_benchmark(*, nodes: int, partitions: int) -> dict[str, Any]:
    if nodes <= 0:
        raise ValueError("nodes must be positive")
    if partitions <= 0 or partitions > nodes:
        raise ValueError("partitions must be between one and nodes")

    with tempfile.TemporaryDirectory(prefix="truthlease-benchmark-") as directory:
        runtime = TruthLease(
            SQLiteStore(Path(directory) / "benchmark.db"),
            max_impact_nodes=nodes + partitions + 1,
            max_graph_depth=(nodes // partitions) + 2,
        )
        for partition in range(partitions):
            runtime.observe(
                key=f"source-{partition}",
                version="v1",
                value={"partition": partition, "revision": 1},
            )

        previous_by_partition: dict[int, Artifact] = {}
        for index in range(nodes):
            partition = index % partitions
            previous = previous_by_partition.get(partition)
            dependencies = (
                [ArtifactDependency.artifact(previous)]
                if previous is not None
                else [
                    ArtifactDependency.observation(runtime.get_observation(f"source-{partition}"))
                ]
            )
            previous_by_partition[partition] = runtime.publish_artifact(
                artifact_id=f"artifact-{index}",
                kind="synthetic-work",
                payload={"index": index, "partition": partition},
                dependencies=dependencies,
                completeness=DependencyCompleteness.OBSERVED,
            )

        started = time.perf_counter_ns()
        result = runtime.observe(
            key="source-0",
            version="v2",
            value={"partition": 0, "revision": 2},
            expected_generation=runtime.get_observation("source-0").generation,
        )
        elapsed_ns = time.perf_counter_ns() - started
        impacted = len(result.all_impacted)
        reused = nodes - impacted
        expected_impacted = len(range(0, nodes, partitions))
        expected_refs = {(f"artifact-{index}", 1) for index in range(0, nodes, partitions)}
        actual_refs = {(ref.artifact_id, ref.revision) for ref in result.all_impacted}
        return {
            "schema_version": 1,
            "nodes": nodes,
            "partitions": partitions,
            "changed_sources": 1,
            "impacted_nodes": impacted,
            "reused_nodes": reused,
            "work_reuse_ratio": reused / nodes,
            "invalidation_ms": elapsed_ns / 1_000_000,
            "correctness": {
                "expected_impacted_nodes": expected_impacted,
                "exact_minimal_invalidation": actual_refs == expected_refs,
            },
            "note": (
                "Latency is environment-specific. The stable claim is exact impacted-node "
                "count versus a full rerun of every node."
            ),
        }
