"""Deterministic, domain-neutral demonstration of partial invalidation."""

from __future__ import annotations

from pathlib import Path

from truthlease.canonical import Digestable
from truthlease.engine import TruthLease
from truthlease.models import (
    ArtifactDependency,
    ArtifactRef,
    AssuranceLevel,
    DependencyCompleteness,
)
from truthlease.storage.sqlite import SQLiteStore


def run_demo(path: Path, *, reset: bool = False) -> dict[str, object]:
    if reset:
        _remove_database_files(path)
    if path.exists():
        raise FileExistsError(f"demo database already exists: {path}; pass --reset to replace it")

    runtime = TruthLease(SQLiteStore(path))
    runtime.observe(
        key="source-code",
        version="sha1",
        value={"tree": "sha1"},
        event_id="demo-source-sha1",
    )
    runtime.observe(
        key="release-policy",
        version="v12",
        value={"reviewers": 2},
        event_id="demo-policy-v12",
    )
    runtime.observe(
        key="tool-schema",
        version="sha4",
        value={"promote": {"required": ["release"]}},
        event_id="demo-schema-sha4",
    )

    test_report = runtime.publish_artifact(
        artifact_id="test-report",
        kind="verification",
        payload={"source": "sha1", "passed": True},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("source-code"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    release_plan_v1 = runtime.publish_artifact(
        artifact_id="release-plan",
        kind="plan",
        payload={"policy": "v12", "reuse": "test-report@1"},
        dependencies=[
            ArtifactDependency.observation(runtime.get_observation("release-policy")),
            ArtifactDependency.observation(runtime.get_observation("tool-schema")),
            ArtifactDependency.artifact(test_report),
        ],
        completeness=DependencyCompleteness.OBSERVED,
    )
    effect_v1: Digestable = {"release": "2026.07", "plan_revision": 1}
    lease_v1 = runtime.acquire_lease(
        artifact=release_plan_v1.ref,
        effect=effect_v1,
        target="deployment/current",
        operation="promote",
        required_assurance=AssuranceLevel.OBSERVED,
    )

    invalidation = runtime.observe(
        key="release-policy",
        version="v13",
        value={"reviewers": 2, "artifact_signature": True},
        event_id="demo-policy-v13",
        expected_generation=runtime.get_observation("release-policy").generation,
    )
    duplicate = runtime.observe(
        key="release-policy",
        version="v13",
        value={"reviewers": 2, "artifact_signature": True},
        event_id="demo-policy-v13",
    )
    old_gate = runtime.gate_effect(lease_id=lease_v1.lease_id, effect=effect_v1)
    plan = runtime.reconciliation_plan("release-policy")

    release_plan_v2 = runtime.publish_artifact(
        artifact_id="release-plan",
        kind="plan",
        payload={"policy": "v13", "reuse": "test-report@1", "sign": True},
        dependencies=[
            ArtifactDependency.observation(runtime.get_observation("release-policy")),
            ArtifactDependency.observation(runtime.get_observation("tool-schema")),
            ArtifactDependency.artifact(test_report),
        ],
        completeness=DependencyCompleteness.OBSERVED,
    )
    effect_v2: Digestable = {"release": "2026.07", "plan_revision": 2}
    lease_v2 = runtime.acquire_lease(
        artifact=release_plan_v2.ref,
        effect=effect_v2,
        target="deployment/current",
        operation="promote",
        required_assurance=AssuranceLevel.OBSERVED,
    )
    new_gate = runtime.gate_effect(lease_id=lease_v2.lease_id, effect=effect_v2)

    return {
        "summary": (
            "Policy v13 invalidated only the old release plan. The test report remained "
            "fresh, the old lease was blocked, and the rebuilt plan consumed a new lease."
        ),
        "old_plan": {
            "ref": _ref(release_plan_v1.ref),
            "status": runtime.get_artifact("release-plan", 1).status.value,
            "gate": old_gate.code,
        },
        "reusable": [_ref(item) for item in plan.reusable],
        "new_plan": {
            "ref": _ref(release_plan_v2.ref),
            "status": runtime.get_artifact("release-plan", 2).status.value,
            "gate": new_gate.code,
            "receipt_id": new_gate.receipt_id,
        },
        "invalidation": {
            "direct": [_ref(item) for item in invalidation.directly_impacted],
            "transitive": [_ref(item) for item in invalidation.transitively_impacted],
            "invalidated_leases": list(invalidation.invalidated_leases),
            "duplicate_event_was_noop": duplicate.duplicate,
        },
        "integrity": runtime.store.integrity_check(),
    }


def _ref(ref: ArtifactRef) -> str:
    return f"{ref.artifact_id}@{ref.revision}"


def _remove_database_files(path: Path) -> None:
    for suffix in ("", "-journal", "-shm", "-wal"):
        Path(f"{path}{suffix}").unlink(missing_ok=True)
