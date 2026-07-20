from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from threading import Barrier, Thread

import pytest
from conftest import ManualClock, SequentialIds

from truthlease import (
    Artifact,
    ArtifactDependency,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    SQLiteStore,
    TruthLease,
)
from truthlease.errors import ArtifactNotFreshError, IncompleteDependencyError


def _artifact(runtime: TruthLease, *, completeness: DependencyCompleteness) -> Artifact:
    runtime.observe(key="policy", version="v1", value={"enabled": True})
    return runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"policy": "v1"},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=completeness,
    )


def test_stale_lease_and_effect_substitution_are_blocked(runtime: TruthLease) -> None:
    artifact = _artifact(runtime, completeness=DependencyCompleteness.OBSERVED)
    effect = {"action": "promote", "release": "one"}
    lease = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
    )
    mismatch = runtime.gate_effect(
        lease_id=lease.lease_id,
        effect={"action": "promote", "release": "two"},
    )
    assert mismatch.allowed is False
    assert mismatch.code == "EFFECT_MISMATCH"
    assert mismatch.guarantee.startswith("No effect")
    assert runtime.validate_lease(lease.lease_id).allowed is True

    runtime.observe(
        key="policy",
        version="v2",
        value={"enabled": False},
        expected_generation=runtime.get_observation("policy").generation,
    )
    stale = runtime.gate_effect(lease_id=lease.lease_id, effect=effect)
    assert stale.allowed is False
    assert stale.code == "STALE_LEASE"
    assert stale.changed_dependencies == ("policy",)
    assert stale.guarantee.startswith("No effect")


def test_lease_is_single_use_and_expires(runtime: TruthLease, clock: ManualClock) -> None:
    artifact = _artifact(runtime, completeness=DependencyCompleteness.OBSERVED)
    effect = {"action": "promote"}
    lease = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
    )
    first = runtime.gate_effect(lease_id=lease.lease_id, effect=effect)
    second = runtime.gate_effect(lease_id=lease.lease_id, effect=effect)
    assert first.allowed and first.receipt_id
    assert second.allowed is False and second.code == "LEASE_CONSUMED"

    next_artifact = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"policy": "v1", "retry": 2},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    expiring = runtime.acquire_lease(
        artifact=next_artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
        ttl=timedelta(seconds=1),
    )
    clock.advance(seconds=2)
    expired = runtime.validate_lease(expiring.lease_id)
    assert expired.allowed is False and expired.code == "LEASE_EXPIRED"


def test_completeness_and_assurance_are_explicit(runtime: TruthLease, clock: ManualClock) -> None:
    runtime.trusted_evidence_authorities = frozenset({"adapter:authoritative-read"})
    unknown = _artifact(runtime, completeness=DependencyCompleteness.UNKNOWN)
    with pytest.raises(IncompleteDependencyError):
        runtime.acquire_lease(
            artifact=unknown.ref,
            effect={"action": "promote"},
            target="deployment/current",
            operation="promote",
        )

    observed = runtime.publish_artifact(
        artifact_id="observed-plan",
        kind="plan",
        payload={"policy": "v1"},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    effect = {"action": "promote"}
    lease = runtime.acquire_lease(
        artifact=observed.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
        required_assurance=AssuranceLevel.REVALIDATED,
    )
    insufficient = runtime.gate_effect(lease_id=lease.lease_id, effect=effect)
    assert insufficient.code == "ASSURANCE_INSUFFICIENT"
    unsupported = runtime.gate_effect(
        lease_id=lease.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.FENCED,
    )
    assert unsupported.code == "ASSURANCE_UNSUPPORTED"
    missing = runtime.gate_effect(
        lease_id=lease.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.REVALIDATED,
    )
    assert missing.code == "ASSURANCE_EVIDENCE_REQUIRED"
    evidence = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="adapter:authoritative-read",
        evidence_digest="sha256:evidence",
        observed_at=clock.now(),
        lease_id=lease.lease_id,
        closure_digest=lease.closure_digest,
        effect_digest=lease.effect_digest,
        target=lease.target,
        operation=lease.operation,
    )
    allowed = runtime.gate_effect(
        lease_id=lease.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.REVALIDATED,
        evidence=evidence,
    )
    assert allowed.allowed is True
    assert "authoritative revalidation" in allowed.guarantee


def test_expired_observation_prevents_lease(runtime: TruthLease, clock: ManualClock) -> None:
    runtime.observe(
        key="short-lived",
        version="v1",
        value={"ok": True},
        valid_for=timedelta(seconds=1),
    )
    artifact = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("short-lived"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    clock.advance(seconds=2)
    with pytest.raises(ArtifactNotFreshError, match="expired"):
        runtime.acquire_lease(
            artifact=artifact.ref,
            effect={},
            target="target",
            operation="write",
        )


def test_gate_has_one_winner_under_concurrency(db_path: Path, clock: ManualClock) -> None:
    creator = TruthLease(SQLiteStore(db_path), clock=clock, ids=SequentialIds())
    artifact = _artifact(creator, completeness=DependencyCompleteness.OBSERVED)
    effect = {"action": "promote"}
    lease = creator.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
    )
    barrier = Barrier(2)
    results: list[bool] = []

    def consume() -> None:
        runtime = TruthLease(SQLiteStore(db_path), clock=clock)
        barrier.wait()
        results.append(runtime.gate_effect(lease_id=lease.lease_id, effect=effect).allowed)

    threads = [Thread(target=consume), Thread(target=consume)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == [False, True]
