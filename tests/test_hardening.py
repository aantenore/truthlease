from __future__ import annotations

import sqlite3
import time
from datetime import timedelta
from pathlib import Path
from threading import Event, Thread

import pytest
from conftest import ManualClock, SequentialIds

from truthlease import (
    Artifact,
    ArtifactDependency,
    ArtifactStatus,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    SQLiteStore,
    TruthLease,
)
from truthlease.errors import ConflictError, IncompleteDependencyError


def _observed_artifact(runtime: TruthLease, artifact_id: str = "plan") -> Artifact:
    runtime.observe(key="policy", version="v1", value={"enabled": True})
    return runtime.publish_artifact(
        artifact_id=artifact_id,
        kind="plan",
        payload={"step": 1},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=DependencyCompleteness.OBSERVED,
    )


def test_gate_samples_expiry_after_waiting_for_writer_lock(
    db_path: Path, clock: ManualClock
) -> None:
    creator = TruthLease(SQLiteStore(db_path), clock=clock, ids=SequentialIds())
    artifact = _observed_artifact(creator)
    effect = {"write": True}
    lease = creator.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target",
        operation="write",
        ttl=timedelta(seconds=1),
    )
    gate = TruthLease(SQLiteStore(db_path), clock=clock)
    blocker = sqlite3.connect(db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    started = Event()
    decisions = []

    def consume() -> None:
        started.set()
        decisions.append(gate.gate_effect(lease_id=lease.lease_id, effect=effect))

    thread = Thread(target=consume)
    thread.start()
    assert started.wait(timeout=1)
    time.sleep(0.05)
    clock.advance(seconds=2)
    blocker.commit()
    blocker.close()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert decisions[0].code == "LEASE_EXPIRED"


def test_revalidation_evidence_is_fresh_trusted_and_lease_bound(
    db_path: Path, clock: ManualClock
) -> None:
    runtime = TruthLease(
        SQLiteStore(db_path),
        clock=clock,
        ids=SequentialIds(),
        trusted_evidence_authorities={"adapter:policy"},
    )
    artifact = _observed_artifact(runtime)
    effect = {"write": True}
    first = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target",
        operation="write",
        required_assurance=AssuranceLevel.REVALIDATED,
    )
    second = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target",
        operation="write",
        required_assurance=AssuranceLevel.REVALIDATED,
    )
    evidence = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="adapter:policy",
        evidence_digest="sha256:evidence",
        observed_at=clock.now(),
        lease_id=first.lease_id,
        closure_digest=first.closure_digest,
        effect_digest=first.effect_digest,
        target=first.target,
        operation=first.operation,
    )
    replay = runtime.gate_effect(
        lease_id=second.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.REVALIDATED,
        evidence=evidence,
    )
    assert replay.code == "ASSURANCE_EVIDENCE_INVALID"

    clock.advance(seconds=6)
    stale = runtime.gate_effect(
        lease_id=first.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.REVALIDATED,
        evidence=evidence,
    )
    assert stale.code == "ASSURANCE_EVIDENCE_INVALID"

    fresh = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="adapter:policy",
        evidence_digest="sha256:fresh",
        observed_at=clock.now(),
        lease_id=first.lease_id,
        closure_digest=first.closure_digest,
        effect_digest=first.effect_digest,
        target=first.target,
        operation=first.operation,
    )
    assert runtime.gate_effect(
        lease_id=first.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.REVALIDATED,
        evidence=fresh,
    ).allowed


def test_revalidation_cannot_promote_declared_dependency_completeness(
    db_path: Path, clock: ManualClock
) -> None:
    runtime = TruthLease(
        SQLiteStore(db_path),
        clock=clock,
        ids=SequentialIds(),
        trusted_evidence_authorities={"adapter:policy"},
    )
    artifact = runtime.publish_artifact(
        artifact_id="declared",
        kind="plan",
        payload={},
        completeness=DependencyCompleteness.DECLARED,
    )
    effect = {"write": True}
    lease = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target",
        operation="write",
        required_assurance=AssuranceLevel.DECLARED,
    )
    evidence = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="adapter:policy",
        evidence_digest="sha256:evidence",
        observed_at=clock.now(),
        lease_id=lease.lease_id,
        closure_digest=lease.closure_digest,
        effect_digest=lease.effect_digest,
        target=lease.target,
        operation=lease.operation,
    )

    decision = runtime.gate_effect(
        lease_id=lease.lease_id,
        effect=effect,
        achieved_assurance=AssuranceLevel.REVALIDATED,
        evidence=evidence,
    )

    assert decision.allowed is True
    assert decision.assurance is AssuranceLevel.DECLARED


def test_transitive_declared_dependency_cannot_be_laundered(runtime: TruthLease) -> None:
    base = runtime.publish_artifact(
        artifact_id="declared-base",
        kind="result",
        payload={"value": 1},
        completeness=DependencyCompleteness.DECLARED,
    )
    top = runtime.publish_artifact(
        artifact_id="claimed-observed-top",
        kind="plan",
        payload={"uses": "base"},
        dependencies=[ArtifactDependency.artifact(base)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    assert top.completeness is DependencyCompleteness.DECLARED
    with pytest.raises(IncompleteDependencyError, match="declared"):
        runtime.acquire_lease(
            artifact=top.ref,
            effect={"write": True},
            target="target",
            operation="write",
            required_assurance=AssuranceLevel.OBSERVED,
        )


def test_status_join_is_order_independent_and_ttl_is_visible(
    runtime: TruthLease, clock: ManualClock
) -> None:
    runtime.observe(key="disputed", version="v1", value={"value": 1})
    runtime.observe(
        key="disputed",
        version="v1",
        value={"value": 2},
        expected_generation=runtime.get_observation("disputed").generation,
    )
    runtime.observe(key="stale", version="v1", value={"value": 1})
    old_stale = runtime.get_observation("stale")
    runtime.observe(
        key="stale",
        version="v2",
        value={"value": 2},
        expected_generation=old_stale.generation,
    )
    disputed = ArtifactDependency.observation(runtime.get_observation("disputed"))
    stale = ArtifactDependency.observation(old_stale)
    for artifact_id, dependencies in (
        ("left", [disputed, stale]),
        ("right", [stale, disputed]),
    ):
        artifact = runtime.publish_artifact(
            artifact_id=artifact_id,
            kind="plan",
            payload={},
            dependencies=dependencies,
            completeness=DependencyCompleteness.OBSERVED,
        )
        assert artifact.status is ArtifactStatus.DISPUTED

    runtime.observe(
        key="short",
        version="v1",
        value={"value": 1},
        valid_for=timedelta(seconds=1),
    )
    expiring = runtime.publish_artifact(
        artifact_id="expiring",
        kind="plan",
        payload={},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("short"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    clock.advance(seconds=2)
    assert runtime.get_artifact("expiring").status is ArtifactStatus.STALE
    graph_item = next(item for item in runtime.graph()["artifacts"] if item["id"] == "expiring")
    assert graph_item["status"] == "stale"
    assert {item.artifact for item in runtime.reconciliation_plan("short").items} == {expiring.ref}


def test_observation_generation_prevents_out_of_order_overwrite(runtime: TruthLease) -> None:
    runtime.observe(key="source", version="v1", value={"value": 1})
    first_generation = runtime.get_observation("source").generation
    artifact = runtime.publish_artifact(
        artifact_id="generation-bound",
        kind="plan",
        payload={},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("source"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    noop = runtime.observe(
        key="source",
        version="v1",
        value={"value": 1},
        expected_generation=first_generation,
    )
    assert noop.all_impacted == ()
    assert runtime.get_observation("source").generation == first_generation
    runtime.observe(
        key="source",
        version="v2",
        value={"value": 2},
        expected_generation=first_generation,
    )
    with pytest.raises(ConflictError, match="generation changed"):
        runtime.observe(
            key="source",
            version="delayed-v1",
            value={"value": 1},
            expected_generation=first_generation,
        )
    assert runtime.get_observation("source").version == "v2"
    runtime.observe(
        key="source",
        version="v1",
        value={"value": 1},
        expected_generation=runtime.get_observation("source").generation,
    )
    assert (
        runtime.get_artifact(artifact.ref.artifact_id, artifact.ref.revision).status
        is ArtifactStatus.STALE
    )


def test_incompatible_sqlite_database_is_not_modified(tmp_path: Path) -> None:
    path = tmp_path / "foreign.db"
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
    connection.execute("PRAGMA user_version = 99")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="unsupported"):
        SQLiteStore(path).initialize()

    connection = sqlite3.connect(path)
    tables = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    connection.close()
    assert tables == []
    assert version == 99
    assert journal_mode == "wal"


def test_schema_shape_validation_rejects_spoofed_versioned_database(tmp_path: Path) -> None:
    path = tmp_path / "wrong-shape.db"
    store = SQLiteStore(path)
    store.initialize()
    connection = sqlite3.connect(path)
    connection.execute("DROP TABLE runtime_state")
    connection.execute(
        "CREATE TABLE runtime_state(singleton INTEGER PRIMARY KEY, coherence_fault TEXT)"
    )
    connection.execute("INSERT INTO runtime_state VALUES (1, NULL)")
    connection.commit()
    connection.close()

    assert store.integrity_check()["schema_shape_ok"] is False
    assert store.integrity_check()["ok"] is False
    with pytest.raises(RuntimeError, match="schema shape"):
        store.initialize()


def test_direct_fanout_overflow_also_fails_closed(tmp_path: Path) -> None:
    runtime = TruthLease(SQLiteStore(tmp_path / "fanout.db"), max_impact_nodes=1)
    runtime.observe(key="source", version="v1", value={"value": 1})
    dependency = ArtifactDependency.observation(runtime.get_observation("source"))
    for artifact_id in ("one", "two"):
        runtime.publish_artifact(
            artifact_id=artifact_id,
            kind="result",
            payload={},
            dependencies=[dependency],
            completeness=DependencyCompleteness.OBSERVED,
        )

    result = runtime.observe(
        key="source",
        version="v2",
        value={"value": 2},
        expected_generation=runtime.get_observation("source").generation,
    )

    assert result.fail_closed is True
    assert result.complete is False
    assert result.invalidated_leases == ()
    with pytest.raises(ValueError, match="unavailable"):
        _ = result.all_impacted
    assert runtime.graph()["artifacts"][0]["status"] == "unknown"


def test_status_recomputation_overflow_cannot_roll_back_new_observation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "upstream-depth.db"
    builder = TruthLease(SQLiteStore(path), max_graph_depth=10)
    builder.observe(key="trigger", version="v1", value={"value": 1})
    builder.observe(key="upstream", version="v1", value={"value": 1})
    base = builder.publish_artifact(
        artifact_id="base",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.observation(builder.get_observation("upstream"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    middle = builder.publish_artifact(
        artifact_id="middle",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.artifact(base)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    top = builder.publish_artifact(
        artifact_id="top",
        kind="plan",
        payload={},
        dependencies=[
            ArtifactDependency.observation(builder.get_observation("trigger")),
            ArtifactDependency.artifact(middle),
        ],
        completeness=DependencyCompleteness.OBSERVED,
    )
    effect = {"write": True}
    lease = builder.acquire_lease(
        artifact=top.ref,
        effect=effect,
        target="target",
        operation="write",
    )

    limited = TruthLease(SQLiteStore(path), max_graph_depth=1)
    result = limited.observe(
        key="trigger",
        version="v2",
        value={"value": 2},
        expected_generation=limited.get_observation("trigger").generation,
    )

    assert result.fail_closed is True
    assert limited.get_observation("trigger").version == "v2"
    assert limited.store.integrity_check()["coherence_fault"]
    assert limited.gate_effect(lease_id=lease.lease_id, effect=effect).code == "COHERENCE_FAULT"


def test_overlapping_direct_and_transitive_edges_do_not_trigger_false_overflow(
    tmp_path: Path,
) -> None:
    runtime = TruthLease(SQLiteStore(tmp_path / "overlap.db"), max_impact_nodes=2)
    runtime.observe(key="source", version="v1", value={"value": 1})
    source = ArtifactDependency.observation(runtime.get_observation("source"))
    first = runtime.publish_artifact(
        artifact_id="first",
        kind="result",
        payload={},
        dependencies=[source],
        completeness=DependencyCompleteness.OBSERVED,
    )
    second = runtime.publish_artifact(
        artifact_id="second",
        kind="result",
        payload={},
        dependencies=[source, ArtifactDependency.artifact(first)],
        completeness=DependencyCompleteness.OBSERVED,
    )

    result = runtime.observe(
        key="source",
        version="v2",
        value={"value": 2},
        expected_generation=runtime.get_observation("source").generation,
    )

    assert result.complete is True
    assert result.fail_closed is False
    assert set(result.all_impacted) == {first.ref, second.ref}
    assert runtime.store.integrity_check()["coherence_fault"] is None


def test_historical_stale_revisions_do_not_count_as_new_direct_fanout(
    tmp_path: Path,
) -> None:
    runtime = TruthLease(SQLiteStore(tmp_path / "history.db"), max_impact_nodes=2)
    runtime.observe(key="source", version="v1", value={"value": 1})
    artifacts = []
    for generation in range(1, 4):
        artifacts.append(
            runtime.publish_artifact(
                artifact_id=f"artifact-{generation}",
                kind="result",
                payload={"generation": generation},
                dependencies=[ArtifactDependency.observation(runtime.get_observation("source"))],
                completeness=DependencyCompleteness.OBSERVED,
            )
        )
        runtime.observe(
            key="source",
            version=f"v{generation + 1}",
            value={"value": generation + 1},
            expected_generation=runtime.get_observation("source").generation,
        )

    assert runtime.store.integrity_check()["coherence_fault"] is None
    assert {item["status"] for item in runtime.graph()["artifacts"]} == {"stale"}
    assert all(
        runtime.get_artifact(item.ref.artifact_id, item.ref.revision).status is ArtifactStatus.STALE
        for item in artifacts
    )
