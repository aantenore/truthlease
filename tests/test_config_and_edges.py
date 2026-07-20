from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from conftest import ManualClock, SequentialIds

import truthlease
from truthlease import (
    ArtifactDependency,
    ArtifactStatus,
    AssuranceLevel,
    DependencyCompleteness,
    SQLiteStore,
    TruthLease,
)
from truthlease.config import Settings
from truthlease.errors import (
    ArtifactNotFreshError,
    GraphLimitError,
    IncompleteDependencyError,
    InvalidDependencyError,
    NotFoundError,
)


def test_settings_load_environment_and_reject_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRUTHLEASE_DB", "/tmp/custom.db")
    monkeypatch.setenv("TRUTHLEASE_DEFAULT_LEASE_SECONDS", "12")
    monkeypatch.setenv("TRUTHLEASE_MAX_GRAPH_DEPTH", "7")
    monkeypatch.setenv("TRUTHLEASE_MAX_IMPACT_NODES", "99")
    monkeypatch.setenv("TRUTHLEASE_BUSY_TIMEOUT_MS", "123")
    settings = Settings.from_env()
    assert settings == Settings(
        database_path=Path("/tmp/custom.db"),
        default_lease_seconds=12,
        max_graph_depth=7,
        max_impact_nodes=99,
        busy_timeout_ms=123,
    )
    for arguments in (
        {"default_lease_seconds": 0},
        {"max_graph_depth": 0},
        {"max_impact_nodes": 0},
        {"busy_timeout_ms": 0},
    ):
        with pytest.raises(ValueError):
            Settings(**arguments)  # type: ignore[arg-type]


def test_public_model_types_are_exported() -> None:
    assert {
        "DependencySnapshot",
        "ReconcileAction",
        "ReconciliationItem",
    } <= set(truthlease.__all__)


def test_constructor_and_input_guards(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "db")
    with pytest.raises(ValueError, match="boot_epoch"):
        TruthLease(store, boot_epoch="")
    with pytest.raises(ValueError, match="graph limits"):
        TruthLease(store, max_graph_depth=0)
    with pytest.raises(ValueError, match="busy_timeout"):
        SQLiteStore(tmp_path / "db", busy_timeout_ms=0)

    runtime = TruthLease(store)
    with pytest.raises(ValueError, match="non-empty"):
        runtime.observe(key="", version="v1", value={})
    with pytest.raises(ValueError, match="exactly one"):
        runtime.observe(key="key", version="v1")
    with pytest.raises(ValueError, match="exactly one"):
        runtime.observe(key="key", version="v1", value={}, digest="sha256:x")
    with pytest.raises(ValueError, match="exactly one"):
        runtime.publish_artifact(artifact_id="a", kind="plan")


def test_digest_inputs_noop_refresh_and_query_errors(runtime: TruthLease) -> None:
    first = runtime.observe(key="source", version="v1", digest="sha256:source")
    refresh = runtime.observe(
        key="source",
        version="v1",
        digest="sha256:source",
        expected_generation=runtime.get_observation("source").generation,
    )
    assert first.previous_version is None
    assert refresh.all_impacted == ()
    artifact = runtime.publish_artifact(
        artifact_id="opaque",
        kind="result",
        payload_digest="sha256:artifact",
        payload_ref="blob://artifact",
        completeness=DependencyCompleteness.DECLARED,
    )
    assert artifact.payload_ref == "blob://artifact"
    assert runtime.graph()["artifacts"][0]["id"] == "opaque"
    assert runtime.events()
    runtime.observe(key="null-source", version="v1", value=None)
    null_artifact = runtime.publish_artifact(
        artifact_id="null-artifact",
        kind="result",
        payload=None,
        completeness=DependencyCompleteness.OBSERVED,
    )
    assert null_artifact.status is ArtifactStatus.FRESH
    with pytest.raises(NotFoundError):
        runtime.get_artifact("missing")
    with pytest.raises(NotFoundError):
        runtime.get_observation("missing")
    with pytest.raises(NotFoundError):
        runtime.validate_lease("missing")
    with pytest.raises(NotFoundError):
        runtime.gate_effect(lease_id="missing", effect={})


def test_publish_derives_stale_and_rejects_wrong_artifact_digest(runtime: TruthLease) -> None:
    runtime.observe(key="policy", version="v1", value={"version": 1})
    old = runtime.get_observation("policy")
    upstream = runtime.publish_artifact(
        artifact_id="upstream",
        kind="result",
        payload={"ok": True},
        completeness=DependencyCompleteness.OBSERVED,
    )
    runtime.observe(
        key="policy",
        version="v2",
        value={"version": 2},
        expected_generation=runtime.get_observation("policy").generation,
    )
    stale = runtime.publish_artifact(
        artifact_id="stale-plan",
        kind="plan",
        payload={},
        dependencies=[ArtifactDependency.observation(old)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    assert stale.status is ArtifactStatus.STALE
    with pytest.raises(ArtifactNotFreshError):
        runtime.acquire_lease(
            artifact=stale.ref,
            effect={},
            target="target",
            operation="write",
        )
    wrong = ArtifactDependency(
        kind=ArtifactDependency.artifact(upstream).kind,
        dependency_id=upstream.ref.artifact_id,
        expected_revision=upstream.ref.revision,
        expected_generation=None,
        expected_digest="sha256:wrong",
    )
    with pytest.raises(InvalidDependencyError, match="digest"):
        runtime.publish_artifact(
            artifact_id="bad-child",
            kind="result",
            payload={},
            dependencies=[wrong],
        )


def test_declared_dependencies_cannot_satisfy_observed_assurance(runtime: TruthLease) -> None:
    runtime.observe(key="policy", version="v1", value={"enabled": True})
    artifact = runtime.publish_artifact(
        artifact_id="declared-plan",
        kind="plan",
        payload={},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=DependencyCompleteness.DECLARED,
    )
    with pytest.raises(IncompleteDependencyError, match="declared"):
        runtime.acquire_lease(
            artifact=artifact.ref,
            effect={"write": True},
            target="target",
            operation="write",
            required_assurance=AssuranceLevel.OBSERVED,
        )


def test_boot_epoch_and_ttl_fail_closed(db_path: Path, clock: ManualClock) -> None:
    first = TruthLease(SQLiteStore(db_path), clock=clock, ids=SequentialIds(), boot_epoch="boot-a")
    first.observe(key="source", version="v1", value={"ok": True})
    artifact = first.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={},
        dependencies=[ArtifactDependency.observation(first.get_observation("source"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    with pytest.raises(ValueError, match="ttl"):
        first.acquire_lease(
            artifact=artifact.ref,
            effect={},
            target="target",
            operation="write",
            ttl=timedelta(0),
        )
    lease = first.acquire_lease(
        artifact=artifact.ref,
        effect={},
        target="target",
        operation="write",
    )
    second = TruthLease(SQLiteStore(db_path), clock=clock, boot_epoch="boot-b")
    result = second.validate_lease(lease.lease_id)
    assert result.code == "BOOT_EPOCH_CHANGED"


def test_graph_limits_bound_closure_and_invalidation(tmp_path: Path) -> None:
    closure_path = tmp_path / "closure.db"
    builder = TruthLease(SQLiteStore(closure_path), max_graph_depth=10, max_impact_nodes=20)
    builder.observe(key="source", version="v1", value={"ok": True})
    root = builder.publish_artifact(
        artifact_id="root",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.observation(builder.get_observation("source"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    child = builder.publish_artifact(
        artifact_id="child",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.artifact(root)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    grandchild = builder.publish_artifact(
        artifact_id="grandchild",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.artifact(child)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    closure = TruthLease(SQLiteStore(closure_path), max_graph_depth=1, max_impact_nodes=20)
    with pytest.raises(GraphLimitError, match="depth"):
        closure.acquire_lease(
            artifact=grandchild.ref,
            effect={},
            target="target",
            operation="write",
        )
    root_lease = closure.acquire_lease(
        artifact=root.ref,
        effect={"write": True},
        target="target",
        operation="write",
    )
    depth_result = closure.observe(
        key="source",
        version="v2",
        value={"ok": False},
        expected_generation=closure.get_observation("source").generation,
    )
    assert depth_result.fail_closed is True
    assert depth_result.complete is False
    assert closure.get_observation("source").version == "v2"
    assert closure.gate_effect(lease_id=root_lease.lease_id, effect={"write": True}).code == (
        "COHERENCE_FAULT"
    )

    bounded = TruthLease(SQLiteStore(tmp_path / "nodes.db"), max_graph_depth=10, max_impact_nodes=1)
    bounded.observe(key="source", version="v1", value={"ok": True})
    first = bounded.publish_artifact(
        artifact_id="first",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.observation(bounded.get_observation("source"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    bounded.publish_artifact(
        artifact_id="second",
        kind="result",
        payload={},
        dependencies=[ArtifactDependency.artifact(first)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    node_result = bounded.observe(
        key="source",
        version="v2",
        value={"ok": False},
        expected_generation=bounded.get_observation("source").generation,
    )
    assert node_result.fail_closed is True
    assert bounded.store.integrity_check()["coherence_fault"]
