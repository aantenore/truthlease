from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from truthlease import (
    Artifact,
    ArtifactDependency,
    ArtifactStatus,
    DependencyCompleteness,
    ObservationStatus,
    SQLiteStore,
    TruthLease,
)
from truthlease.errors import ConflictError, InvalidDependencyError


def _observe(runtime: TruthLease, key: str, version: str) -> None:
    runtime.observe(key=key, version=version, value={"version": version})


def test_minimal_transitive_invalidation_preserves_unrelated_work(runtime: TruthLease) -> None:
    _observe(runtime, "policy", "v1")
    _observe(runtime, "source", "s1")
    policy = runtime.get_observation("policy")
    source = runtime.get_observation("source")
    reusable = runtime.publish_artifact(
        artifact_id="tests",
        kind="report",
        payload={"passed": True},
        dependencies=[ArtifactDependency.observation(source)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    plan = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"step": 1},
        dependencies=[
            ArtifactDependency.observation(policy),
            ArtifactDependency.artifact(reusable),
        ],
        completeness=DependencyCompleteness.OBSERVED,
    )
    child = runtime.publish_artifact(
        artifact_id="summary",
        kind="summary",
        payload={"plan": 1},
        dependencies=[ArtifactDependency.artifact(plan)],
        completeness=DependencyCompleteness.OBSERVED,
    )

    result = runtime.observe(
        key="policy",
        version="v2",
        value={"version": "v2"},
        expected_generation=runtime.get_observation("policy").generation,
    )

    assert result.directly_impacted == (plan.ref,)
    assert result.transitively_impacted == (child.ref,)
    assert runtime.get_artifact("plan", 1).status is ArtifactStatus.STALE
    assert runtime.get_artifact("summary", 1).status is ArtifactStatus.STALE
    assert runtime.get_artifact("tests", 1).status is ArtifactStatus.FRESH
    reconciliation = runtime.reconciliation_plan("policy")
    assert {item.artifact for item in reconciliation.items} == {plan.ref, child.ref}
    assert reconciliation.reusable == (reusable.ref,)


def test_duplicate_event_is_noop_and_reused_identity_with_new_payload_fails(
    runtime: TruthLease,
) -> None:
    first = runtime.observe(key="policy", version="v1", value={"enabled": True}, event_id="event-1")
    duplicate = runtime.observe(
        key="policy", version="v1", value={"enabled": True}, event_id="event-1"
    )
    assert first.duplicate is False
    assert duplicate.duplicate is True
    with pytest.raises(ConflictError, match="reused"):
        runtime.observe(key="policy", version="v2", value={"enabled": False}, event_id="event-1")


def test_same_version_with_different_content_becomes_disputed(runtime: TruthLease) -> None:
    runtime.observe(key="policy", version="v1", value={"limit": 1})
    policy = runtime.get_observation("policy")
    plan = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"uses": "v1"},
        dependencies=[ArtifactDependency.observation(policy)],
        completeness=DependencyCompleteness.OBSERVED,
    )
    runtime.observe(
        key="policy",
        version="v1",
        value={"limit": 2},
        expected_generation=runtime.get_observation("policy").generation,
    )
    assert runtime.get_observation("policy").status is ObservationStatus.DISPUTED
    assert (
        runtime.get_artifact(plan.ref.artifact_id, plan.ref.revision).status
        is ArtifactStatus.DISPUTED
    )


def test_advisory_dependency_does_not_invalidate(runtime: TruthLease) -> None:
    _observe(runtime, "required", "r1")
    _observe(runtime, "advisory", "a1")
    artifact = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"ok": True},
        dependencies=[
            ArtifactDependency.observation(runtime.get_observation("required")),
            ArtifactDependency.observation(runtime.get_observation("advisory"), required=False),
        ],
        completeness=DependencyCompleteness.OBSERVED,
    )
    result = runtime.observe(
        key="advisory",
        version="a2",
        value={"version": "a2"},
        expected_generation=runtime.get_observation("advisory").generation,
    )
    assert result.all_impacted == ()
    assert runtime.get_artifact("plan", artifact.ref.revision).status is ArtifactStatus.FRESH


def test_invalid_dependencies_fail_before_publication(runtime: TruthLease) -> None:
    _observe(runtime, "source", "s1")
    observation = runtime.get_observation("source")
    duplicate = ArtifactDependency.observation(observation)
    with pytest.raises(InvalidDependencyError, match="duplicate"):
        runtime.publish_artifact(
            artifact_id="bad",
            kind="plan",
            payload={},
            dependencies=[duplicate, duplicate],
        )
    with pytest.raises(InvalidDependencyError, match="does not exist"):
        runtime.publish_artifact(
            artifact_id="bad",
            kind="plan",
            payload={},
            dependencies=[
                ArtifactDependency(
                    kind=duplicate.kind,
                    dependency_id="missing",
                    expected_version="v1",
                    expected_generation=1,
                    expected_digest="sha256:missing",
                )
            ],
        )


@settings(max_examples=40, deadline=None)
@given(
    st.lists(
        st.tuples(st.booleans(), st.integers(min_value=0, max_value=1_000)),
        min_size=1,
        max_size=12,
    )
)
def test_generated_dags_invalidate_exactly_the_reachable_closure(
    graph_spec: list[tuple[bool, int]],
) -> None:
    with TemporaryDirectory() as directory:
        runtime = TruthLease(SQLiteStore(Path(directory) / "property.db"))
        _observe(runtime, "source-a", "a1")
        _observe(runtime, "source-b", "b1")
        artifacts: list[Artifact] = []
        parents: dict[int, int | None] = {}
        directly_bound_to_a: set[int] = set()
        for index, (uses_a, parent_seed) in enumerate(graph_spec):
            source_key = "source-a" if uses_a else "source-b"
            dependencies = [ArtifactDependency.observation(runtime.get_observation(source_key))]
            parent: int | None = None
            if index:
                parent = parent_seed % index
                dependencies.append(ArtifactDependency.artifact(artifacts[parent]))
            artifact = runtime.publish_artifact(
                artifact_id=f"node-{index}",
                kind="derived",
                payload={"index": index},
                dependencies=dependencies,
                completeness=DependencyCompleteness.OBSERVED,
            )
            artifacts.append(artifact)
            parents[index] = parent
            if uses_a:
                directly_bound_to_a.add(index)

        runtime.observe(
            key="source-a",
            version="a2",
            value={"version": "a2"},
            expected_generation=runtime.get_observation("source-a").generation,
        )
        expected = set(directly_bound_to_a)
        changed = True
        while changed:
            changed = False
            for index, parent in parents.items():
                if parent in expected and index not in expected:
                    expected.add(index)
                    changed = True

        stale = {
            index
            for index, artifact in enumerate(artifacts)
            if runtime.get_artifact(artifact.ref.artifact_id, 1).status is ArtifactStatus.STALE
        }
        assert stale == expected
