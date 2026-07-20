from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from pathlib import Path
from threading import Barrier, Lock, Thread

import pytest
from conftest import ManualClock, SequentialIds

from truthlease.canonical import Digestable, digest_json
from truthlease.engine import TruthLease, digest_effect
from truthlease.errors import ConflictError, TargetFenceError
from truthlease.models import (
    Artifact,
    ArtifactDependency,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    FenceCoverage,
    FencedAttemptStatus,
    FencedCommitRequest,
    FencedOutcome,
    FencedResult,
    Lease,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import SQLiteStore


def _runtime(
    db_path: Path,
    clock: ManualClock,
    *,
    observations: Sequence[str] = ("policy",),
) -> tuple[TruthLease, Artifact]:
    runtime = TruthLease(
        SQLiteStore(db_path),
        clock=clock,
        ids=SequentialIds(),
        trusted_evidence_authorities={"evidence:test"},
        trusted_target_adapters={"adapter:test"},
    )
    dependencies: list[ArtifactDependency] = []
    for key in observations:
        runtime.observe(key=key, version="v1", value={"key": key})
        dependencies.append(ArtifactDependency.observation(runtime.get_observation(key)))
    artifact = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"step": 1},
        dependencies=dependencies,
        completeness=DependencyCompleteness.OBSERVED,
    )
    return runtime, artifact


def _precondition(
    protected_observations: Sequence[str] = ("policy",),
    *,
    expected_token: str = "target-token-v1",
    replay_assurance: ReplayAssurance = ReplayAssurance.DEDUPLICATED,
    adapter_id: str = "adapter:test",
) -> TargetPrecondition:
    return TargetPrecondition(
        adapter_id=adapter_id,
        mechanism="test-compare-and-swap",
        expected_token=expected_token,
        protected_observations=tuple(protected_observations),
        replay_assurance=replay_assurance,
    )


def _evidence(lease: Lease, clock: ManualClock) -> AssuranceEvidence:
    return AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="evidence:test",
        evidence_digest="sha256:fresh-evidence",
        observed_at=clock.now(),
        lease_id=lease.lease_id,
        closure_digest=lease.closure_digest,
        effect_digest=lease.effect_digest,
        target=lease.target,
        operation=lease.operation,
    )


def _result(
    request: FencedCommitRequest,
    outcome: FencedOutcome,
    *,
    marker: str,
    clock: ManualClock,
) -> FencedResult:
    committed = outcome is FencedOutcome.COMMITTED
    return FencedResult(
        attempt_id=request.attempt_id,
        lease_id=request.lease_id,
        adapter_id=request.adapter_id,
        outcome=outcome,
        request_digest=request.request_digest,
        closure_digest=request.closure_digest,
        effect_digest=request.effect_digest,
        target=request.target,
        operation=request.operation,
        precondition_digest=request.precondition_digest,
        scope_digest=request.scope_digest,
        replay_assurance=request.precondition.replay_assurance,
        coverage=request.coverage,
        target_result_digest=digest_json(
            {"outcome": outcome.value, "marker": marker}, domain="test:target-result"
        ),
        target_receipt_ref=f"target-receipt:{marker}" if committed else None,
        committed_at=clock.now() if committed else None,
    )


def _acquire(
    runtime: TruthLease,
    artifact: Artifact,
    *,
    effect: Digestable,
    precondition: TargetPrecondition,
    required_assurance: AssuranceLevel = AssuranceLevel.FENCED,
) -> Lease:
    return runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target/current",
        operation="write",
        required_assurance=required_assurance,
        target_precondition=precondition,
    )


def _table_count(db_path: Path, table: str) -> int:
    connection = sqlite3.connect(db_path)
    try:
        return int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
    finally:
        connection.close()


def test_full_fenced_commit_derives_receipt_without_persisting_raw_inputs(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect: dict[str, Digestable] = {"write": "raw-effect-secret-8392"}
    precondition = _precondition(expected_token="raw-target-token-secret-1731")
    lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)

    request = runtime.prepare_fenced_commit(
        lease.lease_id,
        effect,
        precondition,
        _evidence(lease, clock),
    )
    receipt = runtime.record_fenced_result(
        _result(request, FencedOutcome.COMMITTED, marker="one", clock=clock)
    )

    assert receipt is not None
    assert receipt.assurance is AssuranceLevel.FENCED
    assert receipt.coverage is FenceCoverage.FULL
    assert receipt.replay_assurance is ReplayAssurance.DEDUPLICATED
    assert runtime.get_fenced_receipt(request.attempt_id) == receipt
    attempt = runtime.get_fenced_attempt(request.attempt_id)
    assert attempt is not None and attempt.status is FencedAttemptStatus.COMMITTED
    database_bytes = db_path.read_bytes()
    assert b"raw-effect-secret-8392" not in database_bytes
    assert b"raw-target-token-secret-1731" not in database_bytes


def test_partial_commit_is_audited_but_never_claims_fenced_assurance(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock, observations=("policy", "quota"))
    effect = {"write": True}
    partial = _precondition(("policy",))
    with pytest.raises(TargetFenceError, match="full observation-closure"):
        _acquire(runtime, artifact, effect=effect, precondition=partial)

    lease = _acquire(
        runtime,
        artifact,
        effect=effect,
        precondition=partial,
        required_assurance=AssuranceLevel.OBSERVED,
    )
    request = runtime.prepare_fenced_commit(
        lease.lease_id, effect, partial, _evidence(lease, clock)
    )
    receipt = runtime.record_fenced_result(
        _result(request, FencedOutcome.COMMITTED, marker="partial", clock=clock)
    )

    assert request.coverage is FenceCoverage.PARTIAL
    assert receipt is None
    attempt = runtime.get_fenced_attempt(request.attempt_id)
    assert attempt is not None and attempt.status is FencedAttemptStatus.COMMITTED
    assert _table_count(db_path, "fenced_results") == 1
    assert _table_count(db_path, "fenced_receipts") == 0


@pytest.mark.parametrize(
    "precondition, trusted",
    [
        (_precondition(replay_assurance=ReplayAssurance.ONE_SHOT), True),
        (_precondition(replay_assurance=ReplayAssurance.UNKNOWN), True),
        (_precondition(adapter_id="adapter:untrusted"), False),
    ],
)
def test_fenced_acquire_rejects_weak_replay_or_untrusted_adapter(
    db_path: Path,
    clock: ManualClock,
    precondition: TargetPrecondition,
    trusted: bool,
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    assert trusted is (precondition.adapter_id == "adapter:test")
    with pytest.raises(TargetFenceError):
        _acquire(runtime, artifact, effect={"write": True}, precondition=precondition)


def test_prepare_rejects_stale_lease_and_precondition_mismatch(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect = {"write": True}
    precondition = _precondition()
    mismatch_lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    with pytest.raises(TargetFenceError, match="does not match"):
        runtime.prepare_fenced_commit(
            mismatch_lease.lease_id,
            effect,
            _precondition(expected_token="different-token"),
            _evidence(mismatch_lease, clock),
        )

    stale_lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    observation = runtime.get_observation("policy")
    runtime.observe(
        key="policy",
        version="v2",
        value={"key": "policy", "changed": True},
        expected_generation=observation.generation,
    )
    with pytest.raises(TargetFenceError, match="cannot prepare"):
        runtime.prepare_fenced_commit(
            stale_lease.lease_id,
            effect,
            precondition,
            _evidence(stale_lease, clock),
        )


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (FencedOutcome.NOT_COMMITTED, FencedAttemptStatus.NOT_COMMITTED),
        (FencedOutcome.INDETERMINATE, FencedAttemptStatus.INDETERMINATE),
    ],
)
def test_non_committed_outcomes_never_create_a_receipt(
    db_path: Path,
    clock: ManualClock,
    outcome: FencedOutcome,
    status: FencedAttemptStatus,
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect = {"write": True}
    precondition = _precondition()
    lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    request = runtime.prepare_fenced_commit(
        lease.lease_id, effect, precondition, _evidence(lease, clock)
    )

    assert (
        runtime.record_fenced_result(_result(request, outcome, marker=outcome.value, clock=clock))
        is None
    )
    attempt = runtime.get_fenced_attempt(request.attempt_id)
    assert attempt is not None and attempt.status is status
    assert runtime.get_fenced_receipt(request.attempt_id) is None


def test_indeterminate_recovery_is_append_only_and_terminal_replays_are_idempotent(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect = {"write": True}
    precondition = _precondition()
    lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    request = runtime.prepare_fenced_commit(
        lease.lease_id, effect, precondition, _evidence(lease, clock)
    )
    ambiguous = _result(request, FencedOutcome.INDETERMINATE, marker="lost-response", clock=clock)
    committed = _result(request, FencedOutcome.COMMITTED, marker="recovered", clock=clock)

    assert runtime.record_fenced_result(ambiguous) is None
    assert runtime.record_fenced_result(ambiguous) is None
    assert _table_count(db_path, "fenced_results") == 1
    receipt = runtime.record_fenced_result(committed)
    assert receipt is not None
    assert _table_count(db_path, "fenced_results") == 2
    assert runtime.record_fenced_result(committed) == receipt
    assert _table_count(db_path, "fenced_results") == 2
    assert _table_count(db_path, "fenced_receipts") == 1


def test_late_indeterminate_result_preserves_known_terminal_commit(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect = {"write": True}
    precondition = _precondition()
    lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    request = runtime.prepare_fenced_commit(
        lease.lease_id, effect, precondition, _evidence(lease, clock)
    )
    committed = _result(request, FencedOutcome.COMMITTED, marker="known", clock=clock)
    late = _result(request, FencedOutcome.INDETERMINATE, marker="late-timeout", clock=clock)

    receipt = runtime.record_fenced_result(committed)
    assert receipt is not None
    assert runtime.record_fenced_result(late) == receipt

    attempt = runtime.get_fenced_attempt(request.attempt_id)
    assert attempt is not None and attempt.status is FencedAttemptStatus.COMMITTED
    assert runtime.get_fenced_receipt(request.attempt_id) == receipt
    assert _table_count(db_path, "fenced_results") == 2
    with runtime.store.transaction(write=False) as tx:
        assert tx.get_coherence_fault() is None


def test_conflicting_terminal_result_is_persisted_as_disputed_and_fails_closed(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect = {"write": True}
    precondition = _precondition()
    lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    request = runtime.prepare_fenced_commit(
        lease.lease_id, effect, precondition, _evidence(lease, clock)
    )
    first = _result(request, FencedOutcome.COMMITTED, marker="first", clock=clock)
    conflict = _result(request, FencedOutcome.COMMITTED, marker="second", clock=clock)
    assert runtime.record_fenced_result(first) is not None

    with pytest.raises(ConflictError, match="conflicting terminal"):
        runtime.record_fenced_result(conflict)

    attempt = runtime.get_fenced_attempt(request.attempt_id)
    assert attempt is not None and attempt.status is FencedAttemptStatus.DISPUTED
    assert _table_count(db_path, "fenced_results") == 2
    with pytest.raises(ConflictError, match="disputed"):
        runtime.get_fenced_receipt(request.attempt_id)
    with pytest.raises(ConflictError, match="disputed"):
        runtime.record_fenced_result(first)
    with runtime.store.transaction(write=False) as tx:
        assert tx.get_coherence_fault() is not None


def test_prepare_deep_freezes_effect_and_uses_one_shared_digest_domain(
    db_path: Path, clock: ManualClock
) -> None:
    runtime, artifact = _runtime(db_path, clock)
    effect: dict[str, Digestable] = {
        "nested": {"revision": 1},
        "items": ["one", "two"],
    }
    precondition = _precondition()
    lease = _acquire(runtime, artifact, effect=effect, precondition=precondition)
    request = runtime.prepare_fenced_commit(
        lease.lease_id, effect, precondition, _evidence(lease, clock)
    )

    effect["nested"] = {"revision": 99}
    effect["items"] = ["changed"]
    assert request.effect["nested"]["revision"] == 1
    assert request.effect["items"] == ("one", "two")
    assert (
        digest_effect(request.effect, operation=request.operation, target=request.target)
        == request.effect_digest
    )
    with pytest.raises(TypeError, match="immutable"):
        request.effect["nested"]["revision"] = 2
    with pytest.raises(TypeError, match="immutable"):
        request.effect["new"] = True


def test_concurrent_identical_prepare_calls_reuse_one_attempt(
    db_path: Path, clock: ManualClock
) -> None:
    creator, artifact = _runtime(db_path, clock)
    effect = {"write": True}
    precondition = _precondition()
    lease = _acquire(creator, artifact, effect=effect, precondition=precondition)
    evidence = _evidence(lease, clock)
    barrier = Barrier(2)
    lock = Lock()
    requests: list[FencedCommitRequest] = []
    errors: list[BaseException] = []

    def prepare() -> None:
        runtime = TruthLease(
            SQLiteStore(db_path),
            clock=clock,
            trusted_evidence_authorities={"evidence:test"},
            trusted_target_adapters={"adapter:test"},
        )
        barrier.wait()
        try:
            request = runtime.prepare_fenced_commit(lease.lease_id, effect, precondition, evidence)
            with lock:
                requests.append(request)
        except BaseException as error:
            with lock:
                errors.append(error)

    threads = [Thread(target=prepare), Thread(target=prepare)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(requests) == 2
    assert {request.attempt_id for request in requests} == {requests[0].attempt_id}
    assert {request.request_digest for request in requests} == {requests[0].request_digest}
    assert _table_count(db_path, "fenced_attempts") == 1
    assert _table_count(db_path, "gate_receipts") == 1
