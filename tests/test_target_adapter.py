from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from conftest import ManualClock, SequentialIds

from truthlease.canonical import Digestable, digest_json
from truthlease.engine import TruthLease
from truthlease.models import (
    ArtifactDependency,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    FenceCoverage,
    FencedAttempt,
    FencedAttemptStatus,
    FencedCommitRequest,
    FencedOutcome,
    FencedReceipt,
    FencedResult,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import SQLiteStore
from truthlease.target import TargetAdapter, execute_fenced


def _precondition(adapter_id: str = "adapter:test") -> TargetPrecondition:
    return TargetPrecondition(
        adapter_id=adapter_id,
        mechanism="test-cas",
        expected_token="version-1",
        protected_observations=("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )


def _request(adapter_id: str = "adapter:test") -> FencedCommitRequest:
    prepared_at = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
    return FencedCommitRequest(
        attempt_id="attempt_0001",
        lease_id="lease_0001",
        gate_receipt_id="receipt_0001",
        adapter_id=adapter_id,
        effect={"write": True},
        target="target",
        operation="write",
        closure_digest="sha256:closure",
        effect_digest="sha256:effect",
        precondition=_precondition(adapter_id),
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        request_digest="sha256:request",
        coverage=FenceCoverage.FULL,
        prepared_at=prepared_at,
        expires_at=prepared_at + timedelta(seconds=30),
    )


@dataclass
class _RuntimeDouble:
    request: FencedCommitRequest
    prepare_calls: int = 0
    recorded: list[FencedResult] = field(default_factory=list)

    def prepare_fenced_commit(
        self,
        *,
        lease_id: str,
        effect: Digestable,
        target_precondition: TargetPrecondition,
        evidence: AssuranceEvidence | None = None,
    ) -> FencedCommitRequest:
        self.prepare_calls += 1
        assert lease_id == self.request.lease_id
        assert effect == self.request.effect
        assert target_precondition.adapter_id == self.request.adapter_id
        assert evidence is None
        return self.request

    def record_fenced_result(self, result: FencedResult) -> FencedReceipt | None:
        self.recorded.append(result)
        return None

    def get_fenced_attempt(self, attempt_id: str) -> FencedAttempt | None:
        assert attempt_id == self.request.attempt_id
        return FencedAttempt(
            attempt_id=self.request.attempt_id,
            lease_id=self.request.lease_id,
            gate_receipt_id=self.request.gate_receipt_id,
            adapter_id=self.request.adapter_id,
            request_digest=self.request.request_digest,
            precondition_digest=self.request.precondition_digest,
            scope_digest=self.request.scope_digest,
            replay_assurance=self.request.precondition.replay_assurance,
            coverage=self.request.coverage,
            status=FencedAttemptStatus.PREPARED,
            prepared_at=self.request.prepared_at,
        )


@dataclass
class _RaisingAdapter:
    adapter_id: str
    seen_attempts: list[str] = field(default_factory=list)

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        self.seen_attempts.append(request.attempt_id)
        raise TimeoutError("ambiguous target timeout")


@dataclass
class _MalformedAdapter:
    mode: str
    adapter_id: str = "adapter:test"
    seen_attempt_id: str | None = None

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        self.seen_attempt_id = request.attempt_id
        if self.mode == "wrong-type":
            return cast(FencedResult, object())
        result = FencedResult(
            attempt_id=request.attempt_id,
            lease_id=request.lease_id,
            adapter_id=request.adapter_id,
            outcome=FencedOutcome.COMMITTED,
            request_digest="sha256:wrong-binding",
            closure_digest=request.closure_digest,
            effect_digest=request.effect_digest,
            target=request.target,
            operation=request.operation,
            precondition_digest=request.precondition_digest,
            scope_digest=request.scope_digest,
            replay_assurance=request.precondition.replay_assurance,
            coverage=request.coverage,
            target_result_digest="sha256:claimed-result",
            target_receipt_ref="target-receipt:malformed",
            committed_at=datetime(2026, 7, 20, 12, 0, tzinfo=UTC),
        )
        if self.mode == "wrong-outcome":
            object.__setattr__(result, "outcome", "bogus")
        return result


@dataclass
class _TerminalAdapter:
    outcome: FencedOutcome
    adapter_id: str = "adapter:test"
    commit_calls: int = 0

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        self.commit_calls += 1
        committed = self.outcome is FencedOutcome.COMMITTED
        return FencedResult(
            attempt_id=request.attempt_id,
            lease_id=request.lease_id,
            adapter_id=request.adapter_id,
            outcome=self.outcome,
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
                {"outcome": self.outcome.value}, domain="test:terminal-result"
            ),
            target_receipt_ref="target-receipt:terminal" if committed else None,
            committed_at=datetime(2026, 7, 20, 12, 0, tzinfo=UTC) if committed else None,
        )


@dataclass
class _IndependentWriterAdapter:
    db_path: Path
    adapter_id: str = "adapter:test"
    acquired_write_lock: bool = False
    seen_attempt_id: str | None = None

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        self.seen_attempt_id = request.attempt_id
        connection = sqlite3.connect(self.db_path, isolation_level=None, timeout=0.2)
        try:
            connection.execute("BEGIN IMMEDIATE")
            self.acquired_write_lock = True
            connection.rollback()
        finally:
            connection.close()
        return FencedResult(
            attempt_id=request.attempt_id,
            lease_id=request.lease_id,
            adapter_id=request.adapter_id,
            outcome=FencedOutcome.INDETERMINATE,
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
                {"kind": "independent-writer-probe"},
                domain="test:target-result",
            ),
        )


def _fenced_runtime(
    db_path: Path,
    clock: ManualClock,
) -> tuple[
    TruthLease,
    str,
    dict[str, bool],
    TargetPrecondition,
    AssuranceEvidence,
]:
    runtime = TruthLease(
        SQLiteStore(db_path),
        clock=clock,
        ids=SequentialIds(),
        trusted_evidence_authorities={"evidence:test"},
        trusted_target_adapters={"adapter:test"},
    )
    runtime.observe(key="policy", version="v1", value={"enabled": True})
    artifact = runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"step": 1},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=DependencyCompleteness.OBSERVED,
    )
    effect = {"write": True}
    precondition = _precondition()
    lease = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target",
        operation="write",
        required_assurance=AssuranceLevel.FENCED,
        target_precondition=precondition,
    )
    evidence = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="evidence:test",
        evidence_digest="sha256:evidence",
        observed_at=clock.now(),
        lease_id=lease.lease_id,
        closure_digest=lease.closure_digest,
        effect_digest=lease.effect_digest,
        target=lease.target,
        operation=lease.operation,
    )
    return runtime, lease.lease_id, effect, precondition, evidence


def test_target_adapter_protocol_and_exception_preserve_attempt_as_indeterminate() -> None:
    request = _request()
    runtime = _RuntimeDouble(request)
    adapter = _RaisingAdapter(request.adapter_id)
    assert isinstance(adapter, TargetAdapter)

    receipt = execute_fenced(
        cast(TruthLease, runtime),
        adapter,
        lease_id=request.lease_id,
        effect={"write": True},
        target_precondition=request.precondition,
    )

    assert receipt is None
    assert runtime.prepare_calls == 1
    assert adapter.seen_attempts == [request.attempt_id]
    assert len(runtime.recorded) == 1
    assert runtime.recorded[0].attempt_id == request.attempt_id
    assert runtime.recorded[0].outcome is FencedOutcome.INDETERMINATE


def test_adapter_identity_mismatch_is_rejected_before_preparation_or_consumption() -> None:
    request = _request()
    runtime = _RuntimeDouble(request)
    adapter = _RaisingAdapter("adapter:wrong")

    with pytest.raises(ValueError, match="before preparation"):
        execute_fenced(
            cast(TruthLease, runtime),
            adapter,
            lease_id=request.lease_id,
            effect={"write": True},
            target_precondition=request.precondition,
        )

    assert runtime.prepare_calls == 0
    assert runtime.recorded == []
    assert adapter.seen_attempts == []


def test_adapter_commit_runs_after_prepare_transaction_releases_sqlite_writer(
    db_path: Path,
    clock: ManualClock,
) -> None:
    runtime, lease_id, effect, precondition, evidence = _fenced_runtime(db_path, clock)
    adapter = _IndependentWriterAdapter(db_path)

    receipt = execute_fenced(
        runtime,
        adapter,
        lease_id=lease_id,
        effect=effect,
        target_precondition=precondition,
        evidence=evidence,
    )

    assert receipt is None
    assert adapter.acquired_write_lock is True
    assert adapter.seen_attempt_id is not None
    attempt = runtime.get_fenced_attempt(adapter.seen_attempt_id)
    assert attempt is not None
    assert attempt.status is FencedAttemptStatus.INDETERMINATE


@pytest.mark.parametrize("mode", ["wrong-type", "wrong-binding", "wrong-outcome"])
def test_malformed_adapter_result_is_recorded_as_indeterminate(
    db_path: Path,
    clock: ManualClock,
    mode: str,
) -> None:
    runtime, lease_id, effect, precondition, evidence = _fenced_runtime(db_path, clock)
    adapter = _MalformedAdapter(mode)

    receipt = execute_fenced(
        runtime,
        adapter,
        lease_id=lease_id,
        effect=effect,
        target_precondition=precondition,
        evidence=evidence,
    )

    assert receipt is None
    assert adapter.seen_attempt_id is not None
    attempt = runtime.get_fenced_attempt(adapter.seen_attempt_id)
    assert attempt is not None
    assert attempt.status is FencedAttemptStatus.INDETERMINATE


@pytest.mark.parametrize(
    "outcome",
    [FencedOutcome.COMMITTED, FencedOutcome.NOT_COMMITTED],
)
def test_terminal_attempt_is_returned_without_redispatch(
    db_path: Path,
    clock: ManualClock,
    outcome: FencedOutcome,
) -> None:
    runtime, lease_id, effect, precondition, evidence = _fenced_runtime(db_path, clock)
    adapter = _TerminalAdapter(outcome)

    first = execute_fenced(
        runtime,
        adapter,
        lease_id=lease_id,
        effect=effect,
        target_precondition=precondition,
        evidence=evidence,
    )
    second = execute_fenced(
        runtime,
        adapter,
        lease_id=lease_id,
        effect=effect,
        target_precondition=precondition,
        evidence=evidence,
    )

    assert adapter.commit_calls == 1
    assert second == first
    assert (first is not None) is (outcome is FencedOutcome.COMMITTED)
    with runtime.store.transaction(write=False) as tx:
        assert tx.get_coherence_fault() is None
