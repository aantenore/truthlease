from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from truthlease.models import (
    AssuranceLevel,
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

NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


def _precondition() -> TargetPrecondition:
    return TargetPrecondition(
        adapter_id="adapter:test",
        mechanism="test-cas",
        expected_token="version-1",
        protected_observations=("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )


def _request() -> FencedCommitRequest:
    return FencedCommitRequest(
        attempt_id="attempt-1",
        lease_id="lease-1",
        gate_receipt_id="gate-1",
        adapter_id="adapter:test",
        effect={"write": True},
        target="target",
        operation="write",
        closure_digest="sha256:closure",
        effect_digest="sha256:effect",
        precondition=_precondition(),
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        request_digest="sha256:request",
        coverage=FenceCoverage.FULL,
        prepared_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def _result() -> FencedResult:
    request = _request()
    return FencedResult(
        attempt_id=request.attempt_id,
        lease_id=request.lease_id,
        adapter_id=request.adapter_id,
        outcome=FencedOutcome.COMMITTED,
        request_digest=request.request_digest,
        closure_digest=request.closure_digest,
        effect_digest=request.effect_digest,
        target=request.target,
        operation=request.operation,
        precondition_digest=request.precondition_digest,
        scope_digest=request.scope_digest,
        replay_assurance=ReplayAssurance.DEDUPLICATED,
        coverage=FenceCoverage.FULL,
        target_result_digest="sha256:result",
        target_receipt_ref="target-receipt-1",
        committed_at=NOW,
    )


def _attempt() -> FencedAttempt:
    request = _request()
    return FencedAttempt(
        attempt_id=request.attempt_id,
        lease_id=request.lease_id,
        gate_receipt_id=request.gate_receipt_id,
        adapter_id=request.adapter_id,
        request_digest=request.request_digest,
        precondition_digest=request.precondition_digest,
        scope_digest=request.scope_digest,
        replay_assurance=ReplayAssurance.DEDUPLICATED,
        coverage=FenceCoverage.FULL,
        status=FencedAttemptStatus.PREPARED,
        prepared_at=NOW,
    )


def _receipt() -> FencedReceipt:
    result = _result()
    return FencedReceipt(
        receipt_id="receipt-1",
        attempt_id=result.attempt_id,
        lease_id=result.lease_id,
        gate_receipt_id="gate-1",
        assurance=AssuranceLevel.FENCED,
        adapter_id=result.adapter_id,
        request_digest=result.request_digest,
        closure_digest=result.closure_digest,
        effect_digest=result.effect_digest,
        target=result.target,
        operation=result.operation,
        precondition_digest=result.precondition_digest,
        scope_digest=result.scope_digest,
        replay_assurance=result.replay_assurance,
        coverage=result.coverage,
        target_result_digest=result.target_result_digest,
        target_receipt_ref=result.target_receipt_ref or "",
        committed_at=result.committed_at or NOW,
        recorded_at=NOW,
        guarantee="historical conditional commit",
    )


def test_target_precondition_rejects_empty_or_duplicate_identity() -> None:
    with pytest.raises(ValueError, match="adapter_id"):
        replace(_precondition(), adapter_id="")
    with pytest.raises(ValueError, match="non-empty"):
        replace(_precondition(), protected_observations=("",))
    with pytest.raises(ValueError, match="duplicates"):
        replace(_precondition(), protected_observations=("policy", "policy"))


def test_fenced_request_rejects_unbound_or_invalid_values() -> None:
    request = _request()
    with pytest.raises(ValueError, match="attempt_id"):
        replace(request, attempt_id="")
    with pytest.raises(ValueError, match="algorithm-prefixed"):
        replace(request, request_digest="invalid")
    with pytest.raises(ValueError, match="adapter_id"):
        replace(request, adapter_id="adapter:other")
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(request, prepared_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="after prepared_at"):
        replace(request, expires_at=request.prepared_at)


def test_fenced_result_enforces_terminal_evidence_shape() -> None:
    result = _result()
    with pytest.raises(ValueError, match="target"):
        replace(result, target="")
    with pytest.raises(ValueError, match="algorithm-prefixed"):
        replace(result, target_result_digest="invalid")
    with pytest.raises(ValueError, match="target_receipt_ref"):
        replace(result, target_receipt_ref=None)
    with pytest.raises(ValueError, match="committed_at"):
        replace(result, committed_at=None)
    with pytest.raises(ValueError, match="cannot claim"):
        replace(result, outcome=FencedOutcome.NOT_COMMITTED)


def test_fenced_attempt_rejects_malformed_persisted_state() -> None:
    attempt = _attempt()
    with pytest.raises(ValueError, match="gate_receipt_id"):
        replace(attempt, gate_receipt_id="")
    with pytest.raises(ValueError, match="algorithm-prefixed"):
        replace(attempt, scope_digest="invalid")
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(attempt, prepared_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(attempt, completed_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="result_digest"):
        replace(attempt, result_digest="invalid")


def test_fenced_receipt_can_only_represent_full_deduplicated_assurance() -> None:
    receipt = _receipt()
    with pytest.raises(ValueError, match="must be fenced"):
        replace(receipt, assurance=AssuranceLevel.REVALIDATED)
    with pytest.raises(ValueError, match="deduplicated"):
        replace(receipt, replay_assurance=ReplayAssurance.UNKNOWN)
    with pytest.raises(ValueError, match="full closure"):
        replace(receipt, coverage=FenceCoverage.PARTIAL)
    with pytest.raises(ValueError, match="receipt_id"):
        replace(receipt, receipt_id="")
    with pytest.raises(ValueError, match="algorithm-prefixed"):
        replace(receipt, effect_digest="invalid")
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(receipt, committed_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(receipt, recorded_at=NOW.replace(tzinfo=None))
