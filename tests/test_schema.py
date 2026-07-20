from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from truthlease.cli import _jsonable
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

V1_SHARED_DEFINITIONS = (
    "digest",
    "artifactRef",
    "dependency",
    "observation",
    "artifact",
    "snapshot",
    "lease",
    "assuranceEvidence",
    "gateDecision",
)

OBSERVATION: dict[str, Any] = {
    "key": "policy/release",
    "version": "v12",
    "digest": "sha256:policy",
    "generation": 1,
    "observed_at": "2026-07-20T12:00:00Z",
    "valid_until": None,
    "status": "current",
    "source": "manual",
}
ARTIFACT: dict[str, Any] = {
    "ref": {"artifact_id": "release-plan", "revision": 1},
    "kind": "plan",
    "payload_digest": "sha256:artifact",
    "payload_ref": None,
    "dependencies": [
        {
            "kind": "observation",
            "dependency_id": "policy/release",
            "expected_digest": "sha256:policy",
            "expected_version": "v12",
            "expected_generation": 1,
            "expected_revision": None,
            "required": True,
        }
    ],
    "completeness": "observed",
    "status": "fresh",
    "reconcile_action": "replan",
    "created_at": "2026-07-20T12:00:01Z",
    "invalidated_at": None,
    "invalidation_reason": None,
}
LEASE: dict[str, Any] = {
    "lease_id": "lease_1",
    "artifact": {"artifact_id": "release-plan", "revision": 1},
    "artifact_digest": "sha256:artifact",
    "effect_digest": "sha256:effect",
    "target": "deployment/current",
    "operation": "promote",
    "required_assurance": "fenced",
    "dependency_completeness": "observed",
    "closure_digest": "sha256:closure",
    "snapshot": [
        {
            "key": "policy/release",
            "version": "v12",
            "generation": 1,
            "digest": "sha256:policy",
        }
    ],
    "issued_at": "2026-07-20T12:00:02Z",
    "expires_at": "2026-07-20T12:00:32Z",
    "boot_epoch": "persistent-local-v1",
    "status": "active",
    "consumed_at": None,
}
ASSURANCE_EVIDENCE: dict[str, Any] = {
    "level": "revalidated",
    "authority": "adapter:policy",
    "evidence_digest": "sha256:evidence",
    "observed_at": "2026-07-20T12:00:03Z",
    "lease_id": "lease_1",
    "closure_digest": "sha256:closure",
    "effect_digest": "sha256:effect",
    "target": "deployment/current",
    "operation": "promote",
}
GATE_DECISION: dict[str, Any] = {
    "allowed": True,
    "code": "EFFECT_DISPATCHABLE",
    "lease_id": "lease_1",
    "assurance": "observed",
    "guarantee": "Local-currentness evidence only.",
    "receipt_id": "receipt_gate_1",
    "changed_dependencies": [],
}
V1_DOCUMENTS = (OBSERVATION, ARTIFACT, LEASE, ASSURANCE_EVIDENCE, GATE_DECISION)

TARGET_PRECONDITION: dict[str, Any] = {
    "adapter_id": "http-etag:v1",
    "mechanism": "http-if-match",
    "expected_token": '"release-41"',
    "protected_observations": ["policy/release"],
    "replay_assurance": "deduplicated",
}
FENCED_COMMIT_REQUEST: dict[str, Any] = {
    "attempt_id": "attempt_1",
    "lease_id": "lease_1",
    "gate_receipt_id": "receipt_gate_1",
    "adapter_id": "http-etag:v1",
    "effect": {"release": "2026.07", "environment": "production"},
    "target": "deployment/current",
    "operation": "promote",
    "closure_digest": "sha256:closure",
    "effect_digest": "sha256:effect",
    "precondition": TARGET_PRECONDITION,
    "precondition_digest": "sha256:precondition",
    "scope_digest": "sha256:scope",
    "request_digest": "sha256:request",
    "coverage": "full",
    "prepared_at": "2026-07-20T12:00:04Z",
    "expires_at": "2026-07-20T12:00:32Z",
}
FENCED_ATTEMPT: dict[str, Any] = {
    "attempt_id": "attempt_1",
    "lease_id": "lease_1",
    "gate_receipt_id": "receipt_gate_1",
    "adapter_id": "http-etag:v1",
    "request_digest": "sha256:request",
    "precondition_digest": "sha256:precondition",
    "scope_digest": "sha256:scope",
    "replay_assurance": "deduplicated",
    "coverage": "full",
    "status": "prepared",
    "prepared_at": "2026-07-20T12:00:04Z",
    "completed_at": None,
    "result_digest": None,
}
FENCED_RESULT: dict[str, Any] = {
    "attempt_id": "attempt_1",
    "lease_id": "lease_1",
    "adapter_id": "http-etag:v1",
    "outcome": "committed",
    "request_digest": "sha256:request",
    "closure_digest": "sha256:closure",
    "effect_digest": "sha256:effect",
    "target": "deployment/current",
    "operation": "promote",
    "precondition_digest": "sha256:precondition",
    "scope_digest": "sha256:scope",
    "replay_assurance": "deduplicated",
    "coverage": "full",
    "target_result_digest": "sha256:target-result",
    "target_receipt_ref": "target-receipt-41",
    "committed_at": "2026-07-20T12:00:05Z",
}
FENCED_RECEIPT: dict[str, Any] = {
    "receipt_id": "receipt_fenced_1",
    "attempt_id": "attempt_1",
    "lease_id": "lease_1",
    "gate_receipt_id": "receipt_gate_1",
    "assurance": "fenced",
    "adapter_id": "http-etag:v1",
    "request_digest": "sha256:request",
    "closure_digest": "sha256:closure",
    "effect_digest": "sha256:effect",
    "target": "deployment/current",
    "operation": "promote",
    "precondition_digest": "sha256:precondition",
    "scope_digest": "sha256:scope",
    "replay_assurance": "deduplicated",
    "coverage": "full",
    "target_result_digest": "sha256:target-result",
    "target_receipt_ref": "target-receipt-41",
    "committed_at": "2026-07-20T12:00:05Z",
    "recorded_at": "2026-07-20T12:00:06Z",
    "guarantee": "The target atomically enforced the bound full-scope precondition.",
}


def _schema(version: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads(Path(f"schemas/{version}/coherence.schema.json").read_text()),
    )


def _validator(version: str) -> Draft202012Validator:
    return Draft202012Validator(_schema(version), format_checker=FormatChecker())


@pytest.mark.parametrize("version", ("v1", "v2"))
def test_public_contract_schemas_are_valid_offline(version: str) -> None:
    schema = _schema(version)

    Draft202012Validator.check_schema(schema)
    assert all(not value.startswith("http") for value in _all_refs(schema)), (
        "contract schemas must not require remote reference resolution"
    )


def test_v2_preserves_every_v1_definition_and_document() -> None:
    v1 = _schema("v1")
    v2 = _schema("v2")
    for name in V1_SHARED_DEFINITIONS:
        assert v2["$defs"][name] == v1["$defs"][name]
    for document in V1_DOCUMENTS:
        _validator("v1").validate(document)
        _validator("v2").validate(document)


@pytest.mark.parametrize(
    "document",
    (
        TARGET_PRECONDITION,
        FENCED_COMMIT_REQUEST,
        FENCED_ATTEMPT,
        FENCED_RESULT,
        FENCED_RECEIPT,
    ),
)
def test_v2_accepts_each_target_fencing_read_model(document: dict[str, Any]) -> None:
    _validator("v2").validate(document)
    assert not _validator("v1").is_valid(document)


def test_v2_documents_match_serialized_public_models() -> None:
    prepared_at = datetime(2026, 7, 20, 12, 0, 4, tzinfo=UTC)
    committed_at = datetime(2026, 7, 20, 12, 0, 5, tzinfo=UTC)
    recorded_at = datetime(2026, 7, 20, 12, 0, 6, tzinfo=UTC)
    expires_at = datetime(2026, 7, 20, 12, 0, 32, tzinfo=UTC)
    precondition = TargetPrecondition(
        adapter_id="http-etag:v1",
        mechanism="http-if-match",
        expected_token='"release-41"',
        protected_observations=("policy/release",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )
    request = FencedCommitRequest(
        attempt_id="attempt_1",
        lease_id="lease_1",
        gate_receipt_id="receipt_gate_1",
        adapter_id="http-etag:v1",
        effect={"release": "2026.07", "environment": "production"},
        target="deployment/current",
        operation="promote",
        closure_digest="sha256:closure",
        effect_digest="sha256:effect",
        precondition=precondition,
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        request_digest="sha256:request",
        coverage=FenceCoverage.FULL,
        prepared_at=prepared_at,
        expires_at=expires_at,
    )
    attempt = FencedAttempt(
        attempt_id="attempt_1",
        lease_id="lease_1",
        gate_receipt_id="receipt_gate_1",
        adapter_id="http-etag:v1",
        request_digest="sha256:request",
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        replay_assurance=ReplayAssurance.DEDUPLICATED,
        coverage=FenceCoverage.FULL,
        status=FencedAttemptStatus.PREPARED,
        prepared_at=prepared_at,
    )
    result = FencedResult(
        attempt_id="attempt_1",
        lease_id="lease_1",
        adapter_id="http-etag:v1",
        outcome=FencedOutcome.COMMITTED,
        request_digest="sha256:request",
        closure_digest="sha256:closure",
        effect_digest="sha256:effect",
        target="deployment/current",
        operation="promote",
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        replay_assurance=ReplayAssurance.DEDUPLICATED,
        coverage=FenceCoverage.FULL,
        target_result_digest="sha256:target-result",
        target_receipt_ref="target-receipt-41",
        committed_at=committed_at,
    )
    receipt = FencedReceipt(
        receipt_id="receipt_fenced_1",
        attempt_id="attempt_1",
        lease_id="lease_1",
        gate_receipt_id="receipt_gate_1",
        assurance=AssuranceLevel.FENCED,
        adapter_id="http-etag:v1",
        request_digest="sha256:request",
        closure_digest="sha256:closure",
        effect_digest="sha256:effect",
        target="deployment/current",
        operation="promote",
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        replay_assurance=ReplayAssurance.DEDUPLICATED,
        coverage=FenceCoverage.FULL,
        target_result_digest="sha256:target-result",
        target_receipt_ref="target-receipt-41",
        committed_at=committed_at,
        recorded_at=recorded_at,
        guarantee="The target atomically enforced the bound full-scope precondition.",
    )

    for model, document in (
        (precondition, TARGET_PRECONDITION),
        (request, FENCED_COMMIT_REQUEST),
        (attempt, FENCED_ATTEMPT),
        (result, FENCED_RESULT),
        (receipt, FENCED_RECEIPT),
    ):
        serialized = cast(dict[str, Any], _jsonable(model))
        assert serialized == document
        _validator("v2").validate(serialized)


@pytest.mark.parametrize("outcome", ("not_committed", "indeterminate"))
def test_non_committed_results_require_null_commit_evidence(outcome: str) -> None:
    result = copy.deepcopy(FENCED_RESULT)
    result.update(
        outcome=outcome,
        target_receipt_ref=None,
        committed_at=None,
    )

    _validator("v2").validate(result)
    result["target_receipt_ref"] = "must-not-exist"
    assert not _validator("v2").is_valid(result)


def test_committed_result_requires_bound_commit_evidence() -> None:
    result = copy.deepcopy(FENCED_RESULT)
    result["target_receipt_ref"] = None
    result["committed_at"] = None

    assert not _validator("v2").is_valid(result)


@pytest.mark.parametrize(
    ("field", "value"),
    (("coverage", "partial"), ("replay_assurance", "one_shot"), ("assurance", "observed")),
)
def test_fenced_receipt_requires_full_deduplicated_fenced_evidence(field: str, value: str) -> None:
    receipt = copy.deepcopy(FENCED_RECEIPT)
    receipt[field] = value

    assert not _validator("v2").is_valid(receipt)


@pytest.mark.parametrize(
    "document",
    (
        TARGET_PRECONDITION,
        FENCED_COMMIT_REQUEST,
        FENCED_ATTEMPT,
        FENCED_RESULT,
        FENCED_RECEIPT,
    ),
)
def test_v2_read_models_are_closed(document: dict[str, Any]) -> None:
    spoofed = copy.deepcopy(document)
    spoofed["unbound_claim"] = True

    assert not _validator("v2").is_valid(spoofed)


def _all_refs(value: Any) -> list[str]:
    if isinstance(value, dict):
        references = [value["$ref"]] if isinstance(value.get("$ref"), str) else []
        return references + [reference for item in value.values() for reference in _all_refs(item)]
    if isinstance(value, list):
        return [reference for item in value for reference in _all_refs(item)]
    return []
