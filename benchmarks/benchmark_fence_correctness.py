"""Deterministic correctness benchmark for the target-fencing protocol."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from truthlease.canonical import Digestable, digest_json
from truthlease.engine import TruthLease
from truthlease.models import (
    ArtifactDependency,
    ArtifactRef,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    FencedAttemptStatus,
    FencedCommitRequest,
    FencedOutcome,
    FencedResult,
    Lease,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import SQLiteStore
from truthlease.target import execute_fenced


class _Clock:
    def __init__(self) -> None:
        self.current = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current


class _Ids:
    def __init__(self) -> None:
        self.counter = 0

    def new(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}_{self.counter:06d}"


@dataclass
class _AtomicTarget:
    """Reference CAS and deduplication target for benchmark correctness only."""

    clock: _Clock
    adapter_id: str = "benchmark:atomic-cas"
    etag: str = '"v1"'
    effect_count: int = 0
    lose_next_response: bool = False
    accepted_attempts: set[str] = field(default_factory=set)
    results: dict[str, FencedResult] = field(default_factory=dict)
    seen_attempts: list[str] = field(default_factory=list)

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        self.seen_attempts.append(request.attempt_id)
        prior = self.results.get(request.attempt_id)
        if prior is not None:
            return prior
        if request.precondition.expected_token != self.etag:
            result = self._result(request, FencedOutcome.NOT_COMMITTED)
            self.results[request.attempt_id] = result
            return result

        self.effect_count += 1
        self.accepted_attempts.add(request.attempt_id)
        self.etag = f'"v{int(self.etag[2:-1]) + 1}"'
        result = self._result(request, FencedOutcome.COMMITTED)
        self.results[request.attempt_id] = result
        if self.lose_next_response:
            self.lose_next_response = False
            raise TimeoutError("benchmark simulates a response lost after commit")
        return result

    def _result(
        self,
        request: FencedCommitRequest,
        outcome: FencedOutcome,
    ) -> FencedResult:
        committed = outcome is FencedOutcome.COMMITTED
        marker: dict[str, Digestable] = {
            "attempt_id": request.attempt_id,
            "outcome": outcome.value,
        }
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
            target_result_digest=digest_json(marker, domain="benchmark:target-result"),
            target_receipt_ref=(f"benchmark-receipt:{request.attempt_id}" if committed else None),
            committed_at=self.clock.now() if committed else None,
        )


def _evidence(lease: Lease, clock: _Clock) -> AssuranceEvidence:
    return AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="benchmark:reader",
        evidence_digest=digest_json(
            {"lease_id": lease.lease_id},
            domain="benchmark:revalidation",
        ),
        observed_at=clock.now(),
        lease_id=lease.lease_id,
        closure_digest=lease.closure_digest,
        effect_digest=lease.effect_digest,
        target=lease.target,
        operation=lease.operation,
    )


def _precondition(target: _AtomicTarget, *, full: bool) -> TargetPrecondition:
    return TargetPrecondition(
        adapter_id=target.adapter_id,
        mechanism="benchmark-cas",
        expected_token=target.etag,
        protected_observations=("policy", "schema") if full else ("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )


def _lease(
    runtime: TruthLease,
    artifact: ArtifactRef,
    target: _AtomicTarget,
    *,
    case: str,
    full: bool,
) -> tuple[Lease, dict[str, Digestable], TargetPrecondition]:
    effect: dict[str, Digestable] = {"case": case, "write": True}
    precondition = _precondition(target, full=full)
    lease = runtime.acquire_lease(
        artifact=artifact,
        effect=effect,
        target="benchmark-target/current",
        operation="write",
        required_assurance=(AssuranceLevel.FENCED if full else AssuranceLevel.OBSERVED),
        target_precondition=precondition,
    )
    return lease, effect, precondition


def run_benchmark() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="truthlease-fence-benchmark-") as directory:
        clock = _Clock()
        runtime = TruthLease(
            SQLiteStore(Path(directory) / "benchmark.db"),
            clock=clock,
            ids=_Ids(),
            trusted_evidence_authorities={"benchmark:reader"},
            trusted_target_adapters={"benchmark:atomic-cas"},
        )
        for key in ("policy", "schema"):
            runtime.observe(key=key, version="v1", value={"key": key})
        artifact = runtime.publish_artifact(
            artifact_id="benchmark-plan",
            kind="plan",
            payload={"steps": ["write"]},
            dependencies=[
                ArtifactDependency.observation(runtime.get_observation(key))
                for key in ("policy", "schema")
            ],
            completeness=DependencyCompleteness.OBSERVED,
        )
        target = _AtomicTarget(clock)

        success_lease, success_effect, success_precondition = _lease(
            runtime, artifact.ref, target, case="success", full=True
        )
        success = execute_fenced(
            runtime,
            target,
            lease_id=success_lease.lease_id,
            effect=success_effect,
            target_precondition=success_precondition,
            evidence=_evidence(success_lease, clock),
        )

        stale_lease, stale_effect, stale_precondition = _lease(
            runtime, artifact.ref, target, case="stale-target", full=True
        )
        target.etag = '"v3"'
        stale = execute_fenced(
            runtime,
            target,
            lease_id=stale_lease.lease_id,
            effect=stale_effect,
            target_precondition=stale_precondition,
            evidence=_evidence(stale_lease, clock),
        )
        stale_attempt = runtime.get_fenced_attempt(target.seen_attempts[-1])

        recovery_lease, recovery_effect, recovery_precondition = _lease(
            runtime, artifact.ref, target, case="lost-response", full=True
        )
        target.lose_next_response = True
        ambiguous = execute_fenced(
            runtime,
            target,
            lease_id=recovery_lease.lease_id,
            effect=recovery_effect,
            target_precondition=recovery_precondition,
            evidence=_evidence(recovery_lease, clock),
        )
        recovery_attempt_id = target.seen_attempts[-1]
        ambiguous_attempt = runtime.get_fenced_attempt(recovery_attempt_id)
        recovered = execute_fenced(
            runtime,
            target,
            lease_id=recovery_lease.lease_id,
            effect=recovery_effect,
            target_precondition=recovery_precondition,
            evidence=_evidence(recovery_lease, clock),
        )

        partial_lease, partial_effect, partial_precondition = _lease(
            runtime, artifact.ref, target, case="partial", full=False
        )
        partial = execute_fenced(
            runtime,
            target,
            lease_id=partial_lease.lease_id,
            effect=partial_effect,
            target_precondition=partial_precondition,
            evidence=_evidence(partial_lease, clock),
        )
        partial_attempt = runtime.get_fenced_attempt(target.seen_attempts[-1])

        correctness = {
            "success_has_fenced_receipt": success is not None,
            "stale_target_has_no_receipt": stale is None,
            "stale_target_is_certainly_not_committed": (
                stale_attempt is not None
                and stale_attempt.status is FencedAttemptStatus.NOT_COMMITTED
            ),
            "ambiguous_not_promoted_before_resolution": (
                ambiguous is None
                and ambiguous_attempt is not None
                and ambiguous_attempt.status is FencedAttemptStatus.INDETERMINATE
            ),
            "same_attempt_recovers_to_one_receipt": (
                recovered is not None and recovered.attempt_id == recovery_attempt_id
            ),
            "partial_coverage_has_no_fenced_receipt": partial is None,
            "partial_commit_is_durably_audited": (
                partial_attempt is not None
                and partial_attempt.status is FencedAttemptStatus.COMMITTED
                and partial_attempt.result_digest is not None
            ),
            "no_duplicate_target_effects": (
                target.effect_count == len(target.accepted_attempts) == 3
            ),
        }
        return {
            "schema_version": 1,
            "scenarios": 4,
            "target_effect_count": target.effect_count,
            "unique_accepted_attempts": len(target.accepted_attempts),
            "correctness": correctness,
            "passed": all(correctness.values()),
            "note": (
                "This deterministic harness measures protocol invariants, not universal "
                "network latency or target trustworthiness."
            ),
        }


def main() -> None:
    result = run_benchmark()
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
