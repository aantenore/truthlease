"""LangGraph persistence with TruthLease commit-time coherence checks.

LangGraph remembers where a workflow paused. TruthLease independently decides
whether the referenced artifact and target precondition are still valid when
the workflow resumes. The graph checkpoints only references, digests, durable
identifiers, and stable decision codes; resolvers keep raw effects and target
tokens outside checkpoint storage.

The node names and state fields in this module are a persistence contract for
paused workflows. Rename them only with an explicit checkpoint migration.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import timedelta
from typing import Literal, TypedDict

try:
    from langgraph.checkpoint.base import BaseCheckpointSaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.state import CompiledStateGraph
    from langgraph.types import interrupt
except ModuleNotFoundError as error:  # pragma: no cover - exercised without the extra
    if error.name is not None and error.name.startswith("langgraph"):
        raise ImportError(
            "LangGraph support is optional; install it with `pip install 'truthlease[langgraph]'`."
        ) from error
    raise

from truthlease.canonical import Digestable, digest_json
from truthlease.engine import TruthLease, digest_effect
from truthlease.errors import TruthLeaseError
from truthlease.models import (
    ArtifactRef,
    AssuranceEvidence,
    AssuranceLevel,
    FencedAttempt,
    FencedAttemptStatus,
    FencedReceipt,
    Lease,
    TargetPrecondition,
)
from truthlease.target import TargetAdapter, execute_fenced

LANGGRAPH_STATE_VERSION = 1
_DIGEST_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*:[A-Za-z0-9._~+/=-]+$")

Phase = Literal[
    "awaiting_approval",
    "approved",
    "ready_to_execute",
    "awaiting_recovery",
    "committed",
    "reconcile",
]


class StaleSafeState(TypedDict, total=False):
    """Digest-only checkpoint contract for a stale-safe target effect."""

    schema_version: int
    idempotency_key: str
    artifact_id: str
    artifact_revision: int
    effect_ref: str
    effect_digest: str
    precondition_ref: str
    precondition_digest: str
    target: str
    operation: str
    phase: Phase
    approved: bool
    recovery_approved: bool
    recovery_basis_digest: str
    lease_id: str
    attempt_id: str
    receipt_id: str
    decision_code: str
    reconciliation_cause: str


Revalidate = Callable[[TruthLease, ArtifactRef], None]
EffectResolver = Callable[[str], Digestable]
PreconditionResolver = Callable[[str], TargetPrecondition]
EvidenceProvider = Callable[[TruthLease, Lease], AssuranceEvidence]
StaleSafeGraph = CompiledStateGraph[
    StaleSafeState,
    None,
    StaleSafeState,
    StaleSafeState,
]


def workflow_idempotency_key(stable_workflow_key: Digestable) -> str:
    """Convert a stable caller key into the digest persisted by both stores."""
    return digest_json(stable_workflow_key, domain="langgraph:workflow-idempotency:v1")


def make_resume_state(
    *,
    idempotency_key: str,
    artifact: ArtifactRef,
    effect_ref: str,
    effect: Digestable,
    precondition_ref: str,
    target_precondition: TargetPrecondition,
    target: str,
    operation: str,
) -> StaleSafeState:
    """Create the only supported initial state without checkpointing raw inputs."""
    _require_digest("idempotency_key", idempotency_key)
    for name, value in (
        ("effect_ref", effect_ref),
        ("precondition_ref", precondition_ref),
        ("target", target),
        ("operation", operation),
    ):
        _require_text(name, value)
    return StaleSafeState(
        schema_version=LANGGRAPH_STATE_VERSION,
        idempotency_key=idempotency_key,
        artifact_id=artifact.artifact_id,
        artifact_revision=artifact.revision,
        effect_ref=effect_ref,
        effect_digest=digest_effect(effect, operation=operation, target=target),
        precondition_ref=precondition_ref,
        precondition_digest=_precondition_descriptor_digest(target_precondition),
        target=target,
        operation=operation,
        phase="awaiting_approval",
        approved=False,
        recovery_approved=False,
        recovery_basis_digest="",
        decision_code="AWAITING_APPROVAL",
    )


def build_stale_safe_graph(
    runtime: TruthLease,
    *,
    checkpointer: BaseCheckpointSaver[str],
    revalidate: Revalidate,
    resolve_effect: EffectResolver,
    resolve_precondition: PreconditionResolver,
    evidence_provider: EvidenceProvider,
    target_adapter: TargetAdapter,
    lease_ttl: timedelta = timedelta(seconds=30),
) -> StaleSafeGraph:
    """Compile a restart-safe approval and fenced-commit workflow.

    The supplied callbacks are deliberately replaceable. ``revalidate`` must
    re-read authoritative sources and commit any changed observations before
    returning. Resolvers must reconstruct raw values from references and are
    invoked again immediately before target execution.
    """
    if lease_ttl <= timedelta(0):
        raise ValueError("lease_ttl must be positive")

    def approval(state: StaleSafeState) -> StaleSafeState:
        failure = _state_failure(state, expected_phases={"awaiting_approval"})
        if failure is not None:
            return failure
        approved = interrupt(
            {
                "kind": "truthlease_effect_approval",
                "schema_version": LANGGRAPH_STATE_VERSION,
                "artifact_id": state["artifact_id"],
                "artifact_revision": state["artifact_revision"],
                "effect_digest": state["effect_digest"],
                "precondition_digest": state["precondition_digest"],
                "target": state["target"],
                "operation": state["operation"],
            }
        )
        if type(approved) is not bool:
            return _reconcile("APPROVAL_RESPONSE_INVALID", "approval")
        if not approved:
            return {
                "approved": False,
                **_reconcile("APPROVAL_DENIED", "approval"),
            }
        return {
            "approved": True,
            "phase": "approved",
            "decision_code": "APPROVED_FOR_REVALIDATION",
            "reconciliation_cause": "",
        }

    def authorize(state: StaleSafeState) -> StaleSafeState:
        failure = _state_failure(state, expected_phases={"approved"})
        if failure is not None:
            return failure
        artifact = ArtifactRef(state["artifact_id"], state["artifact_revision"])
        try:
            revalidate(runtime, artifact)
        except TruthLeaseError as error:
            return _reconcile(error.code, "artifact")
        except Exception:
            return _reconcile("AUTHORITATIVE_REVALIDATION_FAILED", "artifact")
        try:
            effect, precondition = _resolve_and_verify(
                state,
                resolve_effect=resolve_effect,
                resolve_precondition=resolve_precondition,
            )
        except _ReferenceFailure as error:
            return _reconcile(error.code, "resolver")
        try:
            lease = runtime.acquire_lease(
                artifact=artifact,
                effect=effect,
                target=state["target"],
                operation=state["operation"],
                required_assurance=AssuranceLevel.FENCED,
                ttl=lease_ttl,
                target_precondition=precondition,
                idempotency_key=state["idempotency_key"],
            )
        except TruthLeaseError as error:
            return _reconcile(error.code, "artifact")
        except Exception:
            return _reconcile("LEASE_ACQUISITION_FAILED", "lease")
        return {
            "lease_id": lease.lease_id,
            "phase": "ready_to_execute",
            "recovery_approved": False,
            "recovery_basis_digest": "",
            "decision_code": "FENCED_LEASE_ACQUIRED",
            "reconciliation_cause": "",
        }

    def execute(state: StaleSafeState) -> StaleSafeState:
        failure = _state_failure(state, expected_phases={"ready_to_execute"})
        if failure is not None:
            return failure
        lease_id = state.get("lease_id")
        if not isinstance(lease_id, str) or not lease_id:
            return _reconcile("LEASE_REFERENCE_MISSING", "state")
        try:
            existing = runtime.get_fenced_attempt_for_lease(lease_id)
        except TruthLeaseError as error:
            return _reconcile(error.code, "state")
        except Exception:
            return _reconcile("ATTEMPT_STATE_READ_FAILED", "state")
        ambiguous_statuses = {
            FencedAttemptStatus.PREPARED,
            FencedAttemptStatus.INDETERMINATE,
        }
        if existing is not None and existing.status not in ambiguous_statuses:
            return _state_from_durable_attempt(
                runtime,
                existing,
                missing_code="FENCED_ATTEMPT_MISSING",
                missing_cause="state",
            )
        if (
            existing is not None
            and existing.status in ambiguous_statuses
            and (
                state.get("recovery_approved") is not True
                or state.get("recovery_basis_digest") != _attempt_recovery_basis(existing)
            )
        ):
            return {
                "attempt_id": existing.attempt_id,
                "phase": "awaiting_recovery",
                "recovery_approved": False,
                "recovery_basis_digest": _attempt_recovery_basis(existing),
                "decision_code": "TARGET_OUTCOME_INDETERMINATE",
                "reconciliation_cause": "target",
            }
        try:
            effect, precondition = _resolve_and_verify(
                state,
                resolve_effect=resolve_effect,
                resolve_precondition=resolve_precondition,
            )
        except _ReferenceFailure as error:
            return _reconcile(error.code, "resolver")
        evidence: AssuranceEvidence | None = None
        if existing is None:
            try:
                lease = runtime.get_lease(lease_id)
            except TruthLeaseError as error:
                return _reconcile(error.code, "state")
            except Exception:
                return _reconcile("LEASE_STATE_READ_FAILED", "state")
            if lease is None:
                return _reconcile("LEASE_NOT_FOUND", "state")
            try:
                evidence = evidence_provider(runtime, lease)
            except Exception:
                return _reconcile("EVIDENCE_PROVIDER_FAILED", "evidence")
        try:
            receipt = execute_fenced(
                runtime,
                target_adapter,
                lease_id=lease_id,
                effect=effect,
                target_precondition=precondition,
                evidence=evidence,
            )
        except TruthLeaseError as error:
            return _recover_after_execution_failure(
                runtime,
                lease_id=lease_id,
                fallback_code=error.code,
                fallback_cause="target",
            )
        except Exception:
            return _recover_after_execution_failure(
                runtime,
                lease_id=lease_id,
                fallback_code="TARGET_EXECUTION_FAILED",
                fallback_cause="target",
            )
        if receipt is not None:
            return _committed_state(receipt)
        try:
            attempt = runtime.get_fenced_attempt_for_lease(lease_id)
        except Exception as error:
            raise _PostExecutionStateUnavailable from error
        return _state_from_durable_attempt(
            runtime,
            attempt,
            missing_code="FENCED_ATTEMPT_MISSING",
            missing_cause="state",
        )

    def recovery(state: StaleSafeState) -> StaleSafeState:
        failure = _state_failure(state, expected_phases={"awaiting_recovery"})
        if failure is not None:
            return failure
        retry = interrupt(
            {
                "kind": "truthlease_recovery_decision",
                "schema_version": LANGGRAPH_STATE_VERSION,
                "artifact_id": state["artifact_id"],
                "artifact_revision": state["artifact_revision"],
                "effect_digest": state["effect_digest"],
                "attempt_id": state.get("attempt_id", ""),
                "recovery_basis_digest": state.get("recovery_basis_digest", ""),
                "decision_code": "TARGET_OUTCOME_INDETERMINATE",
            }
        )
        lease_id = state.get("lease_id")
        if not isinstance(lease_id, str) or not lease_id:
            return _reconcile("LEASE_REFERENCE_MISSING", "state")
        try:
            attempt = runtime.get_fenced_attempt_for_lease(lease_id)
        except TruthLeaseError as error:
            return _reconcile(error.code, "target")
        except Exception:
            return _reconcile("RECOVERY_STATE_READ_FAILED", "target")
        if attempt is None:
            return _reconcile("FENCED_ATTEMPT_MISSING", "state")
        if attempt.status not in {
            FencedAttemptStatus.PREPARED,
            FencedAttemptStatus.INDETERMINATE,
        }:
            return _state_from_durable_attempt(
                runtime,
                attempt,
                missing_code="FENCED_ATTEMPT_MISSING",
                missing_cause="state",
            )
        current_basis = _attempt_recovery_basis(attempt)
        if current_basis != state.get("recovery_basis_digest"):
            return {
                "phase": "ready_to_execute",
                "recovery_approved": False,
                "recovery_basis_digest": current_basis,
                "decision_code": "RECOVERY_BASIS_CHANGED",
                "reconciliation_cause": "target",
            }
        if type(retry) is not bool:
            return _reconcile("RECOVERY_RESPONSE_INVALID", "target")
        if not retry:
            return _reconcile("RECOVERY_DECLINED", "target")
        return {
            "phase": "ready_to_execute",
            "recovery_approved": True,
            "recovery_basis_digest": current_basis,
            "decision_code": "RECOVERY_RETRY_APPROVED",
            "reconciliation_cause": "",
        }

    builder = StateGraph(StaleSafeState)
    builder.add_node("approval_v1", approval)
    builder.add_node("authorize_v1", authorize)
    builder.add_node("execute_fenced_v1", execute)
    builder.add_node("recover_indeterminate_v1", recovery)
    builder.add_edge(START, "approval_v1")
    builder.add_conditional_edges(
        "approval_v1",
        _route_after_approval,
        {"authorize": "authorize_v1", "end": END},
    )
    builder.add_conditional_edges(
        "authorize_v1",
        _route_after_authorize,
        {"execute": "execute_fenced_v1", "end": END},
    )
    builder.add_conditional_edges(
        "execute_fenced_v1",
        _route_after_execute,
        {"recover": "recover_indeterminate_v1", "end": END},
    )
    builder.add_conditional_edges(
        "recover_indeterminate_v1",
        _route_after_recovery,
        {"execute": "execute_fenced_v1", "end": END},
    )
    return builder.compile(checkpointer=checkpointer)


class _ReferenceFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class _PostExecutionStateUnavailable(RuntimeError):
    """Keep the node retryable when post-dispatch durable state cannot be read."""

    def __init__(self) -> None:
        super().__init__(
            "post-execution durable state is unavailable; retry this workflow identity"
        )


def _recover_after_execution_failure(
    runtime: TruthLease,
    *,
    lease_id: str,
    fallback_code: str,
    fallback_cause: str,
) -> StaleSafeState:
    """Prefer durable attempt truth over an exception raised after preparation."""
    try:
        attempt = runtime.get_fenced_attempt_for_lease(lease_id)
    except Exception as error:
        raise _PostExecutionStateUnavailable from error
    return _state_from_durable_attempt(
        runtime,
        attempt,
        missing_code=fallback_code,
        missing_cause=fallback_cause,
    )


def _state_from_durable_attempt(
    runtime: TruthLease,
    attempt: FencedAttempt | None,
    *,
    missing_code: str,
    missing_cause: str,
) -> StaleSafeState:
    if attempt is None:
        return _reconcile(missing_code, missing_cause)
    if attempt.status is FencedAttemptStatus.COMMITTED:
        try:
            receipt = runtime.get_fenced_receipt(attempt.attempt_id)
        except Exception as error:
            raise _PostExecutionStateUnavailable from error
        if receipt is None:
            return {
                "attempt_id": attempt.attempt_id,
                **_reconcile("FENCED_RECEIPT_MISSING", "state"),
            }
        return _committed_state(receipt)
    if attempt.status is FencedAttemptStatus.NOT_COMMITTED:
        return {
            "attempt_id": attempt.attempt_id,
            "recovery_approved": False,
            "recovery_basis_digest": "",
            **_reconcile("TARGET_PRECONDITION_REJECTED", "target"),
        }
    if attempt.status in {
        FencedAttemptStatus.PREPARED,
        FencedAttemptStatus.INDETERMINATE,
    }:
        return {
            "attempt_id": attempt.attempt_id,
            "phase": "awaiting_recovery",
            "recovery_approved": False,
            "recovery_basis_digest": _attempt_recovery_basis(attempt),
            "decision_code": "TARGET_OUTCOME_INDETERMINATE",
            "reconciliation_cause": "target",
        }
    return {
        "attempt_id": attempt.attempt_id,
        **_reconcile("FENCED_ATTEMPT_NOT_COMMITTABLE", "target"),
    }


def _committed_state(receipt: FencedReceipt) -> StaleSafeState:
    return {
        "attempt_id": receipt.attempt_id,
        "receipt_id": receipt.receipt_id,
        "phase": "committed",
        "recovery_approved": False,
        "recovery_basis_digest": "",
        "decision_code": "FENCED_COMMIT_RECORDED",
        "reconciliation_cause": "",
    }


def _resolve_and_verify(
    state: StaleSafeState,
    *,
    resolve_effect: EffectResolver,
    resolve_precondition: PreconditionResolver,
) -> tuple[Digestable, TargetPrecondition]:
    try:
        effect = resolve_effect(state["effect_ref"])
    except Exception as error:
        raise _ReferenceFailure("EFFECT_REFERENCE_UNAVAILABLE") from error
    try:
        resolved_effect_digest = digest_effect(
            effect,
            operation=state["operation"],
            target=state["target"],
        )
    except (TypeError, ValueError) as error:
        raise _ReferenceFailure("EFFECT_REFERENCE_INVALID") from error
    if resolved_effect_digest != state["effect_digest"]:
        raise _ReferenceFailure("EFFECT_REFERENCE_CHANGED")
    try:
        precondition = resolve_precondition(state["precondition_ref"])
    except Exception as error:
        raise _ReferenceFailure("PRECONDITION_REFERENCE_UNAVAILABLE") from error
    if not isinstance(precondition, TargetPrecondition):
        raise _ReferenceFailure("PRECONDITION_REFERENCE_INVALID")
    if _precondition_descriptor_digest(precondition) != state["precondition_digest"]:
        raise _ReferenceFailure("PRECONDITION_REFERENCE_CHANGED")
    return effect, precondition


def _precondition_descriptor_digest(precondition: TargetPrecondition) -> str:
    return digest_json(
        {
            "adapter_id": precondition.adapter_id,
            "mechanism": precondition.mechanism,
            "expected_token": precondition.expected_token,
            "protected_observations": sorted(precondition.protected_observations),
            "replay_assurance": precondition.replay_assurance.value,
        },
        domain="langgraph:precondition-descriptor:v1",
    )


def _attempt_recovery_basis(attempt: FencedAttempt) -> str:
    return digest_json(
        {
            "attempt_id": attempt.attempt_id,
            "status": attempt.status.value,
            "result_digest": attempt.result_digest,
        },
        domain="langgraph:recovery-basis:v1",
    )


def _state_failure(
    state: StaleSafeState,
    *,
    expected_phases: set[str],
) -> StaleSafeState | None:
    try:
        if state.get("schema_version") != LANGGRAPH_STATE_VERSION:
            raise ValueError
        if state.get("phase") not in expected_phases:
            raise ValueError
        if state.get("phase") != "awaiting_approval" and state.get("approved") is not True:
            raise ValueError
        if type(state.get("recovery_approved")) is not bool:
            raise ValueError
        recovery_basis = state.get("recovery_basis_digest")
        if not isinstance(recovery_basis, str):
            raise ValueError
        if state.get("phase") == "awaiting_recovery" and not _DIGEST_PATTERN.fullmatch(
            recovery_basis
        ):
            raise ValueError
        if state.get("recovery_approved") is True and not _DIGEST_PATTERN.fullmatch(recovery_basis):
            raise ValueError
        if not isinstance(state.get("artifact_revision"), int) or state["artifact_revision"] <= 0:
            raise ValueError
        for key in (
            "artifact_id",
            "effect_ref",
            "precondition_ref",
            "target",
            "operation",
        ):
            value = state.get(key)
            if not isinstance(value, str) or not value.strip():
                raise ValueError
        for key in ("idempotency_key", "effect_digest", "precondition_digest"):
            value = state.get(key)
            if not isinstance(value, str) or not _DIGEST_PATTERN.fullmatch(value):
                raise ValueError
    except (KeyError, TypeError, ValueError):
        return _reconcile("CHECKPOINT_STATE_INVALID", "state")
    return None


def _reconcile(code: str, cause: str) -> StaleSafeState:
    return {
        "phase": "reconcile",
        "recovery_approved": False,
        "recovery_basis_digest": "",
        "decision_code": code,
        "reconciliation_cause": cause,
    }


def _route_after_approval(state: StaleSafeState) -> Literal["authorize", "end"]:
    return "authorize" if state.get("phase") == "approved" else "end"


def _route_after_authorize(state: StaleSafeState) -> Literal["execute", "end"]:
    return "execute" if state.get("phase") == "ready_to_execute" else "end"


def _route_after_execute(state: StaleSafeState) -> Literal["recover", "end"]:
    return "recover" if state.get("phase") == "awaiting_recovery" else "end"


def _route_after_recovery(state: StaleSafeState) -> Literal["execute", "end"]:
    return "execute" if state.get("phase") == "ready_to_execute" else "end"


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_digest(name: str, value: str) -> None:
    if not isinstance(value, str) or not _DIGEST_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be an algorithm-prefixed digest")


__all__ = [
    "LANGGRAPH_STATE_VERSION",
    "StaleSafeGraph",
    "StaleSafeState",
    "build_stale_safe_graph",
    "make_resume_state",
    "workflow_idempotency_key",
]
