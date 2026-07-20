"""Immutable public domain models."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

_DIGEST_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*:[A-Za-z0-9._~+/=-]+$")


class ObservationStatus(StrEnum):
    CURRENT = "current"
    DISPUTED = "disputed"
    UNKNOWN = "unknown"
    REVOKED = "revoked"


class ArtifactStatus(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    DISPUTED = "disputed"
    UNKNOWN = "unknown"
    REVOKED = "revoked"


class LeaseStatus(StrEnum):
    ACTIVE = "active"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"
    REVOKED = "revoked"


class DependencyKind(StrEnum):
    OBSERVATION = "observation"
    ARTIFACT = "artifact"


class DependencyCompleteness(StrEnum):
    UNKNOWN = "unknown"
    DECLARED = "declared"
    OBSERVED = "observed"

    @property
    def rank(self) -> int:
        return {
            DependencyCompleteness.UNKNOWN: 0,
            DependencyCompleteness.DECLARED: 1,
            DependencyCompleteness.OBSERVED: 2,
        }[self]


class AssuranceLevel(StrEnum):
    DECLARED = "declared"
    OBSERVED = "observed"
    REVALIDATED = "revalidated"
    FENCED = "fenced"

    @property
    def rank(self) -> int:
        return {
            AssuranceLevel.DECLARED: 0,
            AssuranceLevel.OBSERVED: 1,
            AssuranceLevel.REVALIDATED: 2,
            AssuranceLevel.FENCED: 3,
        }[self]


class ReplayAssurance(StrEnum):
    """Target-side protection against replaying one logical effect."""

    DEDUPLICATED = "deduplicated"
    ONE_SHOT = "one_shot"
    UNKNOWN = "unknown"


class FenceCoverage(StrEnum):
    """How much of a lease observation closure the target fence protects."""

    FULL = "full"
    PARTIAL = "partial"


class FencedOutcome(StrEnum):
    """Externally observed outcome for one prepared target attempt."""

    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    INDETERMINATE = "indeterminate"


class FencedAttemptStatus(StrEnum):
    """Durable lifecycle of one fenced target attempt."""

    PREPARED = "prepared"
    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    INDETERMINATE = "indeterminate"
    DISPUTED = "disputed"


class ReconcileAction(StrEnum):
    BLOCK = "block"
    NOTIFY = "notify"
    RECOMPUTE = "recompute"
    REPLAN = "replan"
    HUMAN = "human"


@dataclass(frozen=True, slots=True)
class Observation:
    key: str
    version: str
    digest: str
    generation: int
    observed_at: datetime
    status: ObservationStatus = ObservationStatus.CURRENT
    valid_until: datetime | None = None
    source: str = "manual"


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    artifact_id: str
    revision: int

    def __post_init__(self) -> None:
        if not self.artifact_id.strip():
            raise ValueError("artifact_id must be a non-empty string")
        if self.revision <= 0:
            raise ValueError("artifact revision must be positive")


@dataclass(frozen=True, slots=True)
class ArtifactDependency:
    kind: DependencyKind
    dependency_id: str
    expected_digest: str
    expected_version: str | None = None
    expected_generation: int | None = None
    expected_revision: int | None = None
    required: bool = True

    def __post_init__(self) -> None:
        if not self.dependency_id.strip():
            raise ValueError("dependency_id must be a non-empty string")
        if not _DIGEST_PATTERN.fullmatch(self.expected_digest):
            raise ValueError("expected_digest must be an algorithm-prefixed digest")
        if self.kind is DependencyKind.OBSERVATION:
            if (
                self.expected_version is None
                or not self.expected_version.strip()
                or self.expected_generation is None
                or self.expected_generation <= 0
                or self.expected_revision is not None
            ):
                raise ValueError(
                    "observation dependencies require expected_version and expected_generation"
                )
        elif (
            self.expected_revision is None
            or self.expected_revision <= 0
            or self.expected_version is not None
            or self.expected_generation is not None
        ):
            raise ValueError("artifact dependencies require only expected_revision")

    @classmethod
    def observation(cls, observation: Observation, *, required: bool = True) -> ArtifactDependency:
        return cls(
            kind=DependencyKind.OBSERVATION,
            dependency_id=observation.key,
            expected_version=observation.version,
            expected_generation=observation.generation,
            expected_digest=observation.digest,
            required=required,
        )

    @classmethod
    def artifact(cls, artifact: Artifact, *, required: bool = True) -> ArtifactDependency:
        return cls(
            kind=DependencyKind.ARTIFACT,
            dependency_id=artifact.ref.artifact_id,
            expected_revision=artifact.ref.revision,
            expected_digest=artifact.payload_digest,
            required=required,
        )


@dataclass(frozen=True, slots=True)
class Artifact:
    ref: ArtifactRef
    kind: str
    payload_digest: str
    dependencies: tuple[ArtifactDependency, ...]
    completeness: DependencyCompleteness
    status: ArtifactStatus
    reconcile_action: ReconcileAction
    created_at: datetime
    payload_ref: str | None = None
    invalidated_at: datetime | None = None
    invalidation_reason: str | None = None


@dataclass(frozen=True, slots=True)
class DependencySnapshot:
    key: str
    version: str
    generation: int
    digest: str


@dataclass(frozen=True, slots=True)
class Lease:
    lease_id: str
    artifact: ArtifactRef
    artifact_digest: str
    effect_digest: str
    target: str
    operation: str
    required_assurance: AssuranceLevel
    dependency_completeness: DependencyCompleteness
    closure_digest: str
    snapshot: tuple[DependencySnapshot, ...]
    issued_at: datetime
    expires_at: datetime
    boot_epoch: str
    status: LeaseStatus = LeaseStatus.ACTIVE
    consumed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AssuranceEvidence:
    """A lease-bound pre-gate validation statement from an admitted adapter."""

    level: AssuranceLevel
    authority: str
    evidence_digest: str
    observed_at: datetime
    lease_id: str
    closure_digest: str
    effect_digest: str
    target: str
    operation: str

    def __post_init__(self) -> None:
        if self.level is not AssuranceLevel.REVALIDATED:
            raise ValueError("alpha assurance evidence must have level revalidated")
        for name in (
            "authority",
            "evidence_digest",
            "lease_id",
            "closure_digest",
            "effect_digest",
            "target",
            "operation",
        ):
            value = getattr(self, name)
            if not value or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        for name in ("evidence_digest", "closure_digest", "effect_digest"):
            if not _DIGEST_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be an algorithm-prefixed digest")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("evidence observed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class TargetPrecondition:
    """Opaque target token plus the exact observations it protects.

    ``expected_token`` is deliberately carried only at the API boundary. The
    persistence layer stores its digest, never the raw token.
    """

    adapter_id: str
    mechanism: str
    expected_token: str
    protected_observations: tuple[str, ...]
    replay_assurance: ReplayAssurance

    def __post_init__(self) -> None:
        if isinstance(self.protected_observations, str):
            raise ValueError("protected observations must be a sequence of strings")
        object.__setattr__(self, "protected_observations", tuple(self.protected_observations))
        for name in ("adapter_id", "mechanism", "expected_token"):
            value = getattr(self, name)
            if not value or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.replay_assurance, ReplayAssurance):
            raise ValueError("replay_assurance must be a ReplayAssurance value")
        if any(
            not isinstance(item, str) or not item.strip() for item in self.protected_observations
        ):
            raise ValueError("protected observations must be non-empty strings")
        if len(set(self.protected_observations)) != len(self.protected_observations):
            raise ValueError("protected observations must not contain duplicates")


@dataclass(frozen=True, slots=True)
class FencedCommitRequest:
    """Prepared request for an allowlisted target adapter.

    The request is safe to retry only because ``replay_assurance`` is required
    to be ``deduplicated`` for fenced assurance. It is never persisted in full.
    """

    attempt_id: str
    lease_id: str
    gate_receipt_id: str
    adapter_id: str
    effect: Any
    target: str
    operation: str
    closure_digest: str
    effect_digest: str
    precondition: TargetPrecondition
    precondition_digest: str
    scope_digest: str
    request_digest: str
    coverage: FenceCoverage
    prepared_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "attempt_id",
            "lease_id",
            "gate_receipt_id",
            "adapter_id",
            "target",
            "operation",
        ):
            value = getattr(self, name)
            if not value or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        for name in (
            "closure_digest",
            "effect_digest",
            "precondition_digest",
            "scope_digest",
            "request_digest",
        ):
            if not _DIGEST_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be an algorithm-prefixed digest")
        if not isinstance(self.precondition, TargetPrecondition):
            raise ValueError("precondition must be a TargetPrecondition")
        if self.precondition.adapter_id != self.adapter_id:
            raise ValueError("precondition adapter_id must match request adapter_id")
        if not isinstance(self.coverage, FenceCoverage):
            raise ValueError("coverage must be a FenceCoverage value")
        _require_aware("prepared_at", self.prepared_at)
        _require_aware("expires_at", self.expires_at)
        if self.expires_at <= self.prepared_at:
            raise ValueError("request expires_at must be after prepared_at")


@dataclass(frozen=True, slots=True)
class FencedResult:
    """Digest-only target outcome submitted for durable recording."""

    attempt_id: str
    lease_id: str
    adapter_id: str
    outcome: FencedOutcome
    request_digest: str
    closure_digest: str
    effect_digest: str
    target: str
    operation: str
    precondition_digest: str
    scope_digest: str
    replay_assurance: ReplayAssurance
    coverage: FenceCoverage
    target_result_digest: str
    target_receipt_ref: str | None = None
    committed_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("attempt_id", "lease_id", "adapter_id", "target", "operation"):
            value = getattr(self, name)
            if not value or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        for name in (
            "request_digest",
            "closure_digest",
            "effect_digest",
            "precondition_digest",
            "scope_digest",
            "target_result_digest",
        ):
            if not _DIGEST_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be an algorithm-prefixed digest")
        if not isinstance(self.outcome, FencedOutcome):
            raise ValueError("outcome must be a FencedOutcome value")
        if not isinstance(self.replay_assurance, ReplayAssurance):
            raise ValueError("replay_assurance must be a ReplayAssurance value")
        if not isinstance(self.coverage, FenceCoverage):
            raise ValueError("coverage must be a FenceCoverage value")
        if self.outcome is FencedOutcome.COMMITTED:
            if self.target_receipt_ref is None or not self.target_receipt_ref.strip():
                raise ValueError("committed results require target_receipt_ref")
            if self.committed_at is None:
                raise ValueError("committed results require committed_at")
            _require_aware("committed_at", self.committed_at)
        elif self.target_receipt_ref is not None or self.committed_at is not None:
            raise ValueError(
                "not_committed and indeterminate results cannot claim a target receipt"
            )


@dataclass(frozen=True, slots=True)
class FencedAttempt:
    """Persisted digest-only state for one prepared target attempt."""

    attempt_id: str
    lease_id: str
    gate_receipt_id: str
    adapter_id: str
    request_digest: str
    precondition_digest: str
    scope_digest: str
    replay_assurance: ReplayAssurance
    coverage: FenceCoverage
    status: FencedAttemptStatus
    prepared_at: datetime
    completed_at: datetime | None = None
    result_digest: str | None = None

    def __post_init__(self) -> None:
        for name in ("attempt_id", "lease_id", "gate_receipt_id", "adapter_id"):
            value = getattr(self, name)
            if not value or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        for name in ("request_digest", "precondition_digest", "scope_digest"):
            if not _DIGEST_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be an algorithm-prefixed digest")
        if not isinstance(self.replay_assurance, ReplayAssurance):
            raise ValueError("replay_assurance must be a ReplayAssurance value")
        if not isinstance(self.coverage, FenceCoverage):
            raise ValueError("coverage must be a FenceCoverage value")
        if not isinstance(self.status, FencedAttemptStatus):
            raise ValueError("status must be a FencedAttemptStatus value")
        _require_aware("prepared_at", self.prepared_at)
        if self.completed_at is not None:
            _require_aware("completed_at", self.completed_at)
        if self.result_digest is not None and not _DIGEST_PATTERN.fullmatch(self.result_digest):
            raise ValueError("result_digest must be an algorithm-prefixed digest")


@dataclass(frozen=True, slots=True)
class FencedReceipt:
    """Core-derived proof of a full, deduplicated target commit."""

    receipt_id: str
    attempt_id: str
    lease_id: str
    gate_receipt_id: str
    assurance: AssuranceLevel
    adapter_id: str
    request_digest: str
    closure_digest: str
    effect_digest: str
    target: str
    operation: str
    precondition_digest: str
    scope_digest: str
    replay_assurance: ReplayAssurance
    coverage: FenceCoverage
    target_result_digest: str
    target_receipt_ref: str
    committed_at: datetime
    recorded_at: datetime
    guarantee: str

    def __post_init__(self) -> None:
        if not isinstance(self.assurance, AssuranceLevel):
            raise ValueError("assurance must be an AssuranceLevel value")
        if not isinstance(self.replay_assurance, ReplayAssurance):
            raise ValueError("replay_assurance must be a ReplayAssurance value")
        if not isinstance(self.coverage, FenceCoverage):
            raise ValueError("coverage must be a FenceCoverage value")
        if self.assurance is not AssuranceLevel.FENCED:
            raise ValueError("fenced receipt assurance is core-derived and must be fenced")
        if self.replay_assurance is not ReplayAssurance.DEDUPLICATED:
            raise ValueError("fenced receipt requires deduplicated replay assurance")
        if self.coverage is not FenceCoverage.FULL:
            raise ValueError("fenced receipt requires full closure coverage")
        for name in (
            "receipt_id",
            "attempt_id",
            "lease_id",
            "gate_receipt_id",
            "adapter_id",
            "target",
            "operation",
            "target_receipt_ref",
            "guarantee",
        ):
            value = getattr(self, name)
            if not value or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        for name in (
            "request_digest",
            "closure_digest",
            "effect_digest",
            "precondition_digest",
            "scope_digest",
            "target_result_digest",
        ):
            if not _DIGEST_PATTERN.fullmatch(getattr(self, name)):
                raise ValueError(f"{name} must be an algorithm-prefixed digest")
        _require_aware("committed_at", self.committed_at)
        _require_aware("recorded_at", self.recorded_at)


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    code: str
    lease_id: str
    assurance: AssuranceLevel
    guarantee: str
    receipt_id: str | None = None
    changed_dependencies: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class InvalidationResult:
    """Result of an observation commit.

    ``complete=False`` means the runtime failed closed globally. In that case
    per-artifact and per-lease ID collections are intentionally unavailable;
    only aggregate counts and the diagnostic are authoritative.
    """

    observation_key: str
    previous_version: str | None
    current_version: str
    previous_generation: int | None
    current_generation: int
    directly_impacted: tuple[ArtifactRef, ...]
    transitively_impacted: tuple[ArtifactRef, ...]
    invalidated_leases: tuple[str, ...]
    duplicate: bool = False
    complete: bool = True
    fail_closed: bool = False
    diagnostic: str | None = None
    invalidated_lease_count: int = 0

    @property
    def all_impacted(self) -> tuple[ArtifactRef, ...]:
        if not self.complete:
            raise ValueError("impact set is unavailable after global fail-closed fallback")
        return self.directly_impacted + self.transitively_impacted


@dataclass(frozen=True, slots=True)
class ReconciliationItem:
    artifact: ArtifactRef
    action: ReconcileAction
    reason: str


@dataclass(frozen=True, slots=True)
class ReconciliationPlan:
    cause: str
    items: tuple[ReconciliationItem, ...]
    reusable: tuple[ArtifactRef, ...] = field(default_factory=tuple)
