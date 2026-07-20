"""Immutable public domain models."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

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
