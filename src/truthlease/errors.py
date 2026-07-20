"""Typed failures returned by the deterministic control plane."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class TruthLeaseError(Exception):
    """Base error with a stable machine-readable code."""

    code = "TRUTHLEASE_ERROR"

    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = dict(details or {})

    def as_problem(self) -> dict[str, Any]:
        return {
            "type": f"https://github.com/aantenore/truthlease/problems/{self.code.lower()}",
            "title": self.message,
            "code": self.code,
            "details": self.details,
        }


class NotFoundError(TruthLeaseError):
    code = "NOT_FOUND"


class ConflictError(TruthLeaseError):
    code = "CONFLICT"


class InvalidDependencyError(TruthLeaseError):
    code = "INVALID_DEPENDENCY"


class CycleDetectedError(TruthLeaseError):
    code = "CYCLE_DETECTED"


class ArtifactNotFreshError(TruthLeaseError):
    code = "ARTIFACT_NOT_FRESH"


class IncompleteDependencyError(TruthLeaseError):
    code = "DEPENDENCY_COMPLETENESS_INSUFFICIENT"


class LeaseExpiredError(TruthLeaseError):
    code = "LEASE_EXPIRED"


class StaleLeaseError(TruthLeaseError):
    code = "STALE_LEASE"


class LeaseConsumedError(TruthLeaseError):
    code = "LEASE_CONSUMED"


class EffectMismatchError(TruthLeaseError):
    code = "EFFECT_MISMATCH"


class AssuranceInsufficientError(TruthLeaseError):
    code = "ASSURANCE_INSUFFICIENT"


class GraphLimitError(TruthLeaseError):
    code = "GRAPH_LIMIT_EXCEEDED"


class CoherenceFaultError(TruthLeaseError):
    code = "COHERENCE_FAULT"
