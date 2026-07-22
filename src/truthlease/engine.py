"""Deterministic semantic-coherence application service."""

from __future__ import annotations

import re
from collections import deque
from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, cast

from truthlease.canonical import Digestable, digest_json
from truthlease.clock import Clock, IdGenerator, SystemClock, UUIDGenerator, utc_text
from truthlease.errors import (
    ArtifactNotFreshError,
    CoherenceFaultError,
    ConflictError,
    FencedResultError,
    GraphLimitError,
    IncompleteDependencyError,
    InvalidDependencyError,
    NotFoundError,
    TargetFenceError,
)
from truthlease.models import (
    Artifact,
    ArtifactDependency,
    ArtifactRef,
    ArtifactStatus,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    DependencyKind,
    DependencySnapshot,
    FenceCoverage,
    FencedAttempt,
    FencedAttemptStatus,
    FencedCommitRequest,
    FencedOutcome,
    FencedReceipt,
    FencedResult,
    GateDecision,
    InvalidationResult,
    Lease,
    LeaseStatus,
    Observation,
    ObservationStatus,
    ReconcileAction,
    ReconciliationItem,
    ReconciliationPlan,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import SQLiteStore, SQLiteUnitOfWork

OBSERVED_GUARANTEE = (
    "No superseding observation committed to this TruthLease store before the gate "
    "transaction completed. A remote source can still change afterward."
)


class _Unset:
    pass


_UNSET = _Unset()
_DIGEST_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*:[A-Za-z0-9._~+/=-]+$")


class _FrozenDict(dict[str, Any]):
    """JSON-serializable immutable mapping used for outbound effect snapshots."""

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("fenced commit effects are immutable snapshots")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __ior__ = _immutable  # type: ignore[assignment]
    clear = _immutable
    pop = _immutable
    popitem = _immutable  # type: ignore[assignment]
    setdefault = _immutable
    update = _immutable

    def __deepcopy__(self, _memo: dict[int, Any]) -> _FrozenDict:
        return self


class TruthLease:
    """Coordinate observations, immutable artifacts, invalidation, and validity leases."""

    def __init__(
        self,
        store: SQLiteStore,
        *,
        clock: Clock | None = None,
        ids: IdGenerator | None = None,
        boot_epoch: str = "persistent-local-v1",
        max_graph_depth: int = 64,
        max_impact_nodes: int = 10_000,
        trusted_evidence_authorities: Collection[str] = (),
        trusted_target_adapters: Collection[str] = (),
        max_evidence_age: timedelta = timedelta(seconds=5),
    ) -> None:
        if not boot_epoch:
            raise ValueError("boot_epoch must not be empty")
        if max_graph_depth <= 0 or max_impact_nodes <= 0:
            raise ValueError("graph limits must be positive")
        if max_evidence_age <= timedelta(0):
            raise ValueError("max_evidence_age must be positive")
        if any(not authority.strip() for authority in trusted_evidence_authorities):
            raise ValueError("trusted evidence authorities must be non-empty strings")
        if any(not adapter.strip() for adapter in trusted_target_adapters):
            raise ValueError("trusted target adapters must be non-empty strings")
        self.store = store
        self.clock = clock or SystemClock()
        self.ids = ids or UUIDGenerator()
        self.boot_epoch = boot_epoch
        self.max_graph_depth = max_graph_depth
        self.max_impact_nodes = max_impact_nodes
        self.trusted_evidence_authorities = frozenset(trusted_evidence_authorities)
        self.trusted_target_adapters = frozenset(trusted_target_adapters)
        self.max_evidence_age = max_evidence_age
        self.store.initialize()

    def observe(
        self,
        *,
        key: str,
        version: str,
        value: Digestable | _Unset = _UNSET,
        digest: str | None = None,
        source: str = "manual",
        event_id: str | None = None,
        valid_for: timedelta | None = None,
        expected_generation: int | None = None,
    ) -> InvalidationResult:
        """Commit one authoritative observation and invalidate its minimal closure.

        Updating an existing key requires its last locally observed generation.
        This compare-and-swap prevents a delayed authoritative read from
        overwriting a newer one when source version tokens are opaque.
        """
        _require_text("key", key)
        _require_text("version", version)
        _require_text("source", source)
        if expected_generation is not None and expected_generation < 0:
            raise ValueError("expected_generation must be non-negative")
        if valid_for is not None and valid_for <= timedelta(0):
            raise ValueError("valid_for must be positive")
        if (value is _UNSET) == (digest is None):
            raise ValueError("provide exactly one of value or digest")
        if digest is not None:
            _require_digest("digest", digest)
            content_digest = digest
        else:
            content_digest = digest_json(cast(Digestable, value), domain=f"observation:{key}")
        event_id = event_id or self.ids.new("evt")

        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            valid_until = now + valid_for if valid_for is not None else None
            event_data: dict[str, Any] = {
                "key": key,
                "version": version,
                "digest": content_digest,
                "valid_until": utc_text(valid_until) if valid_until else None,
            }
            event_identity_data: dict[str, Any] = {
                "key": key,
                "version": version,
                "digest": content_digest,
                "valid_for_seconds": valid_for.total_seconds() if valid_for else None,
            }
            event_digest = digest_json(event_identity_data, domain="event:observation")
            recorded = tx.record_event(
                source=source,
                event_id=event_id,
                event_type="dev.truthlease.observation.recorded.v1",
                subject=f"observation/{key}",
                payload_digest=event_digest,
                occurred_at=utc_text(now),
                data=event_data,
            )
            previous = tx.get_observation(key)
            if not recorded:
                current = previous
                if current is None:
                    raise RuntimeError("event exists without its observation projection")
                return InvalidationResult(
                    observation_key=key,
                    previous_version=current.version,
                    current_version=current.version,
                    previous_generation=current.generation,
                    current_generation=current.generation,
                    directly_impacted=(),
                    transitively_impacted=(),
                    invalidated_leases=(),
                    duplicate=True,
                    invalidated_lease_count=0,
                )

            if previous is not None and expected_generation is None:
                raise ConflictError(
                    "updating an observation requires expected_generation",
                    details={"key": key, "current_generation": previous.generation},
                )
            actual_generation = previous.generation if previous else 0
            if expected_generation is not None and expected_generation != actual_generation:
                raise ConflictError(
                    "observation generation changed; perform a new authoritative read",
                    details={
                        "key": key,
                        "expected_generation": expected_generation,
                        "current_generation": actual_generation,
                    },
                )

            status = ObservationStatus.CURRENT
            if previous and previous.version == version and previous.digest != content_digest:
                status = ObservationStatus.DISPUTED
                content_digest = previous.digest

            observation = Observation(
                key=key,
                version=version,
                digest=content_digest,
                generation=actual_generation + 1,
                observed_at=now,
                status=status,
                valid_until=valid_until,
                source=source,
            )
            if (
                previous
                and previous.version == observation.version
                and previous.digest == observation.digest
                and previous.status is ObservationStatus.CURRENT
                and observation.status is ObservationStatus.CURRENT
                and previous.valid_until == observation.valid_until
            ):
                return InvalidationResult(
                    observation_key=key,
                    previous_version=previous.version,
                    current_version=observation.version,
                    previous_generation=previous.generation,
                    current_generation=previous.generation,
                    directly_impacted=(),
                    transitively_impacted=(),
                    invalidated_leases=(),
                    invalidated_lease_count=0,
                )

            tx.put_observation(observation)

            candidates = tx.observation_dependants(
                key,
                expected_generation=previous.generation if previous else None,
                only_fresh=True,
                limit=self.max_impact_nodes + 1,
            )
            try:
                if len(candidates) > self.max_impact_nodes:
                    raise GraphLimitError("invalidation exceeds configured node limit")
                transitive = self._descendants(
                    tx, candidates, excluded=set(candidates), only_fresh=True
                )
                direct = candidates
                all_impacted = direct + transitive
                states = [
                    (
                        ref,
                        self._effective_artifact_status(tx, tx.require_artifact(ref), now=now),
                    )
                    for ref in all_impacted
                ]
            except GraphLimitError as error:
                reason = f"{error.code}: {error.message}; source={key}"
                invalidated_count = tx.fail_closed(reason=reason, at=utc_text(now))
                fault_data: dict[str, Any] = {
                    "observation_key": key,
                    "reason": reason,
                    "invalidated_lease_count": invalidated_count,
                }
                tx.record_event(
                    source="urn:truthlease:runtime",
                    event_id=self.ids.new("evt"),
                    event_type="dev.truthlease.runtime.failed-closed.v1",
                    subject="runtime/coherence",
                    payload_digest=digest_json(fault_data, domain="event:coherence-fault"),
                    occurred_at=utc_text(now),
                    data=fault_data,
                )
                return InvalidationResult(
                    observation_key=key,
                    previous_version=previous.version if previous else None,
                    current_version=observation.version,
                    previous_generation=previous.generation if previous else None,
                    current_generation=observation.generation,
                    directly_impacted=(),
                    transitively_impacted=(),
                    invalidated_leases=(),
                    complete=False,
                    fail_closed=True,
                    diagnostic=error.code,
                    invalidated_lease_count=invalidated_count,
                )

            reason = (
                f"dependency state recomputed after observation {key} "
                f"generation {observation.generation}"
            )
            tx.set_artifact_states(
                states,
                at=utc_text(now),
                reason=reason,
            )
            lease_ids = tx.active_leases_for(all_impacted)
            tx.invalidate_leases(lease_ids)
            return InvalidationResult(
                observation_key=key,
                previous_version=previous.version if previous else None,
                current_version=observation.version,
                previous_generation=previous.generation if previous else None,
                current_generation=observation.generation,
                directly_impacted=tuple(direct),
                transitively_impacted=tuple(transitive),
                invalidated_leases=tuple(sorted(lease_ids)),
                invalidated_lease_count=len(lease_ids),
            )

    def publish_artifact(
        self,
        *,
        artifact_id: str,
        kind: str,
        payload: Digestable | _Unset = _UNSET,
        payload_digest: str | None = None,
        payload_ref: str | None = None,
        dependencies: Sequence[ArtifactDependency] = (),
        completeness: DependencyCompleteness = DependencyCompleteness.DECLARED,
        reconcile_action: ReconcileAction = ReconcileAction.REPLAN,
    ) -> Artifact:
        """Publish a new immutable revision; freshness is derived, never client supplied."""
        _require_text("artifact_id", artifact_id)
        _require_text("kind", kind)
        if (payload is _UNSET) == (payload_digest is None):
            raise ValueError("provide exactly one of payload or payload_digest")
        if payload_digest is not None:
            _require_digest("payload_digest", payload_digest)
            digest = payload_digest
        else:
            digest = digest_json(cast(Digestable, payload), domain=f"artifact:{kind}")
        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            self._raise_if_faulted(tx)
            revision = tx.next_artifact_revision(artifact_id)
            effective_completeness = self._effective_completeness(tx, dependencies, completeness)
            status = self._initial_artifact_status(
                tx, dependencies, effective_completeness, now=now
            )
            artifact = Artifact(
                ref=ArtifactRef(artifact_id, revision),
                kind=kind,
                payload_digest=digest,
                payload_ref=payload_ref,
                dependencies=tuple(dependencies),
                completeness=effective_completeness,
                status=status,
                reconcile_action=reconcile_action,
                created_at=now,
            )
            tx.insert_artifact(artifact)
            data: dict[str, Any] = {
                "artifact_id": artifact_id,
                "revision": revision,
                "kind": kind,
                "payload_digest": digest,
                "status": status.value,
                "completeness": effective_completeness.value,
                "dependency_count": len(dependencies),
            }
            tx.record_event(
                source="urn:truthlease:runtime",
                event_id=self.ids.new("evt"),
                event_type="dev.truthlease.artifact.published.v1",
                subject=f"artifact/{artifact_id}/revision/{revision}",
                payload_digest=digest_json(data, domain="event:artifact"),
                occurred_at=utc_text(now),
                data=data,
            )
            return artifact

    def acquire_lease(
        self,
        *,
        artifact: ArtifactRef,
        effect: Digestable,
        target: str,
        operation: str,
        required_assurance: AssuranceLevel = AssuranceLevel.OBSERVED,
        ttl: timedelta = timedelta(seconds=30),
        target_precondition: TargetPrecondition | None = None,
        idempotency_key: str | None = None,
    ) -> Lease:
        """Bind a short-lived, single-use lease to an exact artifact and effect.

        A digest-only ``idempotency_key`` makes acquisition restart-safe. An
        identical retry returns the originally issued lease even after it was
        consumed; reusing the key for different intent is rejected.
        """
        _require_text("target", target)
        _require_text("operation", operation)
        if ttl <= timedelta(0):
            raise ValueError("lease ttl must be positive")
        if idempotency_key is not None:
            _require_digest("idempotency_key", idempotency_key)
        effect_digest = digest_effect(effect, operation=operation, target=target)
        intent_digest = _lease_request_intent_digest(
            artifact=artifact,
            effect_digest=effect_digest,
            target=target,
            operation=operation,
            required_assurance=required_assurance,
            ttl=ttl,
            boot_epoch=self.boot_epoch,
            target_precondition=target_precondition,
        )

        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            if idempotency_key is not None:
                existing_request = tx.get_lease_request(idempotency_key)
                if existing_request is not None:
                    existing_intent_digest, existing_lease_id = existing_request
                    if existing_intent_digest != intent_digest:
                        raise ConflictError(
                            "idempotency key is already bound to a different lease intent",
                            details={
                                "idempotency_key": idempotency_key,
                                "existing_intent_digest": existing_intent_digest,
                                "requested_intent_digest": intent_digest,
                            },
                        )
                    existing_lease = tx.get_lease(existing_lease_id)
                    if existing_lease is None:
                        raise RuntimeError("lease request references a missing lease")
                    return existing_lease
            self._raise_if_faulted(tx)
            current = tx.require_artifact(artifact)
            current = replace(
                current,
                status=self._effective_artifact_status(tx, current, now=now),
            )
            if current.completeness is DependencyCompleteness.UNKNOWN:
                raise IncompleteDependencyError(
                    "unknown dependency completeness cannot authorize an effect",
                    details={"artifact_id": artifact.artifact_id},
                )
            if current.status is not ArtifactStatus.FRESH:
                raise ArtifactNotFreshError(
                    "artifact is not fresh or a required observation expired",
                    details={
                        "artifact_id": artifact.artifact_id,
                        "revision": artifact.revision,
                        "status": current.status.value,
                    },
                )
            snapshot, closure_completeness = self._dependency_closure(tx, current, now=now)
            if closure_completeness is DependencyCompleteness.UNKNOWN:
                raise IncompleteDependencyError(
                    "unknown transitive dependency completeness cannot authorize an effect",
                    details={"artifact_id": artifact.artifact_id},
                )
            if (
                closure_completeness is DependencyCompleteness.DECLARED
                and required_assurance.rank > AssuranceLevel.DECLARED.rank
            ):
                raise IncompleteDependencyError(
                    "declared dependency completeness cannot satisfy requested assurance",
                    details={
                        "artifact_id": artifact.artifact_id,
                        "required_assurance": required_assurance.value,
                    },
                )
            closure_data: dict[str, Any] = {
                "artifact": {
                    "id": artifact.artifact_id,
                    "revision": artifact.revision,
                    "digest": current.payload_digest,
                },
                "effect_digest": effect_digest,
                "target": target,
                "operation": operation,
                "required_assurance": required_assurance.value,
                "completeness": closure_completeness.value,
                "dependencies": [
                    {
                        "key": item.key,
                        "version": item.version,
                        "generation": item.generation,
                        "digest": item.digest,
                    }
                    for item in snapshot
                ],
            }
            lease = Lease(
                lease_id=self.ids.new("lease"),
                artifact=artifact,
                artifact_digest=current.payload_digest,
                effect_digest=effect_digest,
                target=target,
                operation=operation,
                required_assurance=required_assurance,
                dependency_completeness=closure_completeness,
                closure_digest=digest_json(closure_data, domain="lease:closure"),
                snapshot=tuple(snapshot),
                issued_at=now,
                expires_at=now + ttl,
                boot_epoch=self.boot_epoch,
            )
            fence_binding: tuple[str, str, FenceCoverage] | None = None
            if target_precondition is not None:
                coverage = _fence_coverage(lease.snapshot, target_precondition)
                scope_digest = _scope_digest(lease.snapshot, target_precondition)
                precondition_digest = _precondition_digest(
                    target_precondition,
                    target=target,
                    operation=operation,
                    scope_digest=scope_digest,
                )
                fence_binding = (precondition_digest, scope_digest, coverage)
            if required_assurance is AssuranceLevel.FENCED:
                if target_precondition is None or fence_binding is None:
                    raise TargetFenceError(
                        "fenced assurance requires a target precondition",
                        details={"required_assurance": required_assurance.value},
                    )
                if closure_completeness is not DependencyCompleteness.OBSERVED:
                    raise TargetFenceError(
                        "fenced assurance requires observed dependency completeness"
                    )
                if target_precondition.adapter_id not in self.trusted_target_adapters:
                    raise TargetFenceError(
                        "target adapter is not allowlisted",
                        details={"adapter_id": target_precondition.adapter_id},
                    )
                if target_precondition.replay_assurance is not ReplayAssurance.DEDUPLICATED:
                    raise TargetFenceError("fenced assurance requires deduplicated target replay")
                if fence_binding[2] is not FenceCoverage.FULL:
                    raise TargetFenceError(
                        "fenced assurance requires exact full observation-closure coverage",
                        details={
                            "protected_observations": sorted(
                                target_precondition.protected_observations
                            ),
                            "lease_observations": sorted(item.key for item in lease.snapshot),
                        },
                    )
            tx.insert_lease(lease)
            if target_precondition is not None and fence_binding is not None:
                precondition_digest, scope_digest, coverage = fence_binding
                tx.insert_lease_fence_binding(
                    lease_id=lease.lease_id,
                    adapter_id=target_precondition.adapter_id,
                    mechanism=target_precondition.mechanism,
                    precondition_digest=precondition_digest,
                    scope_digest=scope_digest,
                    coverage=coverage,
                    replay_assurance=target_precondition.replay_assurance,
                    protected_observations=target_precondition.protected_observations,
                    created_at=utc_text(now),
                )
            if idempotency_key is not None:
                tx.insert_lease_request(
                    idempotency_key=idempotency_key,
                    intent_digest=intent_digest,
                    lease_id=lease.lease_id,
                    created_at=utc_text(now),
                )
            event_data: dict[str, Any] = {
                "lease_id": lease.lease_id,
                "artifact_id": artifact.artifact_id,
                "artifact_revision": artifact.revision,
                "closure_digest": lease.closure_digest,
                "required_assurance": required_assurance.value,
                "expires_at": utc_text(lease.expires_at),
            }
            tx.record_event(
                source="urn:truthlease:runtime",
                event_id=self.ids.new("evt"),
                event_type="dev.truthlease.lease.issued.v1",
                subject=f"lease/{lease.lease_id}",
                payload_digest=digest_json(event_data, domain="event:lease"),
                occurred_at=utc_text(now),
                data=event_data,
            )
            return lease

    def validate_lease(self, lease_id: str) -> GateDecision:
        """Check local currentness without consuming or claiming dispatch assurance."""
        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            lease = tx.get_lease(lease_id)
            if lease is None:
                raise NotFoundError("lease does not exist", details={"lease_id": lease_id})
            failure = self._lease_failure(tx, lease, now=now)
            if failure:
                code, changed, terminal = failure
                if terminal is not None:
                    tx.set_lease_status(lease_id, terminal)
                return GateDecision(
                    allowed=False,
                    code=code,
                    lease_id=lease_id,
                    assurance=_assurance_for_completeness(lease.dependency_completeness),
                    guarantee=_denial_guarantee(code),
                    changed_dependencies=tuple(changed),
                )
            local_assurance = _assurance_for_completeness(lease.dependency_completeness)
            if lease.required_assurance.rank > local_assurance.rank:
                return GateDecision(
                    allowed=False,
                    code="ADDITIONAL_ASSURANCE_REQUIRED",
                    lease_id=lease_id,
                    assurance=local_assurance,
                    guarantee=_denial_guarantee("ADDITIONAL_ASSURANCE_REQUIRED"),
                )
            return GateDecision(
                allowed=True,
                code="LEASE_VALID",
                lease_id=lease_id,
                assurance=_assurance_for_completeness(lease.dependency_completeness),
                guarantee=_guarantee_for(
                    _assurance_for_completeness(lease.dependency_completeness)
                ),
            )

    def gate_effect(
        self,
        *,
        lease_id: str,
        effect: Digestable,
        achieved_assurance: AssuranceLevel = AssuranceLevel.OBSERVED,
        evidence: AssuranceEvidence | None = None,
    ) -> GateDecision:
        """Atomically validate and consume a lease for one exact effect.

        Evidence above ``observed`` must come from a trusted adapter selected by
        the embedding application. TruthLease records that statement; it cannot
        authenticate an arbitrary remote source on its own.
        """
        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            lease = tx.get_lease(lease_id)
            if lease is None:
                raise NotFoundError("lease does not exist", details={"lease_id": lease_id})
            effect_digest = digest_effect(effect, operation=lease.operation, target=lease.target)
            local_assurance = _assurance_for_completeness(lease.dependency_completeness)
            if effect_digest != lease.effect_digest:
                return GateDecision(
                    allowed=False,
                    code="EFFECT_MISMATCH",
                    lease_id=lease_id,
                    assurance=local_assurance,
                    guarantee=_denial_guarantee("EFFECT_MISMATCH"),
                )
            failure = self._lease_failure(tx, lease, now=now)
            if failure:
                code, changed, terminal = failure
                if terminal is not None:
                    tx.set_lease_status(lease_id, terminal)
                return GateDecision(
                    allowed=False,
                    code=code,
                    lease_id=lease_id,
                    assurance=local_assurance,
                    guarantee=_denial_guarantee(code),
                    changed_dependencies=tuple(changed),
                )
            completeness_assurance = _assurance_for_completeness(lease.dependency_completeness)
            if (
                lease.dependency_completeness is DependencyCompleteness.DECLARED
                and lease.required_assurance.rank > AssuranceLevel.DECLARED.rank
            ):
                return GateDecision(
                    allowed=False,
                    code="DEPENDENCY_COMPLETENESS_INSUFFICIENT",
                    lease_id=lease_id,
                    assurance=completeness_assurance,
                    guarantee=_denial_guarantee("DEPENDENCY_COMPLETENESS_INSUFFICIENT"),
                )
            if achieved_assurance is AssuranceLevel.FENCED:
                return GateDecision(
                    allowed=False,
                    code="ASSURANCE_UNSUPPORTED",
                    lease_id=lease_id,
                    assurance=local_assurance,
                    guarantee=(
                        "No effect authorized. The alpha gate does not execute target effects, "
                        "so it cannot issue "
                        "fenced assurance. Use a target adapter that records the post-commit "
                        "result."
                    ),
                )
            verified_assurance = (
                AssuranceLevel.DECLARED
                if achieved_assurance is AssuranceLevel.DECLARED
                else local_assurance
            )
            if (
                achieved_assurance is AssuranceLevel.REVALIDATED
                and local_assurance is AssuranceLevel.OBSERVED
                and evidence is None
            ):
                return GateDecision(
                    allowed=False,
                    code="ASSURANCE_EVIDENCE_REQUIRED",
                    lease_id=lease_id,
                    assurance=local_assurance,
                    guarantee=_denial_guarantee("ASSURANCE_EVIDENCE_REQUIRED"),
                )
            if (
                achieved_assurance is AssuranceLevel.REVALIDATED
                and local_assurance is AssuranceLevel.OBSERVED
                and (
                    evidence is None
                    or evidence.level is not AssuranceLevel.REVALIDATED
                    or evidence.authority not in self.trusted_evidence_authorities
                    or evidence.lease_id != lease.lease_id
                    or evidence.closure_digest != lease.closure_digest
                    or evidence.effect_digest != lease.effect_digest
                    or evidence.target != lease.target
                    or evidence.operation != lease.operation
                    or evidence.observed_at < lease.issued_at
                    or evidence.observed_at > now
                    or now - evidence.observed_at > self.max_evidence_age
                )
            ):
                return GateDecision(
                    allowed=False,
                    code="ASSURANCE_EVIDENCE_INVALID",
                    lease_id=lease_id,
                    assurance=local_assurance,
                    guarantee=_denial_guarantee("ASSURANCE_EVIDENCE_INVALID"),
                )
            if (
                achieved_assurance is AssuranceLevel.REVALIDATED
                and local_assurance is AssuranceLevel.OBSERVED
            ):
                verified_assurance = AssuranceLevel.REVALIDATED
            if verified_assurance.rank < lease.required_assurance.rank:
                return GateDecision(
                    allowed=False,
                    code="ASSURANCE_INSUFFICIENT",
                    lease_id=lease_id,
                    assurance=verified_assurance,
                    guarantee=_denial_guarantee("ASSURANCE_INSUFFICIENT"),
                )
            effective_assurance = verified_assurance
            receipt_id = self.ids.new("receipt")
            guarantee = _guarantee_for(effective_assurance)
            tx.set_lease_status(lease_id, LeaseStatus.CONSUMED, consumed_at=utc_text(now))
            tx.insert_receipt(
                receipt_id=receipt_id,
                lease_id=lease_id,
                assurance=effective_assurance,
                effect_digest=effect_digest,
                created_at=utc_text(now),
                guarantee=guarantee,
                evidence=(evidence if effective_assurance is AssuranceLevel.REVALIDATED else None),
            )
            event_data: dict[str, Any] = {
                "lease_id": lease_id,
                "receipt_id": receipt_id,
                "effect_digest": effect_digest,
                "assurance": effective_assurance.value,
            }
            tx.record_event(
                source="urn:truthlease:runtime",
                event_id=self.ids.new("evt"),
                event_type="dev.truthlease.lease.consumed.v1",
                subject=f"lease/{lease_id}",
                payload_digest=digest_json(event_data, domain="event:gate"),
                occurred_at=utc_text(now),
                data=event_data,
            )
            return GateDecision(
                allowed=True,
                code="EFFECT_DISPATCHABLE",
                lease_id=lease_id,
                assurance=effective_assurance,
                guarantee=guarantee,
                receipt_id=receipt_id,
            )

    def prepare_fenced_commit(
        self,
        lease_id: str,
        effect: Digestable,
        target_precondition: TargetPrecondition,
        evidence: AssuranceEvidence | None = None,
    ) -> FencedCommitRequest:
        """Consume a lease and durably prepare one target-side commit attempt.

        This method performs local SQLite work only. The returned request is
        executed by an adapter after the transaction commits. Repeating the
        call with identical inputs returns the same attempt; different inputs
        for the consumed lease are rejected.
        """
        _require_text("lease_id", lease_id)
        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            lease = tx.get_lease(lease_id)
            if lease is None:
                raise NotFoundError("lease does not exist", details={"lease_id": lease_id})
            effect_snapshot = _freeze_digestable(effect)
            effect_digest = digest_effect(
                effect_snapshot, operation=lease.operation, target=lease.target
            )
            binding = tx.get_lease_fence_binding(lease_id)
            if binding is None:
                raise TargetFenceError(
                    "lease has no target fence binding",
                    details={"lease_id": lease_id},
                )
            if target_precondition.adapter_id not in self.trusted_target_adapters:
                raise TargetFenceError(
                    "target adapter is not allowlisted",
                    details={"adapter_id": target_precondition.adapter_id},
                )
            coverage = _fence_coverage(lease.snapshot, target_precondition)
            scope_digest = _scope_digest(lease.snapshot, target_precondition)
            precondition_digest = _precondition_digest(
                target_precondition,
                target=lease.target,
                operation=lease.operation,
                scope_digest=scope_digest,
            )
            self._require_matching_fence_binding(
                binding=binding,
                target_precondition=target_precondition,
                precondition_digest=precondition_digest,
                scope_digest=scope_digest,
                coverage=coverage,
            )
            if lease.dependency_completeness is not DependencyCompleteness.OBSERVED:
                raise TargetFenceError(
                    "target commit preparation requires observed dependency completeness"
                )

            existing = tx.get_fenced_attempt_for_lease(lease_id)
            if existing is not None:
                request_digest = _fenced_request_digest(
                    attempt_id=existing.attempt_id,
                    lease=lease,
                    gate_receipt_id=existing.gate_receipt_id,
                    adapter_id=target_precondition.adapter_id,
                    precondition_digest=precondition_digest,
                    scope_digest=scope_digest,
                    replay_assurance=target_precondition.replay_assurance,
                    coverage=coverage,
                    prepared_at=existing.prepared_at,
                )
                if (
                    effect_digest != lease.effect_digest
                    or existing.adapter_id != target_precondition.adapter_id
                    or existing.precondition_digest != precondition_digest
                    or existing.scope_digest != scope_digest
                    or existing.replay_assurance is not target_precondition.replay_assurance
                    or existing.coverage is not coverage
                    or existing.request_digest != request_digest
                ):
                    raise ConflictError(
                        "consumed lease already has a different fenced attempt",
                        details={
                            "lease_id": lease_id,
                            "attempt_id": existing.attempt_id,
                        },
                    )
                return FencedCommitRequest(
                    attempt_id=existing.attempt_id,
                    lease_id=lease.lease_id,
                    gate_receipt_id=existing.gate_receipt_id,
                    adapter_id=existing.adapter_id,
                    effect=effect_snapshot,
                    target=lease.target,
                    operation=lease.operation,
                    closure_digest=lease.closure_digest,
                    effect_digest=lease.effect_digest,
                    precondition=target_precondition,
                    precondition_digest=precondition_digest,
                    scope_digest=scope_digest,
                    request_digest=request_digest,
                    coverage=coverage,
                    prepared_at=existing.prepared_at,
                    expires_at=lease.expires_at,
                )

            if effect_digest != lease.effect_digest:
                raise ConflictError(
                    "effect does not match the lease",
                    details={"lease_id": lease_id},
                )
            failure = self._lease_failure(tx, lease, now=now)
            if failure:
                code, changed, _terminal = failure
                raise TargetFenceError(
                    "lease cannot prepare a target commit",
                    details={"lease_id": lease_id, "code": code, "changed": changed},
                )
            if evidence is None:
                raise TargetFenceError(
                    "fresh authoritative revalidation evidence is required",
                    details={"lease_id": lease_id},
                )
            if not self._valid_revalidation_evidence(evidence, lease=lease, now=now):
                raise TargetFenceError(
                    "authoritative revalidation evidence is invalid",
                    details={"lease_id": lease_id, "authority": evidence.authority},
                )

            attempt_id = self.ids.new("attempt")
            gate_receipt_id = self.ids.new("receipt")
            request_digest = _fenced_request_digest(
                attempt_id=attempt_id,
                lease=lease,
                gate_receipt_id=gate_receipt_id,
                adapter_id=target_precondition.adapter_id,
                precondition_digest=precondition_digest,
                scope_digest=scope_digest,
                replay_assurance=target_precondition.replay_assurance,
                coverage=coverage,
                prepared_at=now,
            )
            tx.set_lease_status(lease_id, LeaseStatus.CONSUMED, consumed_at=utc_text(now))
            tx.insert_receipt(
                receipt_id=gate_receipt_id,
                lease_id=lease_id,
                assurance=AssuranceLevel.REVALIDATED,
                effect_digest=effect_digest,
                created_at=utc_text(now),
                guarantee=_guarantee_for(AssuranceLevel.REVALIDATED),
                evidence=evidence,
            )
            attempt = FencedAttempt(
                attempt_id=attempt_id,
                lease_id=lease_id,
                gate_receipt_id=gate_receipt_id,
                adapter_id=target_precondition.adapter_id,
                request_digest=request_digest,
                precondition_digest=precondition_digest,
                scope_digest=scope_digest,
                replay_assurance=target_precondition.replay_assurance,
                coverage=coverage,
                status=FencedAttemptStatus.PREPARED,
                prepared_at=now,
            )
            tx.insert_fenced_attempt(attempt)
            event_data: dict[str, Any] = {
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "gate_receipt_id": gate_receipt_id,
                "adapter_id": target_precondition.adapter_id,
                "request_digest": request_digest,
                "precondition_digest": precondition_digest,
                "scope_digest": scope_digest,
                "coverage": coverage.value,
                "replay_assurance": target_precondition.replay_assurance.value,
            }
            tx.record_event(
                source="urn:truthlease:runtime",
                event_id=self.ids.new("evt"),
                event_type="dev.truthlease.fenced-attempt.prepared.v1",
                subject=f"fenced-attempt/{attempt_id}",
                payload_digest=digest_json(event_data, domain="event:fenced-attempt"),
                occurred_at=utc_text(now),
                data=event_data,
            )
            return FencedCommitRequest(
                attempt_id=attempt_id,
                lease_id=lease_id,
                gate_receipt_id=gate_receipt_id,
                adapter_id=target_precondition.adapter_id,
                effect=effect_snapshot,
                target=lease.target,
                operation=lease.operation,
                closure_digest=lease.closure_digest,
                effect_digest=effect_digest,
                precondition=target_precondition,
                precondition_digest=precondition_digest,
                scope_digest=scope_digest,
                request_digest=request_digest,
                coverage=coverage,
                prepared_at=now,
                expires_at=lease.expires_at,
            )

    def record_fenced_result(self, result: FencedResult) -> FencedReceipt | None:
        """Record one adapter result and derive fenced assurance when warranted."""
        if not isinstance(result, FencedResult):
            raise TypeError("result must be a FencedResult")
        if (
            not isinstance(result.outcome, FencedOutcome)
            or not isinstance(result.replay_assurance, ReplayAssurance)
            or not isinstance(result.coverage, FenceCoverage)
        ):
            raise FencedResultError("target result contains invalid enum values")
        result_digest = _fenced_result_digest(result)
        conflict: dict[str, Any] | None = None
        derived_receipt: FencedReceipt | None = None
        with self.store.transaction(write=True) as tx:
            now = self.clock.now()
            attempt = tx.get_fenced_attempt(result.attempt_id)
            if attempt is None:
                raise NotFoundError(
                    "fenced attempt does not exist",
                    details={"attempt_id": result.attempt_id},
                )
            lease = tx.get_lease(attempt.lease_id)
            if lease is None:
                raise RuntimeError("fenced attempt references a missing lease")
            binding = tx.get_lease_fence_binding(lease.lease_id)
            if binding is None:
                raise FencedResultError(
                    "fenced attempt has no durable target binding",
                    details={"attempt_id": result.attempt_id},
                )
            self._require_matching_fenced_result(
                result=result,
                attempt=attempt,
                lease=lease,
                binding=binding,
            )

            if attempt.status is FencedAttemptStatus.DISPUTED:
                raise ConflictError(
                    "fenced attempt is disputed",
                    details={"attempt_id": result.attempt_id},
                )
            existing_result_id = tx.get_fenced_result_id(result.attempt_id, result_digest)
            if existing_result_id is not None:
                return tx.get_fenced_receipt(result.attempt_id)

            result_id = self.ids.new("result")
            tx.insert_fenced_result(
                result_id=result_id,
                result_digest=result_digest,
                result=result,
                recorded_at=utc_text(now),
            )
            if attempt.status in {
                FencedAttemptStatus.COMMITTED,
                FencedAttemptStatus.NOT_COMMITTED,
            }:
                if result.outcome is FencedOutcome.INDETERMINATE:
                    derived_receipt = tx.get_fenced_receipt(attempt.attempt_id)
                    event_data = {
                        "attempt_id": attempt.attempt_id,
                        "lease_id": lease.lease_id,
                        "result_digest": result_digest,
                        "outcome": result.outcome.value,
                        "preserved_terminal_status": attempt.status.value,
                        "fenced_receipt_id": (
                            derived_receipt.receipt_id if derived_receipt is not None else None
                        ),
                    }
                    tx.record_event(
                        source="urn:truthlease:runtime",
                        event_id=self.ids.new("evt"),
                        event_type="dev.truthlease.fenced-attempt.late-indeterminate.v1",
                        subject=f"fenced-attempt/{attempt.attempt_id}",
                        payload_digest=digest_json(
                            event_data, domain="event:fenced-late-indeterminate"
                        ),
                        occurred_at=utc_text(now),
                        data=event_data,
                    )
                else:
                    reason = (
                        "conflicting terminal target result; "
                        f"attempt={attempt.attempt_id}; prior={attempt.result_digest}; "
                        f"new={result_digest}"
                    )
                    tx.set_fenced_attempt_result(
                        attempt.attempt_id,
                        status=FencedAttemptStatus.DISPUTED,
                        completed_at=utc_text(now),
                        result_digest=result_digest,
                    )
                    invalidated_count = tx.fail_closed(reason=reason, at=utc_text(now))
                    conflict = {
                        "attempt_id": attempt.attempt_id,
                        "prior_result_digest": attempt.result_digest,
                        "new_result_digest": result_digest,
                        "invalidated_lease_count": invalidated_count,
                    }
                    event_data = {**conflict, "reason": reason}
                    tx.record_event(
                        source="urn:truthlease:runtime",
                        event_id=self.ids.new("evt"),
                        event_type="dev.truthlease.fenced-attempt.disputed.v1",
                        subject=f"fenced-attempt/{attempt.attempt_id}",
                        payload_digest=digest_json(event_data, domain="event:fenced-dispute"),
                        occurred_at=utc_text(now),
                        data=event_data,
                    )
            else:
                status = FencedAttemptStatus(result.outcome.value)
                completed_at = (
                    None if result.outcome is FencedOutcome.INDETERMINATE else utc_text(now)
                )
                tx.set_fenced_attempt_result(
                    attempt.attempt_id,
                    status=status,
                    completed_at=completed_at,
                    result_digest=result_digest,
                )
                if (
                    result.outcome is FencedOutcome.COMMITTED
                    and result.coverage is FenceCoverage.FULL
                    and result.replay_assurance is ReplayAssurance.DEDUPLICATED
                ):
                    if result.target_receipt_ref is None or result.committed_at is None:
                        raise RuntimeError("validated committed result is incomplete")
                    derived_receipt = FencedReceipt(
                        receipt_id=self.ids.new("fenced_receipt"),
                        attempt_id=attempt.attempt_id,
                        lease_id=lease.lease_id,
                        gate_receipt_id=attempt.gate_receipt_id,
                        assurance=AssuranceLevel.FENCED,
                        adapter_id=attempt.adapter_id,
                        request_digest=attempt.request_digest,
                        closure_digest=lease.closure_digest,
                        effect_digest=lease.effect_digest,
                        target=lease.target,
                        operation=lease.operation,
                        precondition_digest=attempt.precondition_digest,
                        scope_digest=attempt.scope_digest,
                        replay_assurance=result.replay_assurance,
                        coverage=result.coverage,
                        target_result_digest=result.target_result_digest,
                        target_receipt_ref=result.target_receipt_ref,
                        committed_at=result.committed_at,
                        recorded_at=now,
                        guarantee=_fenced_guarantee(),
                    )
                    tx.insert_fenced_receipt(derived_receipt, result_id=result_id)
                event_data = {
                    "attempt_id": attempt.attempt_id,
                    "lease_id": lease.lease_id,
                    "result_digest": result_digest,
                    "outcome": result.outcome.value,
                    "fenced_receipt_id": (
                        derived_receipt.receipt_id if derived_receipt is not None else None
                    ),
                }
                tx.record_event(
                    source="urn:truthlease:runtime",
                    event_id=self.ids.new("evt"),
                    event_type="dev.truthlease.fenced-attempt.result-recorded.v1",
                    subject=f"fenced-attempt/{attempt.attempt_id}",
                    payload_digest=digest_json(event_data, domain="event:fenced-result"),
                    occurred_at=utc_text(now),
                    data=event_data,
                )
        if conflict is not None:
            raise ConflictError("conflicting terminal target result", details=conflict)
        return derived_receipt

    def get_lease(self, lease_id: str) -> Lease | None:
        """Return one persisted lease, including terminal state, without mutating it."""
        _require_text("lease_id", lease_id)
        with self.store.transaction(write=False) as tx:
            return tx.get_lease(lease_id)

    def get_fenced_attempt(self, attempt_id: str) -> FencedAttempt | None:
        with self.store.transaction(write=False) as tx:
            return tx.get_fenced_attempt(attempt_id)

    def get_fenced_attempt_for_lease(self, lease_id: str) -> FencedAttempt | None:
        """Return the durable target attempt, if any, for one lease."""
        _require_text("lease_id", lease_id)
        with self.store.transaction(write=False) as tx:
            return tx.get_fenced_attempt_for_lease(lease_id)

    def get_fenced_receipt(self, attempt_id: str) -> FencedReceipt | None:
        with self.store.transaction(write=False) as tx:
            attempt = tx.get_fenced_attempt(attempt_id)
            if attempt is not None and attempt.status is FencedAttemptStatus.DISPUTED:
                raise ConflictError(
                    "fenced attempt is disputed; its historical receipt is not valid",
                    details={"attempt_id": attempt_id},
                )
            return tx.get_fenced_receipt(attempt_id)

    def get_artifact(self, artifact_id: str, revision: int | None = None) -> Artifact:
        with self.store.transaction(write=False) as tx:
            artifact = tx.get_artifact(artifact_id, revision)
            if artifact is None:
                raise NotFoundError(
                    "artifact does not exist",
                    details={"artifact_id": artifact_id, "revision": revision},
                )
            now = self.clock.now()
            return replace(
                artifact,
                status=self._effective_artifact_status(tx, artifact, now=now),
            )

    def get_observation(self, key: str) -> Observation:
        with self.store.transaction(write=False) as tx:
            observation = tx.get_observation(key)
            if observation is None:
                raise NotFoundError("observation does not exist", details={"key": key})
            return observation

    def reconciliation_plan(self, cause: str) -> ReconciliationPlan:
        with self.store.transaction(write=False) as tx:
            now = self.clock.now()
            observation = tx.get_observation(cause)
            if observation is None:
                raise NotFoundError("observation does not exist", details={"key": cause})
            candidates = tx.observation_dependants(cause, limit=self.max_impact_nodes + 1)
            if len(candidates) > self.max_impact_nodes:
                raise GraphLimitError("reconciliation exceeds configured node limit")
            direct = [
                ref
                for ref in candidates
                if self._observation_dependency_broken(tx, ref, observation, now=now)
            ]
            impacted = direct + self._descendants(tx, direct, excluded=set(direct))
            items: list[ReconciliationItem] = []
            reusable: set[ArtifactRef] = set()
            for ref in impacted:
                artifact = tx.require_artifact(ref)
                artifact = replace(
                    artifact,
                    status=self._effective_artifact_status(tx, artifact, now=now),
                )
                if artifact.status not in {
                    ArtifactStatus.STALE,
                    ArtifactStatus.DISPUTED,
                    ArtifactStatus.UNKNOWN,
                }:
                    continue
                items.append(
                    ReconciliationItem(
                        artifact=ref,
                        action=artifact.reconcile_action,
                        reason=artifact.invalidation_reason or f"dependency {cause} changed",
                    )
                )
                for dependency in artifact.dependencies:
                    if dependency.kind is DependencyKind.ARTIFACT:
                        candidate = ArtifactRef(
                            dependency.dependency_id, dependency.expected_revision or 0
                        )
                        upstream = tx.require_artifact(candidate)
                        upstream = replace(
                            upstream,
                            status=self._effective_artifact_status(tx, upstream, now=now),
                        )
                        if upstream.status is ArtifactStatus.FRESH and candidate not in impacted:
                            reusable.add(candidate)
            return ReconciliationPlan(
                cause=cause,
                items=tuple(items),
                reusable=tuple(
                    sorted(reusable, key=lambda item: (item.artifact_id, item.revision))
                ),
            )

    def graph(self) -> dict[str, Any]:
        with self.store.transaction(write=False) as tx:
            now = self.clock.now()
            artifacts = [
                replace(item, status=self._effective_artifact_status(tx, item, now=now))
                for item in tx.list_artifacts()
            ]
            return {
                "artifacts": [
                    {
                        "id": item.ref.artifact_id,
                        "revision": item.ref.revision,
                        "kind": item.kind,
                        "status": item.status.value,
                        "completeness": item.completeness.value,
                        "dependencies": [
                            {
                                "kind": dependency.kind.value,
                                "id": dependency.dependency_id,
                                "version": dependency.expected_version,
                                "generation": dependency.expected_generation,
                                "revision": dependency.expected_revision,
                                "required": dependency.required,
                            }
                            for dependency in item.dependencies
                        ],
                    }
                    for item in artifacts
                ]
            }

    def events(self) -> list[dict[str, Any]]:
        with self.store.transaction(write=False) as tx:
            return tx.list_events()

    def _initial_artifact_status(
        self,
        tx: SQLiteUnitOfWork,
        dependencies: Sequence[ArtifactDependency],
        completeness: DependencyCompleteness,
        *,
        now: datetime,
    ) -> ArtifactStatus:
        return self._status_from_dependencies(
            tx,
            dependencies,
            completeness,
            now=now,
            memo={},
            visiting=set(),
            depth=0,
        )

    def _status_from_dependencies(
        self,
        tx: SQLiteUnitOfWork,
        dependencies: Sequence[ArtifactDependency],
        completeness: DependencyCompleteness,
        *,
        now: datetime,
        memo: dict[ArtifactRef, ArtifactStatus],
        visiting: set[ArtifactRef],
        depth: int,
    ) -> ArtifactStatus:
        result = (
            ArtifactStatus.UNKNOWN
            if completeness is DependencyCompleteness.UNKNOWN
            else ArtifactStatus.FRESH
        )
        seen: set[tuple[DependencyKind, str, int | str | None]] = set()
        for dependency in dependencies:
            identity = (
                dependency.kind,
                dependency.dependency_id,
                (
                    f"{dependency.expected_version}@{dependency.expected_generation}"
                    if dependency.kind is DependencyKind.OBSERVATION
                    else dependency.expected_revision
                ),
            )
            if identity in seen:
                raise InvalidDependencyError("duplicate dependency", details={"id": identity[1]})
            seen.add(identity)
            if dependency.kind is DependencyKind.OBSERVATION:
                observation = tx.get_observation(dependency.dependency_id)
                if observation is None:
                    raise InvalidDependencyError(
                        "observation dependency does not exist",
                        details={"key": dependency.dependency_id},
                    )
                if not dependency.required:
                    continue
                if observation.status is ObservationStatus.DISPUTED:
                    candidate = ArtifactStatus.DISPUTED
                elif observation.status is ObservationStatus.REVOKED:
                    candidate = ArtifactStatus.REVOKED
                elif observation.status is ObservationStatus.UNKNOWN:
                    candidate = ArtifactStatus.UNKNOWN
                elif (
                    observation.version != dependency.expected_version
                    or observation.generation != dependency.expected_generation
                    or observation.digest != dependency.expected_digest
                    or (observation.valid_until is not None and observation.valid_until <= now)
                ):
                    candidate = ArtifactStatus.STALE
                else:
                    candidate = ArtifactStatus.FRESH
                result = _join_artifact_status(result, candidate)
            else:
                ref = ArtifactRef(
                    dependency.dependency_id,
                    dependency.expected_revision or 0,
                )
                upstream = tx.require_artifact(ref)
                if upstream.payload_digest != dependency.expected_digest:
                    raise InvalidDependencyError(
                        "artifact dependency digest does not match its immutable revision",
                        details={"artifact_id": ref.artifact_id, "revision": ref.revision},
                    )
                if not dependency.required:
                    continue
                candidate = self._effective_artifact_status(
                    tx,
                    upstream,
                    now=now,
                    memo=memo,
                    visiting=visiting,
                    depth=depth + 1,
                )
                result = _join_artifact_status(result, candidate)
        return result

    def _effective_artifact_status(
        self,
        tx: SQLiteUnitOfWork,
        artifact: Artifact,
        *,
        now: datetime,
        memo: dict[ArtifactRef, ArtifactStatus] | None = None,
        visiting: set[ArtifactRef] | None = None,
        depth: int = 0,
    ) -> ArtifactStatus:
        memo = memo if memo is not None else {}
        visiting = visiting if visiting is not None else set()
        if artifact.status is ArtifactStatus.REVOKED:
            return ArtifactStatus.REVOKED
        if tx.get_coherence_fault() is not None:
            return ArtifactStatus.UNKNOWN
        if artifact.ref in memo:
            return memo[artifact.ref]
        if artifact.ref in visiting:
            raise InvalidDependencyError(
                "artifact dependency cycle detected",
                details={"artifact_id": artifact.ref.artifact_id},
            )
        if depth > self.max_graph_depth:
            raise GraphLimitError("dependency graph exceeds configured depth")
        if len(memo) + len(visiting) >= self.max_impact_nodes:
            raise GraphLimitError("dependency graph exceeds configured node limit")
        visiting.add(artifact.ref)
        try:
            status = self._status_from_dependencies(
                tx,
                artifact.dependencies,
                artifact.completeness,
                now=now,
                memo=memo,
                visiting=visiting,
                depth=depth,
            )
            memo[artifact.ref] = status
            return status
        finally:
            visiting.remove(artifact.ref)

    def _effective_completeness(
        self,
        tx: SQLiteUnitOfWork,
        dependencies: Sequence[ArtifactDependency],
        requested: DependencyCompleteness,
    ) -> DependencyCompleteness:
        result = requested
        for dependency in dependencies:
            if not dependency.required or dependency.kind is not DependencyKind.ARTIFACT:
                continue
            upstream = tx.require_artifact(
                ArtifactRef(dependency.dependency_id, dependency.expected_revision or 0)
            )
            if upstream.completeness.rank < result.rank:
                result = upstream.completeness
        return result

    def _dependency_closure(
        self, tx: SQLiteUnitOfWork, artifact: Artifact, *, now: datetime
    ) -> tuple[list[DependencySnapshot], DependencyCompleteness]:
        snapshots: dict[str, DependencySnapshot] = {}
        stack: list[tuple[Artifact, int]] = [(artifact, 0)]
        visited: set[ArtifactRef] = set()
        completeness = artifact.completeness
        while stack:
            current, depth = stack.pop()
            if depth > self.max_graph_depth:
                raise GraphLimitError("dependency graph exceeds configured depth")
            if current.ref in visited:
                continue
            visited.add(current.ref)
            for dependency in current.dependencies:
                if not dependency.required:
                    continue
                if dependency.kind is DependencyKind.OBSERVATION:
                    observation = tx.get_observation(dependency.dependency_id)
                    if observation is None or observation.status is not ObservationStatus.CURRENT:
                        raise ArtifactNotFreshError(
                            "required observation is unavailable",
                            details={"key": dependency.dependency_id},
                        )
                    if observation.valid_until is not None and observation.valid_until <= now:
                        raise ArtifactNotFreshError(
                            "required observation has expired",
                            details={"key": dependency.dependency_id},
                        )
                    if (
                        observation.version != dependency.expected_version
                        or observation.digest != dependency.expected_digest
                    ):
                        raise ArtifactNotFreshError(
                            "required observation changed",
                            details={"key": dependency.dependency_id},
                        )
                    candidate = DependencySnapshot(
                        observation.key,
                        observation.version,
                        observation.generation,
                        observation.digest,
                    )
                    existing = snapshots.get(candidate.key)
                    if existing is not None and existing != candidate:
                        raise ArtifactNotFreshError(
                            "artifact closure contains conflicting versions",
                            details={"key": candidate.key},
                        )
                    snapshots[candidate.key] = candidate
                else:
                    upstream = tx.require_artifact(
                        ArtifactRef(dependency.dependency_id, dependency.expected_revision or 0)
                    )
                    if upstream.completeness.rank < completeness.rank:
                        completeness = upstream.completeness
                    if (
                        self._effective_artifact_status(tx, upstream, now=now)
                        is not ArtifactStatus.FRESH
                        or upstream.payload_digest != dependency.expected_digest
                    ):
                        raise ArtifactNotFreshError(
                            "upstream artifact is not fresh",
                            details={
                                "artifact_id": upstream.ref.artifact_id,
                                "revision": upstream.ref.revision,
                            },
                        )
                    stack.append((upstream, depth + 1))
            if len(visited) + len(snapshots) > self.max_impact_nodes:
                raise GraphLimitError("dependency closure exceeds configured node limit")
        return [snapshots[key] for key in sorted(snapshots)], completeness

    def _lease_failure(
        self, tx: SQLiteUnitOfWork, lease: Lease, *, now: datetime
    ) -> tuple[str, list[str], LeaseStatus | None] | None:
        if lease.status is LeaseStatus.CONSUMED:
            return ("LEASE_CONSUMED", [], None)
        if lease.status is LeaseStatus.REVOKED:
            return ("STALE_LEASE", [], None)
        if tx.get_coherence_fault() is not None:
            return ("COHERENCE_FAULT", [], LeaseStatus.INVALIDATED)
        if lease.status is LeaseStatus.EXPIRED or lease.expires_at <= now:
            return ("LEASE_EXPIRED", [], LeaseStatus.EXPIRED)
        if lease.boot_epoch != self.boot_epoch:
            return ("BOOT_EPOCH_CHANGED", [], LeaseStatus.INVALIDATED)
        changed: list[str] = []
        for expected in lease.snapshot:
            current = tx.get_observation(expected.key)
            if (
                current is None
                or current.status is not ObservationStatus.CURRENT
                or current.version != expected.version
                or current.generation != expected.generation
                or current.digest != expected.digest
                or (current.valid_until is not None and current.valid_until <= now)
            ):
                changed.append(expected.key)
        if changed:
            return ("STALE_LEASE", sorted(changed), LeaseStatus.INVALIDATED)
        artifact = tx.require_artifact(lease.artifact)
        if (
            artifact.status is not ArtifactStatus.FRESH
            or artifact.payload_digest != lease.artifact_digest
        ):
            return (
                "STALE_LEASE",
                [f"artifact:{artifact.ref.artifact_id}"],
                LeaseStatus.INVALIDATED,
            )
        if lease.status is LeaseStatus.INVALIDATED:
            return ("STALE_LEASE", [], None)
        return None

    def _observation_dependency_broken(
        self,
        tx: SQLiteUnitOfWork,
        ref: ArtifactRef,
        observation: Observation,
        *,
        now: datetime,
    ) -> bool:
        artifact = tx.require_artifact(ref)
        return any(
            dependency.required
            and dependency.kind is DependencyKind.OBSERVATION
            and dependency.dependency_id == observation.key
            and (
                observation.status is not ObservationStatus.CURRENT
                or dependency.expected_version != observation.version
                or dependency.expected_generation != observation.generation
                or dependency.expected_digest != observation.digest
                or (observation.valid_until is not None and observation.valid_until <= now)
            )
            for dependency in artifact.dependencies
        )

    def _descendants(
        self,
        tx: SQLiteUnitOfWork,
        roots: Sequence[ArtifactRef],
        *,
        excluded: set[ArtifactRef],
        only_fresh: bool = False,
    ) -> list[ArtifactRef]:
        if len(excluded) > self.max_impact_nodes:
            raise GraphLimitError("invalidation exceeds configured node limit")
        queue = deque((root, 0) for root in roots)
        seen = set(excluded)
        descendants: list[ArtifactRef] = []
        while queue:
            current, depth = queue.popleft()
            if depth >= self.max_graph_depth:
                if any(
                    child not in seen
                    for child in tx.iter_artifact_dependants(current, only_fresh=only_fresh)
                ):
                    raise GraphLimitError("invalidation exceeds configured graph depth")
                continue
            for child in tx.iter_artifact_dependants(current, only_fresh=only_fresh):
                if child in seen:
                    continue
                if len(seen) >= self.max_impact_nodes:
                    raise GraphLimitError("invalidation exceeds configured node limit")
                seen.add(child)
                descendants.append(child)
                queue.append((child, depth + 1))
        return descendants

    def _valid_revalidation_evidence(
        self,
        evidence: AssuranceEvidence,
        *,
        lease: Lease,
        now: datetime,
    ) -> bool:
        return (
            evidence.level is AssuranceLevel.REVALIDATED
            and evidence.authority in self.trusted_evidence_authorities
            and evidence.lease_id == lease.lease_id
            and evidence.closure_digest == lease.closure_digest
            and evidence.effect_digest == lease.effect_digest
            and evidence.target == lease.target
            and evidence.operation == lease.operation
            and evidence.observed_at >= lease.issued_at
            and evidence.observed_at <= now
            and now - evidence.observed_at <= self.max_evidence_age
        )

    @staticmethod
    def _require_matching_fence_binding(
        *,
        binding: dict[str, Any],
        target_precondition: TargetPrecondition,
        precondition_digest: str,
        scope_digest: str,
        coverage: FenceCoverage,
    ) -> None:
        supplied_observations = tuple(sorted(target_precondition.protected_observations))
        expected_observations = tuple(sorted(binding["protected_observations"]))
        if (
            binding["adapter_id"] != target_precondition.adapter_id
            or binding["mechanism"] != target_precondition.mechanism
            or binding["precondition_digest"] != precondition_digest
            or binding["scope_digest"] != scope_digest
            or binding["coverage"] is not coverage
            or binding["replay_assurance"] is not target_precondition.replay_assurance
            or expected_observations != supplied_observations
        ):
            raise TargetFenceError(
                "target precondition does not match the lease binding",
                details={"lease_id": binding["lease_id"]},
            )

    @staticmethod
    def _require_matching_fenced_result(
        *,
        result: FencedResult,
        attempt: FencedAttempt,
        lease: Lease,
        binding: dict[str, Any],
    ) -> None:
        mismatches: list[str] = []
        expected = {
            "lease_id": lease.lease_id,
            "adapter_id": attempt.adapter_id,
            "request_digest": attempt.request_digest,
            "closure_digest": lease.closure_digest,
            "effect_digest": lease.effect_digest,
            "target": lease.target,
            "operation": lease.operation,
            "precondition_digest": attempt.precondition_digest,
            "scope_digest": attempt.scope_digest,
            "replay_assurance": attempt.replay_assurance,
            "coverage": attempt.coverage,
        }
        for name, value in expected.items():
            if getattr(result, name) != value:
                mismatches.append(name)
        if (
            binding["adapter_id"] != attempt.adapter_id
            or binding["precondition_digest"] != attempt.precondition_digest
            or binding["scope_digest"] != attempt.scope_digest
            or binding["replay_assurance"] is not attempt.replay_assurance
            or binding["coverage"] is not attempt.coverage
        ):
            mismatches.append("lease_fence_binding")
        if mismatches:
            raise FencedResultError(
                "target result does not match the prepared attempt",
                details={
                    "attempt_id": attempt.attempt_id,
                    "mismatches": sorted(set(mismatches)),
                },
            )

    @staticmethod
    def _raise_if_faulted(tx: SQLiteUnitOfWork) -> None:
        fault = tx.get_coherence_fault()
        if fault is not None:
            raise CoherenceFaultError(
                "runtime is fail-closed after an incomplete invalidation",
                details={"fault": fault},
            )


def _protected_snapshot(
    snapshot: Sequence[DependencySnapshot],
    target_precondition: TargetPrecondition,
) -> list[DependencySnapshot]:
    by_key = {item.key: item for item in snapshot}
    unknown = sorted(set(target_precondition.protected_observations) - set(by_key))
    if unknown:
        raise TargetFenceError(
            "target precondition names observations outside the lease closure",
            details={"unknown_observations": unknown},
        )
    return [by_key[key] for key in sorted(target_precondition.protected_observations)]


def _lease_request_intent_digest(
    *,
    artifact: ArtifactRef,
    effect_digest: str,
    target: str,
    operation: str,
    required_assurance: AssuranceLevel,
    ttl: timedelta,
    boot_epoch: str,
    target_precondition: TargetPrecondition | None,
) -> str:
    """Bind retry identity to caller intent without persisting opaque raw values."""
    precondition_descriptor_digest = None
    if target_precondition is not None:
        descriptor: dict[str, Any] = {
            "adapter_id": target_precondition.adapter_id,
            "mechanism": target_precondition.mechanism,
            "expected_token": target_precondition.expected_token,
            "protected_observations": sorted(target_precondition.protected_observations),
            "replay_assurance": target_precondition.replay_assurance.value,
        }
        precondition_descriptor_digest = digest_json(
            descriptor, domain="lease:precondition-descriptor:v1"
        )
    ttl_microseconds = ttl.days * 86_400_000_000 + ttl.seconds * 1_000_000 + ttl.microseconds
    intent: dict[str, Any] = {
        "artifact": {"id": artifact.artifact_id, "revision": artifact.revision},
        "effect_digest": effect_digest,
        "target": target,
        "operation": operation,
        "required_assurance": required_assurance.value,
        "ttl_microseconds": ttl_microseconds,
        "boot_epoch": boot_epoch,
        "precondition_descriptor_digest": precondition_descriptor_digest,
    }
    return digest_json(intent, domain="lease:request-intent:v1")


def digest_effect(effect: Digestable, *, operation: str, target: str) -> str:
    """Digest an effect with the single domain used by lease and adapter paths."""
    _require_text("operation", operation)
    _require_text("target", target)
    return digest_json(effect, domain=f"effect:{operation}:{target}")


def _freeze_digestable(value: Digestable) -> Digestable:
    """Take an immutable, JSON-compatible deep snapshot of an effect."""
    if isinstance(value, Mapping):
        frozen: dict[str, Digestable] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            frozen[key] = _freeze_digestable(item)
        return cast(Digestable, _FrozenDict(frozen))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return cast(Digestable, tuple(_freeze_digestable(item) for item in value))
    return value


def _fence_coverage(
    snapshot: Sequence[DependencySnapshot],
    target_precondition: TargetPrecondition,
) -> FenceCoverage:
    _protected_snapshot(snapshot, target_precondition)
    protected = set(target_precondition.protected_observations)
    lease_keys = {item.key for item in snapshot}
    return FenceCoverage.FULL if protected == lease_keys else FenceCoverage.PARTIAL


def _scope_digest(
    snapshot: Sequence[DependencySnapshot],
    target_precondition: TargetPrecondition,
) -> str:
    protected = _protected_snapshot(snapshot, target_precondition)
    scope_data: dict[str, Any] = {
        "observations": [
            {
                "key": item.key,
                "version": item.version,
                "generation": item.generation,
                "digest": item.digest,
            }
            for item in protected
        ]
    }
    return digest_json(scope_data, domain="target-fence:scope:v1")


def _precondition_digest(
    target_precondition: TargetPrecondition,
    *,
    target: str,
    operation: str,
    scope_digest: str,
) -> str:
    precondition_data: dict[str, Any] = {
        "adapter_id": target_precondition.adapter_id,
        "mechanism": target_precondition.mechanism,
        "expected_token": target_precondition.expected_token,
        "protected_observations": sorted(target_precondition.protected_observations),
        "replay_assurance": target_precondition.replay_assurance.value,
        "scope_digest": scope_digest,
        "target": target,
        "operation": operation,
    }
    return digest_json(precondition_data, domain="target-fence:precondition:v1")


def _fenced_request_digest(
    *,
    attempt_id: str,
    lease: Lease,
    gate_receipt_id: str,
    adapter_id: str,
    precondition_digest: str,
    scope_digest: str,
    replay_assurance: ReplayAssurance,
    coverage: FenceCoverage,
    prepared_at: datetime,
) -> str:
    request_data: dict[str, Any] = {
        "attempt_id": attempt_id,
        "lease_id": lease.lease_id,
        "gate_receipt_id": gate_receipt_id,
        "adapter_id": adapter_id,
        "closure_digest": lease.closure_digest,
        "effect_digest": lease.effect_digest,
        "target": lease.target,
        "operation": lease.operation,
        "precondition_digest": precondition_digest,
        "scope_digest": scope_digest,
        "replay_assurance": replay_assurance.value,
        "coverage": coverage.value,
        "prepared_at": utc_text(prepared_at),
        "expires_at": utc_text(lease.expires_at),
    }
    return digest_json(request_data, domain="target-fence:request:v1")


def _fenced_result_digest(result: FencedResult) -> str:
    result_data: dict[str, Any] = {
        "attempt_id": result.attempt_id,
        "lease_id": result.lease_id,
        "adapter_id": result.adapter_id,
        "outcome": result.outcome.value,
        "request_digest": result.request_digest,
        "closure_digest": result.closure_digest,
        "effect_digest": result.effect_digest,
        "target": result.target,
        "operation": result.operation,
        "precondition_digest": result.precondition_digest,
        "scope_digest": result.scope_digest,
        "replay_assurance": result.replay_assurance.value,
        "coverage": result.coverage.value,
        "target_result_digest": result.target_result_digest,
        "target_receipt_ref": result.target_receipt_ref,
        "committed_at": utc_text(result.committed_at) if result.committed_at else None,
    }
    return digest_json(result_data, domain="target-fence:result:v1")


def _fenced_guarantee() -> str:
    return (
        "An allowlisted target adapter reported a committed effect under the exact "
        "lease-bound precondition, with full observation-closure coverage and "
        "deduplicated replay. TruthLease recorded the target result after commit."
    )


def _guarantee_for(level: AssuranceLevel) -> str:
    if level is AssuranceLevel.FENCED:
        return (
            "Fenced assurance is reserved for a target-side commit result and is not emitted "
            "by the local alpha gate."
        )
    if level is AssuranceLevel.REVALIDATED:
        return (
            "A trusted adapter reported authoritative revalidation immediately before this gate. "
            "A change between validation and an unfenced target commit remains possible."
        )
    if level is AssuranceLevel.OBSERVED:
        return OBSERVED_GUARANTEE
    return (
        "Declared dependencies matched the local store at the gate. Dependency capture was not "
        "observed by TruthLease, so omitted dependencies remain possible."
    )


def _denial_guarantee(code: str) -> str:
    return (
        f"No effect is authorized by this decision ({code}). The assurance field reports only "
        "the dependency-capture or evidence level established before denial; it is neither "
        "authorization nor a freshness guarantee."
    )


def _join_artifact_status(left: ArtifactStatus, right: ArtifactStatus) -> ArtifactStatus:
    rank = {
        ArtifactStatus.FRESH: 0,
        ArtifactStatus.STALE: 1,
        ArtifactStatus.UNKNOWN: 2,
        ArtifactStatus.DISPUTED: 3,
        ArtifactStatus.REVOKED: 4,
    }
    return left if rank[left] >= rank[right] else right


def _assurance_for_completeness(
    completeness: DependencyCompleteness,
) -> AssuranceLevel:
    if completeness is DependencyCompleteness.OBSERVED:
        return AssuranceLevel.OBSERVED
    return AssuranceLevel.DECLARED


def _require_text(name: str, value: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_digest(name: str, value: str) -> None:
    if not _DIGEST_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be an algorithm-prefixed digest")
