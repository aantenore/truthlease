from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path
from threading import Barrier, Lock, Thread

import pytest
from conftest import ManualClock, SequentialIds

from truthlease import (
    Artifact,
    ArtifactDependency,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    LeaseStatus,
    ReplayAssurance,
    SQLiteStore,
    TargetPrecondition,
    TruthLease,
)
from truthlease.canonical import digest_json
from truthlease.errors import ConflictError


def _artifact(runtime: TruthLease) -> Artifact:
    runtime.observe(key="policy", version="v1", value={"enabled": True})
    return runtime.publish_artifact(
        artifact_id="plan",
        kind="plan",
        payload={"policy": "v1"},
        dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
        completeness=DependencyCompleteness.OBSERVED,
    )


def _key(name: str) -> str:
    return digest_json({"request": name}, domain="test:lease-idempotency-key")


def _precondition(token: str) -> TargetPrecondition:
    return TargetPrecondition(
        adapter_id="adapter:test",
        mechanism="compare-and-swap",
        expected_token=token,
        protected_observations=("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )


def test_idempotent_acquire_returns_original_consumed_lease_and_rejects_mismatch(
    db_path: Path, clock: ManualClock
) -> None:
    runtime = TruthLease(SQLiteStore(db_path), clock=clock, ids=SequentialIds())
    artifact = _artifact(runtime)
    effect = {"release": "raw-effect-value-3948"}

    first_without_key = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
    )
    second_without_key = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
    )
    assert first_without_key.lease_id != second_without_key.lease_id

    idempotency_key = _key("workflow-one")
    lease = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
        idempotency_key=idempotency_key,
    )
    active_retry = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
        idempotency_key=idempotency_key,
    )
    assert active_retry == lease

    assert runtime.gate_effect(lease_id=lease.lease_id, effect=effect).allowed is True
    observation = runtime.get_observation("policy")
    runtime.observe(
        key="policy",
        version="v2",
        value={"enabled": False},
        expected_generation=observation.generation,
    )

    consumed_retry = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="deployment/current",
        operation="promote",
        idempotency_key=idempotency_key,
    )
    assert consumed_retry.lease_id == lease.lease_id
    assert consumed_retry.status is LeaseStatus.CONSUMED
    assert runtime.get_lease(lease.lease_id) == consumed_retry

    with pytest.raises(ConflictError, match="different lease intent"):
        runtime.acquire_lease(
            artifact=artifact.ref,
            effect={"release": "different"},
            target="deployment/current",
            operation="promote",
            idempotency_key=idempotency_key,
        )

    connection = sqlite3.connect(db_path)
    try:
        request_rows = connection.execute(
            "SELECT idempotency_key, intent_digest, lease_id FROM lease_requests"
        ).fetchall()
        issued_events = int(
            connection.execute(
                "SELECT count(*) FROM event_log "
                "WHERE event_type = 'dev.truthlease.lease.issued.v1' AND subject = ?",
                (f"lease/{lease.lease_id}",),
            ).fetchone()[0]
        )
    finally:
        connection.close()
    assert len(request_rows) == 1
    assert request_rows[0][0] == idempotency_key
    assert request_rows[0][1].startswith("sha256:")
    assert request_rows[0][2] == lease.lease_id
    assert issued_events == 1
    assert b"raw-effect-value-3948" not in db_path.read_bytes()


def test_idempotency_key_and_precondition_descriptor_are_digest_bound(
    db_path: Path, clock: ManualClock
) -> None:
    runtime = TruthLease(
        SQLiteStore(db_path),
        clock=clock,
        ids=SequentialIds(),
        trusted_target_adapters={"adapter:test"},
    )
    artifact = _artifact(runtime)
    effect = {"write": "raw-fenced-effect-7132"}
    precondition = _precondition("raw-target-token-8241")
    idempotency_key = _key("fenced-workflow")

    with pytest.raises(ValueError, match="algorithm-prefixed digest"):
        runtime.acquire_lease(
            artifact=artifact.ref,
            effect=effect,
            target="target/current",
            operation="write",
            idempotency_key="raw-workflow-id",
        )

    lease = runtime.acquire_lease(
        artifact=artifact.ref,
        effect=effect,
        target="target/current",
        operation="write",
        required_assurance=AssuranceLevel.FENCED,
        target_precondition=precondition,
        idempotency_key=idempotency_key,
    )
    assert runtime.get_fenced_attempt_for_lease(lease.lease_id) is None

    with pytest.raises(ConflictError, match="different lease intent"):
        runtime.acquire_lease(
            artifact=artifact.ref,
            effect=effect,
            target="target/current",
            operation="write",
            required_assurance=AssuranceLevel.FENCED,
            target_precondition=_precondition("different-target-token"),
            idempotency_key=idempotency_key,
        )
    with pytest.raises(ConflictError, match="different lease intent"):
        runtime.acquire_lease(
            artifact=artifact.ref,
            effect=effect,
            target="target/current",
            operation="write",
            required_assurance=AssuranceLevel.FENCED,
            ttl=timedelta(seconds=30, microseconds=1),
            target_precondition=precondition,
            idempotency_key=idempotency_key,
        )

    different_boot = TruthLease(
        SQLiteStore(db_path),
        clock=clock,
        boot_epoch="different-process-epoch",
        trusted_target_adapters={"adapter:test"},
    )
    with pytest.raises(ConflictError, match="different lease intent"):
        different_boot.acquire_lease(
            artifact=artifact.ref,
            effect=effect,
            target="target/current",
            operation="write",
            required_assurance=AssuranceLevel.FENCED,
            target_precondition=precondition,
            idempotency_key=idempotency_key,
        )

    evidence = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="evidence:test",
        evidence_digest="sha256:fresh",
        observed_at=clock.now(),
        lease_id=lease.lease_id,
        closure_digest=lease.closure_digest,
        effect_digest=lease.effect_digest,
        target=lease.target,
        operation=lease.operation,
    )
    runtime.trusted_evidence_authorities = frozenset({"evidence:test"})
    request = runtime.prepare_fenced_commit(lease.lease_id, effect, precondition, evidence)
    assert runtime.get_fenced_attempt_for_lease(lease.lease_id) == runtime.get_fenced_attempt(
        request.attempt_id
    )

    database_bytes = db_path.read_bytes()
    assert b"raw-fenced-effect-7132" not in database_bytes
    assert b"raw-target-token-8241" not in database_bytes


def test_concurrent_idempotent_acquire_has_one_durable_winner(
    db_path: Path, clock: ManualClock
) -> None:
    creator = TruthLease(SQLiteStore(db_path), clock=clock, ids=SequentialIds())
    artifact = _artifact(creator)
    runtimes = [
        TruthLease(SQLiteStore(db_path), clock=clock),
        TruthLease(SQLiteStore(db_path), clock=clock),
    ]
    barrier = Barrier(2)
    lock = Lock()
    lease_ids: list[str] = []
    errors: list[BaseException] = []
    idempotency_key = _key("concurrent-workflow")

    def acquire(runtime: TruthLease) -> None:
        barrier.wait()
        try:
            lease = runtime.acquire_lease(
                artifact=artifact.ref,
                effect={"promote": True},
                target="deployment/current",
                operation="promote",
                idempotency_key=idempotency_key,
            )
            with lock:
                lease_ids.append(lease.lease_id)
        except BaseException as error:
            with lock:
                errors.append(error)

    threads = [Thread(target=acquire, args=(runtime,)) for runtime in runtimes]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(lease_ids) == 2
    assert len(set(lease_ids)) == 1
    connection = sqlite3.connect(db_path)
    try:
        request_count = int(connection.execute("SELECT count(*) FROM lease_requests").fetchone()[0])
        lease_count = int(connection.execute("SELECT count(*) FROM leases").fetchone()[0])
    finally:
        connection.close()
    assert request_count == 1
    assert lease_count == 1
