from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from conftest import ManualClock

from truthlease.engine import TruthLease, digest_effect
from truthlease.errors import TargetFenceError
from truthlease.models import (
    AssuranceEvidence,
    AssuranceLevel,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import (
    _SCHEMA_V1,
    APPLICATION_ID,
    SCHEMA_VERSION,
    SQLiteStore,
    _execute_schema,
)

_V1_TABLES = (
    "event_log",
    "observations",
    "artifacts",
    "dependencies",
    "leases",
    "lease_snapshots",
    "gate_receipts",
    "runtime_state",
)
_V2_TABLES = (
    "lease_fence_bindings",
    "fenced_attempts",
    "fenced_results",
    "fenced_receipts",
)


def _create_populated_v1(path: Path) -> None:
    effect_digest = digest_effect({"write": True}, operation="write", target="target/current")
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        _execute_schema(connection, _SCHEMA_V1)
        connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            "INSERT INTO runtime_state(singleton, coherence_fault, faulted_at) "
            "VALUES (1, NULL, NULL)"
        )
        connection.execute(
            """
            INSERT INTO event_log(
                source, event_id, event_type, subject, payload_digest, occurred_at, data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "test:v1",
                "event-1",
                "test.v1",
                "observation/policy",
                "sha256:event",
                "2026-07-20T11:58:00.000000Z",
                json.dumps({"legacy": True}),
            ),
        )
        connection.execute(
            """
            INSERT INTO observations(
                observation_key, version_token, content_digest, generation, observed_at,
                valid_until, source, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "policy",
                "v1",
                "sha256:observation",
                1,
                "2026-07-20T11:58:00.000000Z",
                None,
                "test:v1",
                "current",
            ),
        )
        connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, revision, artifact_kind, payload_digest, payload_ref,
                completeness, status, reconcile_action, created_at, invalidated_at,
                invalidation_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-plan",
                1,
                "plan",
                "sha256:artifact",
                None,
                "observed",
                "fresh",
                "replan",
                "2026-07-20T11:59:00.000000Z",
                None,
                None,
            ),
        )
        connection.execute(
            """
            INSERT INTO dependencies(
                artifact_id, artifact_revision, position, dependency_kind, dependency_id,
                expected_version, expected_generation, expected_revision, expected_digest,
                required
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-plan",
                1,
                0,
                "observation",
                "policy",
                "v1",
                1,
                None,
                "sha256:observation",
                1,
            ),
        )
        connection.execute(
            """
            INSERT INTO leases(
                lease_id, artifact_id, artifact_revision, artifact_digest, effect_digest,
                target, operation, required_assurance, dependency_completeness,
                closure_digest, issued_at, expires_at, boot_epoch, status, consumed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-fenced-lease",
                "legacy-plan",
                1,
                "sha256:artifact",
                effect_digest,
                "target/current",
                "write",
                "fenced",
                "observed",
                "sha256:legacy-closure",
                "2026-07-20T11:59:30.000000Z",
                "2026-07-20T12:30:00.000000Z",
                "persistent-local-v1",
                "active",
                None,
            ),
        )
        connection.execute(
            """
            INSERT INTO lease_snapshots(
                lease_id, position, observation_key, version_token, content_digest,
                observation_generation
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-fenced-lease",
                0,
                "policy",
                "v1",
                "sha256:observation",
                1,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _counts(path: Path, tables: tuple[str, ...]) -> dict[str, int]:
    connection = sqlite3.connect(path)
    try:
        return {
            table: int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
    finally:
        connection.close()


def test_v1_to_v2_migration_preserves_rows_without_false_fence_backfill(
    db_path: Path, clock: ManualClock
) -> None:
    _create_populated_v1(db_path)
    before = _counts(db_path, _V1_TABLES)

    store = SQLiteStore(db_path)
    store.initialize()

    assert store.integrity_check()["ok"] is True
    assert store.integrity_check()["schema_version"] == SCHEMA_VERSION == 2
    assert _counts(db_path, _V1_TABLES) == before
    assert _counts(db_path, _V2_TABLES) == {table: 0 for table in _V2_TABLES}

    runtime = TruthLease(
        store,
        clock=clock,
        trusted_evidence_authorities={"evidence:test"},
        trusted_target_adapters={"adapter:test"},
    )
    lease = runtime.store.transaction(write=False)
    with lease as tx:
        legacy = tx.get_lease("legacy-fenced-lease")
    assert legacy is not None
    precondition = TargetPrecondition(
        adapter_id="adapter:test",
        mechanism="test-compare-and-swap",
        expected_token="v1",
        protected_observations=("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )
    evidence = AssuranceEvidence(
        level=AssuranceLevel.REVALIDATED,
        authority="evidence:test",
        evidence_digest="sha256:evidence",
        observed_at=clock.now(),
        lease_id=legacy.lease_id,
        closure_digest=legacy.closure_digest,
        effect_digest=legacy.effect_digest,
        target=legacy.target,
        operation=legacy.operation,
    )
    with pytest.raises(TargetFenceError, match="no target fence binding"):
        runtime.prepare_fenced_commit(
            legacy.lease_id,
            {"write": True},
            precondition,
            evidence,
        )


def test_malformed_v1_shape_is_rejected_before_any_v2_mutation(db_path: Path) -> None:
    _create_populated_v1(db_path)
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("DROP INDEX leases_by_artifact")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="schema shape"):
        SQLiteStore(db_path).initialize()

    connection = sqlite3.connect(db_path)
    try:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 1
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    finally:
        connection.close()
    assert not (tables & set(_V2_TABLES))
