"""Safe single-writer SQLite adapter.

The adapter deliberately uses the rollback journal. Network I/O must never run
inside ``transaction``; commit-gate consumers should enqueue work after a local
decision or use a target-side fencing primitive.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from truthlease.clock import parse_utc, utc_text
from truthlease.errors import ConflictError, NotFoundError
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
    FencedReceipt,
    FencedResult,
    Lease,
    LeaseStatus,
    Observation,
    ObservationStatus,
    ReconcileAction,
    ReplayAssurance,
)

SCHEMA_VERSION = 2
APPLICATION_ID = 0x54525554  # ASCII "TRUT"
_V1_EXPECTED_TABLES = {
    "artifacts",
    "dependencies",
    "event_log",
    "gate_receipts",
    "lease_snapshots",
    "leases",
    "observations",
    "runtime_state",
}
_V1_EXPECTED_COLUMNS = {
    "event_log": {
        "sequence",
        "source",
        "event_id",
        "event_type",
        "subject",
        "payload_digest",
        "occurred_at",
        "data_json",
    },
    "observations": {
        "observation_key",
        "version_token",
        "content_digest",
        "generation",
        "observed_at",
        "valid_until",
        "source",
        "status",
    },
    "artifacts": {
        "artifact_id",
        "revision",
        "artifact_kind",
        "payload_digest",
        "payload_ref",
        "completeness",
        "status",
        "reconcile_action",
        "created_at",
        "invalidated_at",
        "invalidation_reason",
    },
    "dependencies": {
        "artifact_id",
        "artifact_revision",
        "position",
        "dependency_kind",
        "dependency_id",
        "expected_version",
        "expected_generation",
        "expected_revision",
        "expected_digest",
        "required",
    },
    "leases": {
        "lease_id",
        "artifact_id",
        "artifact_revision",
        "artifact_digest",
        "effect_digest",
        "target",
        "operation",
        "required_assurance",
        "dependency_completeness",
        "closure_digest",
        "issued_at",
        "expires_at",
        "boot_epoch",
        "status",
        "consumed_at",
    },
    "lease_snapshots": {
        "lease_id",
        "position",
        "observation_key",
        "version_token",
        "content_digest",
        "observation_generation",
    },
    "gate_receipts": {
        "receipt_id",
        "lease_id",
        "assurance",
        "effect_digest",
        "created_at",
        "guarantee_text",
        "evidence_authority",
        "evidence_digest",
        "evidence_observed_at",
    },
    "runtime_state": {"singleton", "coherence_fault", "faulted_at"},
}
_V1_EXPECTED_INDEXES = {
    "dependencies_reverse_artifact",
    "dependencies_reverse_observation",
    "leases_by_artifact",
}
_EXPECTED_TABLES = _V1_EXPECTED_TABLES | {
    "lease_fence_bindings",
    "fenced_attempts",
    "fenced_results",
    "fenced_receipts",
}
_EXPECTED_COLUMNS = {
    **_V1_EXPECTED_COLUMNS,
    "lease_fence_bindings": {
        "lease_id",
        "adapter_id",
        "mechanism",
        "precondition_digest",
        "scope_digest",
        "coverage",
        "replay_assurance",
        "protected_observations_json",
        "created_at",
    },
    "fenced_attempts": {
        "attempt_id",
        "lease_id",
        "gate_receipt_id",
        "adapter_id",
        "request_digest",
        "precondition_digest",
        "scope_digest",
        "coverage",
        "replay_assurance",
        "status",
        "prepared_at",
        "completed_at",
        "result_digest",
    },
    "fenced_results": {
        "result_id",
        "attempt_id",
        "result_digest",
        "outcome",
        "adapter_id",
        "request_digest",
        "closure_digest",
        "effect_digest",
        "target",
        "operation",
        "precondition_digest",
        "scope_digest",
        "replay_assurance",
        "coverage",
        "target_result_digest",
        "target_receipt_ref",
        "committed_at",
        "recorded_at",
    },
    "fenced_receipts": {
        "receipt_id",
        "attempt_id",
        "result_id",
        "lease_id",
        "gate_receipt_id",
        "assurance",
        "adapter_id",
        "request_digest",
        "closure_digest",
        "effect_digest",
        "target",
        "operation",
        "precondition_digest",
        "scope_digest",
        "replay_assurance",
        "coverage",
        "target_result_digest",
        "target_receipt_ref",
        "committed_at",
        "recorded_at",
        "guarantee_text",
    },
}
_EXPECTED_INDEXES = _V1_EXPECTED_INDEXES | {
    "fenced_attempts_by_status",
    "fenced_results_by_attempt",
    "fenced_receipts_by_lease",
}


class SQLiteStore:
    """Persistence adapter with explicit transaction boundaries."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5_000) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        if busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be positive")

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=self.busy_timeout_ms / 1_000,
        )
        connection.row_factory = sqlite3.Row
        _configure_connection(connection, self.busy_timeout_ms)
        try:
            application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            existing_tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if application_id not in (0, APPLICATION_ID):
                raise RuntimeError(
                    f"database belongs to application_id {application_id}, not TruthLease"
                )
            if version not in (0, 1, SCHEMA_VERSION):
                raise RuntimeError(
                    f"unsupported database schema version {version}; expected 1 or {SCHEMA_VERSION}"
                )
            if version == 0 and (application_id != 0 or existing_tables):
                raise RuntimeError("refusing to initialize a non-empty unversioned database")
            if version in (1, SCHEMA_VERSION) and application_id != APPLICATION_ID:
                raise RuntimeError("versioned database is missing the TruthLease application_id")
            if version in (1, SCHEMA_VERSION) and not _schema_shape_ok(connection, version=version):
                raise RuntimeError("TruthLease database schema shape is incomplete or unexpected")
            _configure_owned_connection(connection)
            connection.execute("BEGIN IMMEDIATE")
            if version == 0:
                _execute_schema(connection)
                connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                connection.execute(
                    "INSERT INTO runtime_state(singleton, coherence_fault, faulted_at) "
                    "VALUES (1, NULL, NULL)"
                )
            elif version == 1:
                _execute_schema(connection, _SCHEMA_V2_ADDITIONS)
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def transaction(self, *, write: bool) -> Iterator[SQLiteUnitOfWork]:
        connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=self.busy_timeout_ms / 1_000,
        )
        connection.row_factory = sqlite3.Row
        _configure_connection(connection, self.busy_timeout_ms)
        _configure_owned_connection(connection)
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield SQLiteUnitOfWork(connection)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def integrity_check(self) -> dict[str, Any]:
        with self.transaction(write=False) as tx:
            integrity = [row[0] for row in tx.connection.execute("PRAGMA integrity_check")]
            foreign_keys = [dict(row) for row in tx.connection.execute("PRAGMA foreign_key_check")]
            shape_ok = _schema_shape_ok(tx.connection, version=SCHEMA_VERSION)
            application_id = int(tx.connection.execute("PRAGMA application_id").fetchone()[0])
            version = int(tx.connection.execute("PRAGMA user_version").fetchone()[0])
            return {
                "ok": (
                    integrity == ["ok"]
                    and not foreign_keys
                    and application_id == APPLICATION_ID
                    and version == SCHEMA_VERSION
                    and shape_ok
                ),
                "integrity": integrity,
                "foreign_key_errors": foreign_keys,
                "journal_mode": tx.connection.execute("PRAGMA journal_mode").fetchone()[0],
                "foreign_keys_enabled": bool(
                    tx.connection.execute("PRAGMA foreign_keys").fetchone()[0]
                ),
                "schema_version": version,
                "application_id": application_id,
                "schema_shape_ok": shape_ok,
                "coherence_fault": tx.get_coherence_fault() if shape_ok else "schema-unreadable",
                "sqlite_version": sqlite3.sqlite_version,
            }


class SQLiteUnitOfWork:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def record_event(
        self,
        *,
        source: str,
        event_id: str,
        event_type: str,
        subject: str,
        payload_digest: str,
        occurred_at: str,
        data: dict[str, Any],
    ) -> bool:
        existing = self.connection.execute(
            "SELECT payload_digest FROM event_log WHERE source = ? AND event_id = ?",
            (source, event_id),
        ).fetchone()
        if existing is not None:
            if existing["payload_digest"] != payload_digest:
                raise ConflictError(
                    "event identity was reused with different content",
                    details={"source": source, "event_id": event_id},
                )
            return False
        self.connection.execute(
            """
            INSERT INTO event_log(
                source, event_id, event_type, subject, payload_digest, occurred_at, data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                event_id,
                event_type,
                subject,
                payload_digest,
                occurred_at,
                json.dumps(data, sort_keys=True, separators=(",", ":")),
            ),
        )
        return True

    def get_observation(self, key: str) -> Observation | None:
        row = self.connection.execute(
            "SELECT * FROM observations WHERE observation_key = ?", (key,)
        ).fetchone()
        return _observation_from_row(row) if row is not None else None

    def put_observation(self, observation: Observation) -> None:
        self.connection.execute(
            """
            INSERT INTO observations(
                observation_key, version_token, content_digest, generation, observed_at,
                valid_until, source, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(observation_key) DO UPDATE SET
                version_token = excluded.version_token,
                content_digest = excluded.content_digest,
                generation = excluded.generation,
                observed_at = excluded.observed_at,
                valid_until = excluded.valid_until,
                source = excluded.source,
                status = excluded.status
            """,
            (
                observation.key,
                observation.version,
                observation.digest,
                observation.generation,
                utc_text(observation.observed_at),
                utc_text(observation.valid_until) if observation.valid_until else None,
                observation.source,
                observation.status.value,
            ),
        )

    def next_artifact_revision(self, artifact_id: str) -> int:
        row = self.connection.execute(
            """
            SELECT COALESCE(MAX(revision), 0) + 1 AS revision
            FROM artifacts WHERE artifact_id = ?
            """,
            (artifact_id,),
        ).fetchone()
        return int(row["revision"])

    def insert_artifact(self, artifact: Artifact) -> None:
        self.connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, revision, artifact_kind, payload_digest, payload_ref,
                completeness, status, reconcile_action, created_at, invalidated_at,
                invalidation_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact.ref.artifact_id,
                artifact.ref.revision,
                artifact.kind,
                artifact.payload_digest,
                artifact.payload_ref,
                artifact.completeness.value,
                artifact.status.value,
                artifact.reconcile_action.value,
                utc_text(artifact.created_at),
                utc_text(artifact.invalidated_at) if artifact.invalidated_at else None,
                artifact.invalidation_reason,
            ),
        )
        for position, dependency in enumerate(artifact.dependencies):
            self.connection.execute(
                """
                INSERT INTO dependencies(
                    artifact_id, artifact_revision, position, dependency_kind,
                    dependency_id, expected_version, expected_generation, expected_revision,
                    expected_digest, required
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact.ref.artifact_id,
                    artifact.ref.revision,
                    position,
                    dependency.kind.value,
                    dependency.dependency_id,
                    dependency.expected_version,
                    dependency.expected_generation,
                    dependency.expected_revision,
                    dependency.expected_digest,
                    int(dependency.required),
                ),
            )

    def get_artifact(self, artifact_id: str, revision: int | None = None) -> Artifact | None:
        if revision is None:
            row = self.connection.execute(
                """
                SELECT * FROM artifacts
                WHERE artifact_id = ?
                ORDER BY revision DESC LIMIT 1
                """,
                (artifact_id,),
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ? AND revision = ?",
                (artifact_id, revision),
            ).fetchone()
        if row is None:
            return None
        dependencies = self.connection.execute(
            """
            SELECT * FROM dependencies
            WHERE artifact_id = ? AND artifact_revision = ?
            ORDER BY position
            """,
            (row["artifact_id"], row["revision"]),
        ).fetchall()
        return _artifact_from_row(row, dependencies)

    def require_artifact(self, ref: ArtifactRef) -> Artifact:
        artifact = self.get_artifact(ref.artifact_id, ref.revision)
        if artifact is None:
            raise NotFoundError(
                "artifact revision does not exist",
                details={"artifact_id": ref.artifact_id, "revision": ref.revision},
            )
        return artifact

    def observation_dependants(
        self,
        key: str,
        *,
        expected_generation: int | None = None,
        only_fresh: bool = False,
        limit: int | None = None,
    ) -> list[ArtifactRef]:
        generation_clause = (
            " AND d.expected_generation = ?" if expected_generation is not None else ""
        )
        freshness_clause = " AND a.status = 'fresh'" if only_fresh else ""
        limit_clause = " LIMIT ?" if limit is not None else ""
        parameters: tuple[Any, ...] = (DependencyKind.OBSERVATION.value, key)
        if expected_generation is not None:
            parameters += (expected_generation,)
        if limit is not None:
            parameters += (limit,)
        rows = self.connection.execute(
            f"""
            SELECT d.artifact_id, d.artifact_revision
            FROM dependencies d
            JOIN artifacts a
              ON a.artifact_id = d.artifact_id AND a.revision = d.artifact_revision
            WHERE d.dependency_kind = ? AND d.dependency_id = ? AND d.required = 1
            {generation_clause}
            {freshness_clause}
            ORDER BY d.artifact_id, d.artifact_revision
            {limit_clause}
            """,
            parameters,
        ).fetchall()
        return [ArtifactRef(row["artifact_id"], row["artifact_revision"]) for row in rows]

    def artifact_dependants(
        self, ref: ArtifactRef, *, limit: int | None = None
    ) -> list[ArtifactRef]:
        limit_clause = " LIMIT ?" if limit is not None else ""
        parameters: tuple[Any, ...] = (
            DependencyKind.ARTIFACT.value,
            ref.artifact_id,
            ref.revision,
        )
        if limit is not None:
            parameters += (limit,)
        rows = self.connection.execute(
            f"""
            SELECT d.artifact_id, d.artifact_revision
            FROM dependencies d
            WHERE d.dependency_kind = ? AND d.dependency_id = ?
              AND d.expected_revision = ? AND d.required = 1
            ORDER BY d.artifact_id, d.artifact_revision
            {limit_clause}
            """,
            parameters,
        ).fetchall()
        return [ArtifactRef(row["artifact_id"], row["artifact_revision"]) for row in rows]

    def iter_artifact_dependants(
        self, ref: ArtifactRef, *, only_fresh: bool = False
    ) -> Iterator[ArtifactRef]:
        freshness_clause = " AND a.status = 'fresh'" if only_fresh else ""
        rows = self.connection.execute(
            f"""
            SELECT d.artifact_id, d.artifact_revision
            FROM dependencies d
            JOIN artifacts a
              ON a.artifact_id = d.artifact_id AND a.revision = d.artifact_revision
            WHERE d.dependency_kind = ? AND d.dependency_id = ?
              AND d.expected_revision = ? AND d.required = 1
              {freshness_clause}
            ORDER BY d.artifact_id, d.artifact_revision
            """,
            (DependencyKind.ARTIFACT.value, ref.artifact_id, ref.revision),
        )
        for row in rows:
            yield ArtifactRef(row["artifact_id"], row["artifact_revision"])

    def mark_artifacts(
        self,
        refs: Sequence[ArtifactRef],
        *,
        status: ArtifactStatus,
        at: str,
        reason: str,
    ) -> None:
        self.connection.executemany(
            """
            UPDATE artifacts
            SET status = ?, invalidated_at = ?, invalidation_reason = ?
            WHERE artifact_id = ? AND revision = ? AND status != ?
            """,
            [
                (
                    status.value,
                    at,
                    reason,
                    ref.artifact_id,
                    ref.revision,
                    ArtifactStatus.REVOKED.value,
                )
                for ref in refs
            ],
        )

    def set_artifact_states(
        self,
        states: Sequence[tuple[ArtifactRef, ArtifactStatus]],
        *,
        at: str,
        reason: str,
    ) -> None:
        self.connection.executemany(
            """
            UPDATE artifacts
            SET status = ?,
                invalidated_at = CASE WHEN ? = 'fresh' THEN NULL ELSE ? END,
                invalidation_reason = CASE WHEN ? = 'fresh' THEN NULL ELSE ? END
            WHERE artifact_id = ? AND revision = ? AND status != 'revoked'
            """,
            [
                (
                    status.value,
                    status.value,
                    at,
                    status.value,
                    reason,
                    ref.artifact_id,
                    ref.revision,
                )
                for ref, status in states
            ],
        )

    def fail_closed(self, *, reason: str, at: str) -> int:
        self.connection.execute(
            "UPDATE runtime_state SET coherence_fault = ?, faulted_at = ? WHERE singleton = 1",
            (reason, at),
        )
        self.connection.execute(
            """
            UPDATE artifacts
            SET status = 'unknown', invalidated_at = ?, invalidation_reason = ?
            WHERE status != 'revoked'
            """,
            (at, reason),
        )
        cursor = self.connection.execute(
            "UPDATE leases SET status = 'invalidated' WHERE status = 'active'"
        )
        return int(cursor.rowcount)

    def get_coherence_fault(self) -> str | None:
        row = self.connection.execute(
            "SELECT coherence_fault FROM runtime_state WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("runtime_state singleton is missing")
        value = row["coherence_fault"]
        return str(value) if value is not None else None

    def active_leases_for(self, refs: Sequence[ArtifactRef]) -> list[str]:
        lease_ids: list[str] = []
        for ref in refs:
            rows = self.connection.execute(
                """
                SELECT lease_id FROM leases
                WHERE artifact_id = ? AND artifact_revision = ? AND status = ?
                ORDER BY lease_id
                """,
                (ref.artifact_id, ref.revision, LeaseStatus.ACTIVE.value),
            ).fetchall()
            lease_ids.extend(row["lease_id"] for row in rows)
        return lease_ids

    def invalidate_leases(self, lease_ids: Sequence[str]) -> None:
        self.connection.executemany(
            "UPDATE leases SET status = ? WHERE lease_id = ? AND status = ?",
            [
                (LeaseStatus.INVALIDATED.value, lease_id, LeaseStatus.ACTIVE.value)
                for lease_id in lease_ids
            ],
        )

    def insert_lease(self, lease: Lease) -> None:
        self.connection.execute(
            """
            INSERT INTO leases(
                lease_id, artifact_id, artifact_revision, artifact_digest,
                effect_digest, target, operation, required_assurance,
                dependency_completeness, closure_digest, issued_at, expires_at,
                boot_epoch, status, consumed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lease.lease_id,
                lease.artifact.artifact_id,
                lease.artifact.revision,
                lease.artifact_digest,
                lease.effect_digest,
                lease.target,
                lease.operation,
                lease.required_assurance.value,
                lease.dependency_completeness.value,
                lease.closure_digest,
                utc_text(lease.issued_at),
                utc_text(lease.expires_at),
                lease.boot_epoch,
                lease.status.value,
                utc_text(lease.consumed_at) if lease.consumed_at else None,
            ),
        )
        self.connection.executemany(
            """
            INSERT INTO lease_snapshots(
                lease_id, position, observation_key, version_token,
                content_digest, observation_generation
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    lease.lease_id,
                    position,
                    item.key,
                    item.version,
                    item.digest,
                    item.generation,
                )
                for position, item in enumerate(lease.snapshot)
            ],
        )

    def get_lease(self, lease_id: str) -> Lease | None:
        row = self.connection.execute(
            "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            return None
        snapshot_rows = self.connection.execute(
            "SELECT * FROM lease_snapshots WHERE lease_id = ? ORDER BY position",
            (lease_id,),
        ).fetchall()
        return _lease_from_row(row, snapshot_rows)

    def set_lease_status(
        self, lease_id: str, status: LeaseStatus, *, consumed_at: str | None = None
    ) -> None:
        self.connection.execute(
            "UPDATE leases SET status = ?, consumed_at = ? WHERE lease_id = ?",
            (status.value, consumed_at, lease_id),
        )

    def insert_receipt(
        self,
        *,
        receipt_id: str,
        lease_id: str,
        assurance: AssuranceLevel,
        effect_digest: str,
        created_at: str,
        guarantee: str,
        evidence: AssuranceEvidence | None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO gate_receipts(
                receipt_id, lease_id, assurance, effect_digest, created_at, guarantee_text,
                evidence_authority, evidence_digest, evidence_observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt_id,
                lease_id,
                assurance.value,
                effect_digest,
                created_at,
                guarantee,
                evidence.authority if evidence else None,
                evidence.evidence_digest if evidence else None,
                utc_text(evidence.observed_at) if evidence else None,
            ),
        )

    def insert_lease_fence_binding(
        self,
        *,
        lease_id: str,
        adapter_id: str,
        mechanism: str,
        precondition_digest: str,
        scope_digest: str,
        coverage: FenceCoverage,
        replay_assurance: ReplayAssurance,
        protected_observations: Sequence[str],
        created_at: str,
    ) -> None:
        """Persist a digest-only target binding; raw precondition tokens never enter SQLite."""
        self.connection.execute(
            """
            INSERT INTO lease_fence_bindings(
                lease_id, adapter_id, mechanism, precondition_digest, scope_digest,
                coverage, replay_assurance, protected_observations_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lease_id,
                adapter_id,
                mechanism,
                precondition_digest,
                scope_digest,
                coverage.value,
                replay_assurance.value,
                json.dumps(sorted(protected_observations), separators=(",", ":")),
                created_at,
            ),
        )

    def get_lease_fence_binding(self, lease_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM lease_fence_bindings WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            return None
        protected = json.loads(row["protected_observations_json"])
        if not isinstance(protected, list) or any(not isinstance(item, str) for item in protected):
            raise RuntimeError("stored fence binding has invalid protected observations")
        return {
            "lease_id": row["lease_id"],
            "adapter_id": row["adapter_id"],
            "mechanism": row["mechanism"],
            "precondition_digest": row["precondition_digest"],
            "scope_digest": row["scope_digest"],
            "coverage": FenceCoverage(row["coverage"]),
            "replay_assurance": ReplayAssurance(row["replay_assurance"]),
            "protected_observations": tuple(protected),
            "created_at": parse_utc(row["created_at"]),
        }

    def insert_fenced_attempt(self, attempt: FencedAttempt) -> None:
        self.connection.execute(
            """
            INSERT INTO fenced_attempts(
                attempt_id, lease_id, gate_receipt_id, adapter_id, request_digest,
                precondition_digest, scope_digest, coverage, replay_assurance, status,
                prepared_at, completed_at, result_digest
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attempt.attempt_id,
                attempt.lease_id,
                attempt.gate_receipt_id,
                attempt.adapter_id,
                attempt.request_digest,
                attempt.precondition_digest,
                attempt.scope_digest,
                attempt.coverage.value,
                attempt.replay_assurance.value,
                attempt.status.value,
                utc_text(attempt.prepared_at),
                utc_text(attempt.completed_at) if attempt.completed_at else None,
                attempt.result_digest,
            ),
        )

    def get_fenced_attempt(self, attempt_id: str) -> FencedAttempt | None:
        row = self.connection.execute(
            "SELECT * FROM fenced_attempts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return _fenced_attempt_from_row(row) if row is not None else None

    def get_fenced_attempt_for_lease(self, lease_id: str) -> FencedAttempt | None:
        row = self.connection.execute(
            "SELECT * FROM fenced_attempts WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        return _fenced_attempt_from_row(row) if row is not None else None

    def set_fenced_attempt_result(
        self,
        attempt_id: str,
        *,
        status: FencedAttemptStatus,
        completed_at: str | None,
        result_digest: str,
    ) -> None:
        cursor = self.connection.execute(
            """
            UPDATE fenced_attempts
            SET status = ?, completed_at = ?, result_digest = ?
            WHERE attempt_id = ?
            """,
            (status.value, completed_at, result_digest, attempt_id),
        )
        if cursor.rowcount != 1:
            raise NotFoundError("fenced attempt does not exist", details={"attempt_id": attempt_id})

    def insert_fenced_result(
        self,
        *,
        result_id: str,
        result_digest: str,
        result: FencedResult,
        recorded_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO fenced_results(
                result_id, attempt_id, result_digest, outcome, adapter_id,
                request_digest, closure_digest, effect_digest, target, operation,
                precondition_digest, scope_digest, replay_assurance, coverage,
                target_result_digest, target_receipt_ref, committed_at, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result_id,
                result.attempt_id,
                result_digest,
                result.outcome.value,
                result.adapter_id,
                result.request_digest,
                result.closure_digest,
                result.effect_digest,
                result.target,
                result.operation,
                result.precondition_digest,
                result.scope_digest,
                result.replay_assurance.value,
                result.coverage.value,
                result.target_result_digest,
                result.target_receipt_ref,
                utc_text(result.committed_at) if result.committed_at else None,
                recorded_at,
            ),
        )

    def get_fenced_result_id(self, attempt_id: str, result_digest: str) -> str | None:
        row = self.connection.execute(
            """
            SELECT result_id FROM fenced_results
            WHERE attempt_id = ? AND result_digest = ?
            """,
            (attempt_id, result_digest),
        ).fetchone()
        return str(row["result_id"]) if row is not None else None

    def insert_fenced_receipt(self, receipt: FencedReceipt, *, result_id: str) -> None:
        self.connection.execute(
            """
            INSERT INTO fenced_receipts(
                receipt_id, attempt_id, result_id, lease_id, gate_receipt_id,
                assurance, adapter_id, request_digest, closure_digest, effect_digest,
                target, operation, precondition_digest, scope_digest, replay_assurance,
                coverage, target_result_digest, target_receipt_ref, committed_at,
                recorded_at, guarantee_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.attempt_id,
                result_id,
                receipt.lease_id,
                receipt.gate_receipt_id,
                receipt.assurance.value,
                receipt.adapter_id,
                receipt.request_digest,
                receipt.closure_digest,
                receipt.effect_digest,
                receipt.target,
                receipt.operation,
                receipt.precondition_digest,
                receipt.scope_digest,
                receipt.replay_assurance.value,
                receipt.coverage.value,
                receipt.target_result_digest,
                receipt.target_receipt_ref,
                utc_text(receipt.committed_at),
                utc_text(receipt.recorded_at),
                receipt.guarantee,
            ),
        )

    def get_fenced_receipt(self, attempt_id: str) -> FencedReceipt | None:
        row = self.connection.execute(
            "SELECT * FROM fenced_receipts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return _fenced_receipt_from_row(row) if row is not None else None

    def list_artifacts(self) -> list[Artifact]:
        rows = self.connection.execute(
            "SELECT artifact_id, revision FROM artifacts ORDER BY artifact_id, revision"
        ).fetchall()
        return [
            self.require_artifact(ArtifactRef(row["artifact_id"], row["revision"])) for row in rows
        ]

    def list_events(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM event_log ORDER BY sequence").fetchall()
        return [
            {
                "sequence": row["sequence"],
                "source": row["source"],
                "id": row["event_id"],
                "type": row["event_type"],
                "subject": row["subject"],
                "payload_digest": row["payload_digest"],
                "time": row["occurred_at"],
                "data": json.loads(row["data_json"]),
            }
            for row in rows
        ]


def _configure_connection(connection: sqlite3.Connection, busy_timeout_ms: int) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA trusted_schema = OFF")
    connection.execute("PRAGMA read_uncommitted = OFF")
    connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")


def _configure_owned_connection(connection: sqlite3.Connection) -> None:
    journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    if journal_mode != "delete":
        raise RuntimeError(f"TruthLease requires SQLite DELETE journal mode; found {journal_mode}")
    connection.execute("PRAGMA synchronous = FULL")


def _schema_shape_ok(connection: sqlite3.Connection, *, version: int) -> bool:
    if version == 1:
        expected_tables = _V1_EXPECTED_TABLES
        expected_columns = _V1_EXPECTED_COLUMNS
        expected_indexes = _V1_EXPECTED_INDEXES
    elif version == SCHEMA_VERSION:
        expected_tables = _EXPECTED_TABLES
        expected_columns = _EXPECTED_COLUMNS
        expected_indexes = _EXPECTED_INDEXES
    else:
        return False
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if tables != expected_tables:
        return False
    for table, expected in expected_columns.items():
        columns = {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}
        if columns != expected:
            return False
    indexes = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    return indexes >= expected_indexes


def _observation_from_row(row: sqlite3.Row) -> Observation:
    return Observation(
        key=row["observation_key"],
        version=row["version_token"],
        digest=row["content_digest"],
        generation=row["generation"],
        observed_at=parse_utc(row["observed_at"]),
        status=ObservationStatus(row["status"]),
        valid_until=parse_utc(row["valid_until"]) if row["valid_until"] else None,
        source=row["source"],
    )


def _artifact_from_row(row: sqlite3.Row, dependency_rows: Sequence[sqlite3.Row]) -> Artifact:
    dependencies = tuple(
        ArtifactDependency(
            kind=DependencyKind(item["dependency_kind"]),
            dependency_id=item["dependency_id"],
            expected_version=item["expected_version"],
            expected_generation=item["expected_generation"],
            expected_revision=item["expected_revision"],
            expected_digest=item["expected_digest"],
            required=bool(item["required"]),
        )
        for item in dependency_rows
    )
    return Artifact(
        ref=ArtifactRef(row["artifact_id"], row["revision"]),
        kind=row["artifact_kind"],
        payload_digest=row["payload_digest"],
        payload_ref=row["payload_ref"],
        dependencies=dependencies,
        completeness=DependencyCompleteness(row["completeness"]),
        status=ArtifactStatus(row["status"]),
        reconcile_action=ReconcileAction(row["reconcile_action"]),
        created_at=parse_utc(row["created_at"]),
        invalidated_at=parse_utc(row["invalidated_at"]) if row["invalidated_at"] else None,
        invalidation_reason=row["invalidation_reason"],
    )


def _lease_from_row(row: sqlite3.Row, snapshot_rows: Sequence[sqlite3.Row]) -> Lease:
    return Lease(
        lease_id=row["lease_id"],
        artifact=ArtifactRef(row["artifact_id"], row["artifact_revision"]),
        artifact_digest=row["artifact_digest"],
        effect_digest=row["effect_digest"],
        target=row["target"],
        operation=row["operation"],
        required_assurance=AssuranceLevel(row["required_assurance"]),
        dependency_completeness=DependencyCompleteness(row["dependency_completeness"]),
        closure_digest=row["closure_digest"],
        snapshot=tuple(
            DependencySnapshot(
                key=item["observation_key"],
                version=item["version_token"],
                generation=item["observation_generation"],
                digest=item["content_digest"],
            )
            for item in snapshot_rows
        ),
        issued_at=parse_utc(row["issued_at"]),
        expires_at=parse_utc(row["expires_at"]),
        boot_epoch=row["boot_epoch"],
        status=LeaseStatus(row["status"]),
        consumed_at=parse_utc(row["consumed_at"]) if row["consumed_at"] else None,
    )


def _fenced_attempt_from_row(row: sqlite3.Row) -> FencedAttempt:
    return FencedAttempt(
        attempt_id=row["attempt_id"],
        lease_id=row["lease_id"],
        gate_receipt_id=row["gate_receipt_id"],
        adapter_id=row["adapter_id"],
        request_digest=row["request_digest"],
        precondition_digest=row["precondition_digest"],
        scope_digest=row["scope_digest"],
        replay_assurance=ReplayAssurance(row["replay_assurance"]),
        coverage=FenceCoverage(row["coverage"]),
        status=FencedAttemptStatus(row["status"]),
        prepared_at=parse_utc(row["prepared_at"]),
        completed_at=parse_utc(row["completed_at"]) if row["completed_at"] else None,
        result_digest=row["result_digest"],
    )


def _fenced_receipt_from_row(row: sqlite3.Row) -> FencedReceipt:
    return FencedReceipt(
        receipt_id=row["receipt_id"],
        attempt_id=row["attempt_id"],
        lease_id=row["lease_id"],
        gate_receipt_id=row["gate_receipt_id"],
        assurance=AssuranceLevel(row["assurance"]),
        adapter_id=row["adapter_id"],
        request_digest=row["request_digest"],
        closure_digest=row["closure_digest"],
        effect_digest=row["effect_digest"],
        target=row["target"],
        operation=row["operation"],
        precondition_digest=row["precondition_digest"],
        scope_digest=row["scope_digest"],
        replay_assurance=ReplayAssurance(row["replay_assurance"]),
        coverage=FenceCoverage(row["coverage"]),
        target_result_digest=row["target_result_digest"],
        target_receipt_ref=row["target_receipt_ref"],
        committed_at=parse_utc(row["committed_at"]),
        recorded_at=parse_utc(row["recorded_at"]),
        guarantee=row["guarantee_text"],
    )


_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS event_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    data_json TEXT NOT NULL,
    UNIQUE(source, event_id)
);

CREATE TABLE IF NOT EXISTS observations (
    observation_key TEXT PRIMARY KEY,
    version_token TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK(generation > 0),
    observed_at TEXT NOT NULL,
    valid_until TEXT,
    source TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('current', 'disputed', 'unknown', 'revoked'))
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision > 0),
    artifact_kind TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_ref TEXT,
    completeness TEXT NOT NULL CHECK(completeness IN ('unknown', 'declared', 'observed')),
    status TEXT NOT NULL CHECK(status IN ('fresh', 'stale', 'disputed', 'unknown', 'revoked')),
    reconcile_action TEXT NOT NULL
        CHECK(reconcile_action IN ('block', 'notify', 'recompute', 'replan', 'human')),
    created_at TEXT NOT NULL,
    invalidated_at TEXT,
    invalidation_reason TEXT,
    PRIMARY KEY(artifact_id, revision)
);

CREATE TABLE IF NOT EXISTS dependencies (
    artifact_id TEXT NOT NULL,
    artifact_revision INTEGER NOT NULL,
    position INTEGER NOT NULL,
    dependency_kind TEXT NOT NULL CHECK(dependency_kind IN ('observation', 'artifact')),
    dependency_id TEXT NOT NULL,
    expected_version TEXT,
    expected_generation INTEGER,
    expected_revision INTEGER,
    expected_digest TEXT NOT NULL,
    required INTEGER NOT NULL CHECK(required IN (0, 1)),
    PRIMARY KEY(artifact_id, artifact_revision, position),
    FOREIGN KEY(artifact_id, artifact_revision)
        REFERENCES artifacts(artifact_id, revision) ON DELETE CASCADE,
    CHECK(
        (dependency_kind = 'observation'
            AND expected_version IS NOT NULL
            AND expected_generation IS NOT NULL
            AND expected_generation > 0
            AND expected_revision IS NULL)
        OR
        (dependency_kind = 'artifact'
            AND expected_version IS NULL
            AND expected_generation IS NULL
            AND expected_revision IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS dependencies_reverse_observation
ON dependencies(dependency_kind, dependency_id, expected_version);

CREATE INDEX IF NOT EXISTS dependencies_reverse_artifact
ON dependencies(dependency_kind, dependency_id, expected_revision);

CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    artifact_revision INTEGER NOT NULL,
    artifact_digest TEXT NOT NULL,
    effect_digest TEXT NOT NULL,
    target TEXT NOT NULL,
    operation TEXT NOT NULL,
    required_assurance TEXT NOT NULL
        CHECK(required_assurance IN ('declared', 'observed', 'revalidated', 'fenced')),
    dependency_completeness TEXT NOT NULL
        CHECK(dependency_completeness IN ('unknown', 'declared', 'observed')),
    closure_digest TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    boot_epoch TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK(status IN ('active', 'consumed', 'expired', 'invalidated', 'revoked')),
    consumed_at TEXT,
    FOREIGN KEY(artifact_id, artifact_revision)
        REFERENCES artifacts(artifact_id, revision)
);

CREATE INDEX IF NOT EXISTS leases_by_artifact
ON leases(artifact_id, artifact_revision, status, expires_at);

CREATE TABLE IF NOT EXISTS lease_snapshots (
    lease_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    observation_key TEXT NOT NULL,
    version_token TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    observation_generation INTEGER NOT NULL CHECK(observation_generation > 0),
    PRIMARY KEY(lease_id, position),
    FOREIGN KEY(lease_id) REFERENCES leases(lease_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gate_receipts (
    receipt_id TEXT PRIMARY KEY,
    lease_id TEXT NOT NULL UNIQUE,
    assurance TEXT NOT NULL CHECK(assurance IN ('declared', 'observed', 'revalidated', 'fenced')),
    effect_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    guarantee_text TEXT NOT NULL,
    evidence_authority TEXT,
    evidence_digest TEXT,
    evidence_observed_at TEXT,
    FOREIGN KEY(lease_id) REFERENCES leases(lease_id)
);

CREATE TABLE IF NOT EXISTS runtime_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    coherence_fault TEXT,
    faulted_at TEXT
);
"""


_SCHEMA_V2_ADDITIONS = """
CREATE TABLE IF NOT EXISTS lease_fence_bindings (
    lease_id TEXT PRIMARY KEY,
    adapter_id TEXT NOT NULL,
    mechanism TEXT NOT NULL,
    precondition_digest TEXT NOT NULL,
    scope_digest TEXT NOT NULL,
    coverage TEXT NOT NULL CHECK(coverage IN ('full', 'partial')),
    replay_assurance TEXT NOT NULL
        CHECK(replay_assurance IN ('deduplicated', 'one_shot', 'unknown')),
    protected_observations_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(lease_id) REFERENCES leases(lease_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fenced_attempts (
    attempt_id TEXT PRIMARY KEY,
    lease_id TEXT NOT NULL UNIQUE,
    gate_receipt_id TEXT NOT NULL UNIQUE,
    adapter_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    precondition_digest TEXT NOT NULL,
    scope_digest TEXT NOT NULL,
    coverage TEXT NOT NULL CHECK(coverage IN ('full', 'partial')),
    replay_assurance TEXT NOT NULL
        CHECK(replay_assurance IN ('deduplicated', 'one_shot', 'unknown')),
    status TEXT NOT NULL
        CHECK(status IN ('prepared', 'committed', 'not_committed', 'indeterminate', 'disputed')),
    prepared_at TEXT NOT NULL,
    completed_at TEXT,
    result_digest TEXT,
    FOREIGN KEY(lease_id) REFERENCES leases(lease_id),
    FOREIGN KEY(gate_receipt_id) REFERENCES gate_receipts(receipt_id)
);

CREATE INDEX IF NOT EXISTS fenced_attempts_by_status
ON fenced_attempts(status, prepared_at);

CREATE TABLE IF NOT EXISTS fenced_results (
    result_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    result_digest TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('committed', 'not_committed', 'indeterminate')),
    adapter_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    closure_digest TEXT NOT NULL,
    effect_digest TEXT NOT NULL,
    target TEXT NOT NULL,
    operation TEXT NOT NULL,
    precondition_digest TEXT NOT NULL,
    scope_digest TEXT NOT NULL,
    replay_assurance TEXT NOT NULL
        CHECK(replay_assurance IN ('deduplicated', 'one_shot', 'unknown')),
    coverage TEXT NOT NULL CHECK(coverage IN ('full', 'partial')),
    target_result_digest TEXT NOT NULL,
    target_receipt_ref TEXT,
    committed_at TEXT,
    recorded_at TEXT NOT NULL,
    UNIQUE(attempt_id, result_digest),
    FOREIGN KEY(attempt_id) REFERENCES fenced_attempts(attempt_id)
);

CREATE INDEX IF NOT EXISTS fenced_results_by_attempt
ON fenced_results(attempt_id, recorded_at);

CREATE TABLE IF NOT EXISTS fenced_receipts (
    receipt_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE,
    result_id TEXT NOT NULL UNIQUE,
    lease_id TEXT NOT NULL UNIQUE,
    gate_receipt_id TEXT NOT NULL UNIQUE,
    assurance TEXT NOT NULL CHECK(assurance = 'fenced'),
    adapter_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    closure_digest TEXT NOT NULL,
    effect_digest TEXT NOT NULL,
    target TEXT NOT NULL,
    operation TEXT NOT NULL,
    precondition_digest TEXT NOT NULL,
    scope_digest TEXT NOT NULL,
    replay_assurance TEXT NOT NULL CHECK(replay_assurance = 'deduplicated'),
    coverage TEXT NOT NULL CHECK(coverage = 'full'),
    target_result_digest TEXT NOT NULL,
    target_receipt_ref TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    guarantee_text TEXT NOT NULL,
    FOREIGN KEY(attempt_id) REFERENCES fenced_attempts(attempt_id),
    FOREIGN KEY(result_id) REFERENCES fenced_results(result_id),
    FOREIGN KEY(lease_id) REFERENCES leases(lease_id),
    FOREIGN KEY(gate_receipt_id) REFERENCES gate_receipts(receipt_id)
);

CREATE INDEX IF NOT EXISTS fenced_receipts_by_lease
ON fenced_receipts(lease_id);
"""

_SCHEMA = _SCHEMA_V1 + _SCHEMA_V2_ADDITIONS


def _execute_schema(connection: sqlite3.Connection, schema: str = _SCHEMA) -> None:
    pending = ""
    for line in schema.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            connection.execute(pending)
            pending = ""
    if pending.strip():
        raise RuntimeError("incomplete TruthLease schema statement")
