from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from conftest import ManualClock, SequentialIds
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

import truthlease.integrations.langgraph as langgraph_integration
from truthlease.canonical import Digestable, digest_json
from truthlease.engine import TruthLease
from truthlease.integrations.langgraph import (
    StaleSafeGraph,
    StaleSafeState,
    build_stale_safe_graph,
    make_resume_state,
    workflow_idempotency_key,
)
from truthlease.models import (
    ArtifactDependency,
    ArtifactRef,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    FencedAttempt,
    FencedCommitRequest,
    FencedOutcome,
    FencedResult,
    Lease,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import SQLiteStore
from truthlease.target import execute_fenced

_EFFECT_SECRET = "raw-effect-must-never-be-checkpointed"
_TOKEN_SECRET = "raw-etag-must-never-be-checkpointed"
_TARGET = "urn:example:orders"
_OPERATION = "create-order"


@dataclass
class _TargetStore:
    current_token: str = _TOKEN_SECRET
    committed: dict[str, FencedResult] = field(default_factory=dict)
    effect_count: int = 0
    dispatch_count: int = 0
    lose_response_once: bool = False
    hard_crash_after_commit_once: bool = False


@dataclass
class _DeduplicatingTarget:
    store: _TargetStore
    adapter_id: str = "adapter:test"

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        self.store.dispatch_count += 1
        existing = self.store.committed.get(request.attempt_id)
        if existing is not None:
            return existing
        if request.precondition.expected_token != self.store.current_token:
            return _target_result(request, outcome=FencedOutcome.NOT_COMMITTED)

        self.store.effect_count += 1
        result = _target_result(request, outcome=FencedOutcome.COMMITTED)
        self.store.committed[request.attempt_id] = result
        if self.store.hard_crash_after_commit_once:
            self.store.hard_crash_after_commit_once = False
            raise SystemExit("simulated process death after target commit")
        if self.store.lose_response_once:
            self.store.lose_response_once = False
            raise TimeoutError("simulated response loss after target commit")
        return result


def _target_result(
    request: FencedCommitRequest,
    *,
    outcome: FencedOutcome,
) -> FencedResult:
    committed = outcome is FencedOutcome.COMMITTED
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
        target_result_digest=digest_json(
            {"attempt_id": request.attempt_id, "outcome": outcome.value},
            domain="test:langgraph-target-result",
        ),
        target_receipt_ref=f"target-receipt:{request.attempt_id}" if committed else None,
        committed_at=datetime(2026, 7, 22, 12, 0, tzinfo=UTC) if committed else None,
    )


class _Harness:
    def __init__(
        self,
        db_path: Path,
        clock: ManualClock,
        *,
        completeness: DependencyCompleteness = DependencyCompleteness.OBSERVED,
    ) -> None:
        self.db_path = db_path
        self.clock = clock
        self.runtime = self._new_runtime(ids=SequentialIds())
        self.authoritative_policy_version = "v1"
        self.revalidation_failure = False
        self.effect_ref = "effect/order/v1"
        self.precondition_ref = "precondition/orders/v1"
        self.effects: dict[str, Digestable] = {
            self.effect_ref: {"payload": _EFFECT_SECRET, "write": True}
        }
        self.preconditions = {
            self.precondition_ref: TargetPrecondition(
                adapter_id="adapter:test",
                mechanism="strong-etag",
                expected_token=_TOKEN_SECRET,
                protected_observations=("policy",),
                replay_assurance=ReplayAssurance.DEDUPLICATED,
            )
        }
        self.target_store = _TargetStore()
        self.evidence_calls = 0
        self.evidence_failure = False
        self.runtime.observe(key="policy", version="v1", value={"enabled": True})
        artifact = self.runtime.publish_artifact(
            artifact_id="plan",
            kind="plan",
            payload={"step": "create-order"},
            dependencies=[ArtifactDependency.observation(self.runtime.get_observation("policy"))],
            completeness=completeness,
        )
        self.artifact = artifact.ref
        self.idempotency_key = workflow_idempotency_key({"workflow": "order-42"})

    def _new_runtime(self, *, ids: SequentialIds | None = None) -> TruthLease:
        return TruthLease(
            SQLiteStore(self.db_path),
            clock=self.clock,
            ids=ids,
            trusted_evidence_authorities={"evidence:test"},
            trusted_target_adapters={"adapter:test"},
        )

    def restart_runtime(self) -> None:
        self.runtime = self._new_runtime()

    def revalidate(self, runtime: TruthLease, _artifact: ArtifactRef) -> None:
        if self.revalidation_failure:
            raise RuntimeError("simulated authoritative source failure")
        observed = runtime.get_observation("policy")
        if observed.version != self.authoritative_policy_version:
            runtime.observe(
                key="policy",
                version=self.authoritative_policy_version,
                value={"enabled": self.authoritative_policy_version == "v1"},
                expected_generation=observed.generation,
            )

    def resolve_effect(self, reference: str) -> Digestable:
        return self.effects[reference]

    def resolve_precondition(self, reference: str) -> TargetPrecondition:
        return self.preconditions[reference]

    def evidence(self, runtime: TruthLease, lease: Lease) -> AssuranceEvidence:
        self.evidence_calls += 1
        if self.evidence_failure:
            raise RuntimeError("simulated evidence provider failure")
        return AssuranceEvidence(
            level=AssuranceLevel.REVALIDATED,
            authority="evidence:test",
            evidence_digest=digest_json(
                {"lease_id": lease.lease_id, "closure": lease.closure_digest},
                domain="test:langgraph-evidence",
            ),
            observed_at=runtime.clock.now(),
            lease_id=lease.lease_id,
            closure_digest=lease.closure_digest,
            effect_digest=lease.effect_digest,
            target=lease.target,
            operation=lease.operation,
        )

    def graph(self, saver: SqliteSaver) -> StaleSafeGraph:
        return build_stale_safe_graph(
            self.runtime,
            checkpointer=saver,
            revalidate=self.revalidate,
            resolve_effect=self.resolve_effect,
            resolve_precondition=self.resolve_precondition,
            evidence_provider=self.evidence,
            target_adapter=_DeduplicatingTarget(self.target_store),
        )

    def state(
        self,
        *,
        effect_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> StaleSafeState:
        resolved_effect_ref = effect_ref or self.effect_ref
        return make_resume_state(
            idempotency_key=idempotency_key or self.idempotency_key,
            artifact=self.artifact,
            effect_ref=resolved_effect_ref,
            effect=self.effects[resolved_effect_ref],
            precondition_ref=self.precondition_ref,
            target_precondition=self.preconditions[self.precondition_ref],
            target=_TARGET,
            operation=_OPERATION,
        )


def _config(thread_id: str) -> RunnableConfig:
    return {"configurable": {"thread_id": thread_id}}


def _connection(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path, check_same_thread=False)


def _restart_failed_node(
    graph: StaleSafeGraph,
    config: RunnableConfig,
) -> StaleSafeState:
    return cast(StaleSafeState, graph.invoke(cast(StaleSafeState, None), config))


def test_persistent_restart_resumes_to_one_fenced_commit_and_redacts_raw_values(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    truth_db = tmp_path / "truthlease.db"
    checkpoint_db = tmp_path / "checkpoints.db"
    harness = _Harness(truth_db, clock)
    config = _config(harness.idempotency_key)

    first_connection = _connection(checkpoint_db)
    paused = harness.graph(SqliteSaver(first_connection)).invoke(harness.state(), config)
    assert "__interrupt__" in paused
    assert paused["phase"] == "awaiting_approval"
    first_connection.close()

    harness.restart_runtime()
    second_connection = _connection(checkpoint_db)
    committed = harness.graph(SqliteSaver(second_connection)).invoke(Command(resume=True), config)
    assert committed["phase"] == "committed"
    assert committed["decision_code"] == "FENCED_COMMIT_RECORDED"
    assert harness.target_store.effect_count == 1
    assert harness.evidence_calls == 1
    second_connection.close()

    persisted = truth_db.read_bytes() + checkpoint_db.read_bytes()
    assert _EFFECT_SECRET.encode() not in persisted
    assert _TOKEN_SECRET.encode() not in persisted


def test_policy_change_while_paused_fails_closed_before_target_effect(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("policy-change")

    graph.invoke(harness.state(), config)
    harness.authoritative_policy_version = "v2"
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "ARTIFACT_NOT_FRESH"
    assert harness.target_store.effect_count == 0
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_target_token_change_is_not_committed(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("target-token-change")

    graph.invoke(harness.state(), config)
    harness.target_store.current_token = "new-target-etag"
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "TARGET_PRECONDITION_REJECTED"
    assert harness.target_store.effect_count == 0
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_duplicate_workflow_dispatch_reuses_terminal_attempt_without_duplicate_effect(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))

    graph.invoke(harness.state(), _config("first-dispatch"))
    first = graph.invoke(Command(resume=True), _config("first-dispatch"))
    graph.invoke(harness.state(), _config("duplicate-dispatch"))
    duplicate = graph.invoke(Command(resume=True), _config("duplicate-dispatch"))

    assert first["phase"] == duplicate["phase"] == "committed"
    assert first["lease_id"] == duplicate["lease_id"]
    assert first["attempt_id"] == duplicate["attempt_id"]
    assert first["receipt_id"] == duplicate["receipt_id"]
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    assert harness.evidence_calls == 1
    connection.close()


def test_lost_response_restart_recovers_same_attempt_without_duplicate_effect(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    checkpoint_db = tmp_path / "checkpoints.db"
    config = _config("lost-response")
    harness.target_store.lose_response_once = True

    first_connection = _connection(checkpoint_db)
    graph = harness.graph(SqliteSaver(first_connection))
    graph.invoke(harness.state(), config)
    uncertain = graph.invoke(Command(resume=True), config)
    assert uncertain["phase"] == "awaiting_recovery"
    assert uncertain["decision_code"] == "TARGET_OUTCOME_INDETERMINATE"
    assert "__interrupt__" in uncertain
    attempt_id = uncertain["attempt_id"]
    first_connection.close()

    harness.restart_runtime()
    second_connection = _connection(checkpoint_db)
    recovered = harness.graph(SqliteSaver(second_connection)).invoke(Command(resume=True), config)
    assert recovered["phase"] == "committed"
    assert recovered["attempt_id"] == attempt_id
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 2
    assert harness.evidence_calls == 1
    second_connection.close()


def test_result_persistence_failure_after_commit_enters_same_attempt_recovery(
    tmp_path: Path,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("result-persistence-failure")
    original_record = harness.runtime.record_fenced_result

    graph.invoke(harness.state(), config)

    def fail_result_persistence(_result: FencedResult) -> None:
        raise OSError("simulated local result persistence failure")

    monkeypatch.setattr(harness.runtime, "record_fenced_result", fail_result_persistence)
    uncertain = graph.invoke(Command(resume=True), config)

    assert uncertain["phase"] == "awaiting_recovery"
    assert uncertain["decision_code"] == "TARGET_OUTCOME_INDETERMINATE"
    assert "simulated" not in repr(uncertain)
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    attempt = harness.runtime.get_fenced_attempt_for_lease(uncertain["lease_id"])
    assert attempt is not None
    assert attempt.status.value == "prepared"

    monkeypatch.setattr(harness.runtime, "record_fenced_result", original_record)
    committed = graph.invoke(Command(resume=True), config)

    assert committed["phase"] == "committed"
    assert committed["attempt_id"] == attempt.attempt_id
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 2
    connection.close()


def test_persisted_receipt_returns_without_redundant_attempt_read(
    tmp_path: Path,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("receipt-short-circuit")
    original_get = harness.runtime.get_fenced_attempt_for_lease
    reads = 0

    def fail_redundant_read(lease_id: str) -> FencedAttempt | None:
        nonlocal reads
        reads += 1
        if reads > 1:
            raise OSError("unexpected read after persisted receipt")
        return original_get(lease_id)

    graph.invoke(harness.state(), config)
    monkeypatch.setattr(
        harness.runtime,
        "get_fenced_attempt_for_lease",
        fail_redundant_read,
    )
    committed = graph.invoke(Command(resume=True), config)

    assert committed["phase"] == "committed"
    assert committed["decision_code"] == "FENCED_COMMIT_RECORDED"
    assert reads == 1
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_post_execution_state_read_failure_remains_retryable_and_redacted(
    tmp_path: Path,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("post-execution-state-unavailable")
    original_get = harness.runtime.get_fenced_attempt_for_lease
    reads = 0

    def fail_result_persistence(_result: FencedResult) -> None:
        raise OSError("sensitive persistence detail")

    def fail_post_execution_read(lease_id: str) -> FencedAttempt | None:
        nonlocal reads
        reads += 1
        if reads > 1:
            raise OSError("sensitive state-read detail")
        return original_get(lease_id)

    graph.invoke(harness.state(), config)
    monkeypatch.setattr(harness.runtime, "record_fenced_result", fail_result_persistence)
    monkeypatch.setattr(
        harness.runtime,
        "get_fenced_attempt_for_lease",
        fail_post_execution_read,
    )

    with pytest.raises(
        RuntimeError,
        match="post-execution durable state is unavailable; retry this workflow identity",
    ) as captured:
        graph.invoke(Command(resume=True), config)

    assert "sensitive" not in str(captured.value)
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_execution_exception_recovers_an_already_committed_durable_receipt(
    tmp_path: Path,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))

    graph.invoke(harness.state(), _config("original-commit"))
    original = graph.invoke(Command(resume=True), _config("original-commit"))
    assert original["phase"] == "committed"

    def fail_before_receipt_return(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated helper failure")

    monkeypatch.setattr(
        langgraph_integration,
        "execute_fenced",
        fail_before_receipt_return,
    )
    graph.invoke(harness.state(), _config("committed-recovery"))
    recovered = graph.invoke(Command(resume=True), _config("committed-recovery"))

    assert recovered["phase"] == "committed"
    assert recovered["attempt_id"] == original["attempt_id"]
    assert recovered["receipt_id"] == original["receipt_id"]
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_execution_failure_before_attempt_has_stable_terminal_code(
    tmp_path: Path,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("pre-attempt-target-failure")

    def fail_before_prepare(*_args: object, **_kwargs: object) -> None:
        raise OSError("sensitive target detail")

    graph.invoke(harness.state(), config)
    monkeypatch.setattr(langgraph_integration, "execute_fenced", fail_before_prepare)
    failed = graph.invoke(Command(resume=True), config)

    assert failed["phase"] == "reconcile"
    assert failed["decision_code"] == "TARGET_EXECUTION_FAILED"
    assert "sensitive" not in repr(failed)
    assert harness.target_store.dispatch_count == 0
    connection.close()


@pytest.mark.parametrize("recovery_response", [True, False, "invalid"])
def test_newer_terminal_truth_overrides_a_stale_recovery_response(
    tmp_path: Path,
    clock: ManualClock,
    recovery_response: object,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("recovery-basis-change")
    harness.target_store.lose_response_once = True

    graph.invoke(harness.state(), config)
    uncertain = graph.invoke(Command(resume=True), config)
    assert uncertain["phase"] == "awaiting_recovery"
    original_basis = uncertain["recovery_basis_digest"]

    resolved_receipt = execute_fenced(
        harness.runtime,
        _DeduplicatingTarget(harness.target_store),
        lease_id=uncertain["lease_id"],
        effect=harness.effects[harness.effect_ref],
        target_precondition=harness.preconditions[harness.precondition_ref],
    )
    assert resolved_receipt is not None
    assert harness.target_store.dispatch_count == 2
    del harness.effects[harness.effect_ref]

    committed = graph.invoke(Command(resume=recovery_response), config)
    assert committed["phase"] == "committed"
    assert committed["recovery_approved"] is False
    assert committed["recovery_basis_digest"] == ""
    assert original_basis != ""
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 2
    connection.close()


def test_changed_ambiguous_result_requires_a_new_recovery_decision(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    harness.target_store.lose_response_once = True
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("changed-ambiguous-result")

    graph.invoke(harness.state(), config)
    uncertain = graph.invoke(Command(resume=True), config)
    original_basis = uncertain["recovery_basis_digest"]
    attempt_id = uncertain["attempt_id"]
    target_result = harness.target_store.committed[attempt_id]
    changed_result = FencedResult(
        attempt_id=target_result.attempt_id,
        lease_id=target_result.lease_id,
        adapter_id=target_result.adapter_id,
        outcome=FencedOutcome.INDETERMINATE,
        request_digest=target_result.request_digest,
        closure_digest=target_result.closure_digest,
        effect_digest=target_result.effect_digest,
        target=target_result.target,
        operation=target_result.operation,
        precondition_digest=target_result.precondition_digest,
        scope_digest=target_result.scope_digest,
        replay_assurance=target_result.replay_assurance,
        coverage=target_result.coverage,
        target_result_digest=digest_json(
            {"attempt_id": attempt_id, "evidence": "changed"},
            domain="test:changed-ambiguous-result",
        ),
    )
    harness.runtime.record_fenced_result(changed_result)

    reprompted = graph.invoke(Command(resume=True), config)

    assert reprompted["phase"] == "awaiting_recovery"
    assert reprompted["decision_code"] == "TARGET_OUTCOME_INDETERMINATE"
    assert reprompted["recovery_basis_digest"] != original_basis
    assert "__interrupt__" in reprompted
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_crash_after_indeterminate_record_requires_recovery_before_redispatch(
    tmp_path: Path,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    checkpoint_db = tmp_path / "checkpoints.db"
    config = _config("crash-after-indeterminate-record")
    harness.target_store.lose_response_once = True
    connection = _connection(checkpoint_db)
    graph = harness.graph(SqliteSaver(connection))
    graph.invoke(harness.state(), config)

    original_get = harness.runtime.get_fenced_attempt_for_lease
    calls = 0

    def crash_on_post_dispatch_read(lease_id: str) -> FencedAttempt | None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise SystemExit("simulated process death before graph checkpoint")
        return original_get(lease_id)

    monkeypatch.setattr(
        harness.runtime,
        "get_fenced_attempt_for_lease",
        crash_on_post_dispatch_read,
    )
    with pytest.raises(SystemExit, match="before graph checkpoint"):
        graph.invoke(Command(resume=True), config)
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()

    monkeypatch.setattr(harness.runtime, "get_fenced_attempt_for_lease", original_get)
    harness.restart_runtime()
    restarted_connection = _connection(checkpoint_db)
    restarted = harness.graph(SqliteSaver(restarted_connection))
    gated = _restart_failed_node(restarted, config)

    assert gated["phase"] == "awaiting_recovery"
    assert gated["decision_code"] == "TARGET_OUTCOME_INDETERMINATE"
    assert gated["recovery_approved"] is False
    assert "__interrupt__" in gated
    assert harness.target_store.dispatch_count == 1

    committed = restarted.invoke(Command(resume=True), config)
    assert committed["phase"] == "committed"
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 2
    restarted_connection.close()


def test_crash_after_target_commit_before_local_record_requires_recovery_gate(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    checkpoint_db = tmp_path / "checkpoints.db"
    config = _config("crash-after-target-commit")
    harness.target_store.hard_crash_after_commit_once = True
    connection = _connection(checkpoint_db)
    graph = harness.graph(SqliteSaver(connection))
    graph.invoke(harness.state(), config)

    with pytest.raises(SystemExit, match="after target commit"):
        graph.invoke(Command(resume=True), config)
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()

    harness.restart_runtime()
    restarted_connection = _connection(checkpoint_db)
    restarted = harness.graph(SqliteSaver(restarted_connection))
    gated = _restart_failed_node(restarted, config)

    assert gated["phase"] == "awaiting_recovery"
    assert gated["decision_code"] == "TARGET_OUTCOME_INDETERMINATE"
    assert gated["recovery_approved"] is False
    assert "__interrupt__" in gated
    assert harness.target_store.dispatch_count == 1

    committed = restarted.invoke(Command(resume=True), config)
    assert committed["phase"] == "committed"
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 2
    restarted_connection.close()


def test_unknown_dependency_completeness_never_reaches_target(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(
        tmp_path / "truthlease.db",
        clock,
        completeness=DependencyCompleteness.UNKNOWN,
    )
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("unknown-completeness")

    graph.invoke(harness.state(), config)
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "DEPENDENCY_COMPLETENESS_INSUFFICIENT"
    assert harness.target_store.effect_count == 0
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_reference_change_after_approval_fails_before_lease_or_target(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("effect-change")

    graph.invoke(harness.state(), config)
    harness.effects[harness.effect_ref] = {"payload": "changed-after-approval", "write": True}
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "EFFECT_REFERENCE_CHANGED"
    assert harness.target_store.effect_count == 0
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_missing_effect_reference_has_distinct_redacted_code(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("missing-effect-reference")

    graph.invoke(harness.state(), config)
    del harness.effects[harness.effect_ref]
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "EFFECT_REFERENCE_UNAVAILABLE"
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_missing_precondition_reference_has_distinct_redacted_code(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("missing-precondition-reference")

    graph.invoke(harness.state(), config)
    del harness.preconditions[harness.precondition_ref]
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "PRECONDITION_REFERENCE_UNAVAILABLE"
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_same_idempotency_key_with_different_intent_fails_closed(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))

    graph.invoke(harness.state(), _config("original"))
    original = graph.invoke(Command(resume=True), _config("original"))
    assert original["phase"] == "committed"

    changed_ref = "effect/order/v2"
    harness.effects[changed_ref] = {"payload": "different-effect", "write": True}
    graph.invoke(harness.state(effect_ref=changed_ref), _config("mismatch"))
    mismatch = graph.invoke(Command(resume=True), _config("mismatch"))

    assert mismatch["phase"] == "reconcile"
    assert mismatch["decision_code"] == "CONFLICT"
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_non_boolean_approval_is_rejected_without_side_effect(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("invalid-approval")

    graph.invoke(harness.state(), config)
    result = graph.invoke(Command(resume="yes"), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "APPROVAL_RESPONSE_INVALID"
    assert harness.target_store.effect_count == 0
    connection.close()


def test_denied_approval_is_terminal_without_authorization(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("denied-approval")

    graph.invoke(harness.state(), config)
    result = graph.invoke(Command(resume=False), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "APPROVAL_DENIED"
    with harness.runtime.store.transaction(write=False) as tx:
        assert tx.connection.execute("SELECT COUNT(*) FROM leases").fetchone()[0] == 0
    assert harness.target_store.effect_count == 0
    connection.close()


def test_precondition_reference_change_after_approval_fails_closed(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("precondition-change")

    graph.invoke(harness.state(), config)
    harness.preconditions[harness.precondition_ref] = TargetPrecondition(
        adapter_id="adapter:test",
        mechanism="strong-etag",
        expected_token="changed-precondition-token",
        protected_observations=("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "PRECONDITION_REFERENCE_CHANGED"
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_authoritative_revalidation_failure_is_redacted_and_fails_closed(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    harness.revalidation_failure = True
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("revalidation-failure")

    graph.invoke(harness.state(), config)
    result = graph.invoke(Command(resume=True), config)

    assert result["phase"] == "reconcile"
    assert result["decision_code"] == "AUTHORITATIVE_REVALIDATION_FAILED"
    assert "simulated" not in repr(result)
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_indeterminate_recovery_can_be_declined_without_redispatch(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    harness.target_store.lose_response_once = True
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("decline-recovery")

    graph.invoke(harness.state(), config)
    graph.invoke(Command(resume=True), config)
    declined = graph.invoke(Command(resume=False), config)

    assert declined["phase"] == "reconcile"
    assert declined["decision_code"] == "RECOVERY_DECLINED"
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_non_boolean_recovery_response_is_rejected_without_redispatch(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    harness.target_store.lose_response_once = True
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("invalid-recovery-response")

    graph.invoke(harness.state(), config)
    graph.invoke(Command(resume=True), config)
    rejected = graph.invoke(Command(resume="retry"), config)

    assert rejected["phase"] == "reconcile"
    assert rejected["decision_code"] == "RECOVERY_RESPONSE_INVALID"
    assert harness.target_store.effect_count == 1
    assert harness.target_store.dispatch_count == 1
    connection.close()


def test_evidence_provider_failure_is_redacted_before_target_dispatch(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    harness.evidence_failure = True
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))
    config = _config("evidence-provider-failure")

    graph.invoke(harness.state(), config)
    failed = graph.invoke(Command(resume=True), config)

    assert failed["phase"] == "reconcile"
    assert failed["decision_code"] == "EVIDENCE_PROVIDER_FAILED"
    assert "simulated" not in repr(failed)
    assert harness.evidence_calls == 1
    assert harness.target_store.dispatch_count == 0
    connection.close()


def test_public_builders_reject_invalid_ttl_references_and_digest(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    saver = SqliteSaver(connection)

    with pytest.raises(ValueError, match="lease_ttl"):
        build_stale_safe_graph(
            harness.runtime,
            checkpointer=saver,
            revalidate=harness.revalidate,
            resolve_effect=harness.resolve_effect,
            resolve_precondition=harness.resolve_precondition,
            evidence_provider=harness.evidence,
            target_adapter=_DeduplicatingTarget(harness.target_store),
            lease_ttl=timedelta(0),
        )
    with pytest.raises(ValueError, match="effect_ref"):
        make_resume_state(
            idempotency_key=harness.idempotency_key,
            artifact=harness.artifact,
            effect_ref="",
            effect=harness.effects[harness.effect_ref],
            precondition_ref=harness.precondition_ref,
            target_precondition=harness.preconditions[harness.precondition_ref],
            target=_TARGET,
            operation=_OPERATION,
        )
    with pytest.raises(ValueError, match="idempotency_key"):
        make_resume_state(
            idempotency_key="not-a-digest",
            artifact=harness.artifact,
            effect_ref=harness.effect_ref,
            effect=harness.effects[harness.effect_ref],
            precondition_ref=harness.precondition_ref,
            target_precondition=harness.preconditions[harness.precondition_ref],
            target=_TARGET,
            operation=_OPERATION,
        )
    connection.close()


def test_tampered_checkpoint_fields_fail_closed_before_interrupt_or_effect(
    tmp_path: Path,
    clock: ManualClock,
) -> None:
    harness = _Harness(tmp_path / "truthlease.db", clock)
    connection = _connection(tmp_path / "checkpoints.db")
    graph = harness.graph(SqliteSaver(connection))

    malformed_states: list[StaleSafeState] = []
    wrong_schema = harness.state()
    wrong_schema["schema_version"] = 2
    malformed_states.append(wrong_schema)
    wrong_phase = harness.state()
    wrong_phase["phase"] = "approved"
    malformed_states.append(wrong_phase)
    wrong_revision = harness.state()
    wrong_revision["artifact_revision"] = 0
    malformed_states.append(wrong_revision)
    empty_reference = harness.state()
    empty_reference["effect_ref"] = ""
    malformed_states.append(empty_reference)
    invalid_digest = harness.state()
    invalid_digest["effect_digest"] = "not-a-digest"
    malformed_states.append(invalid_digest)

    for index, state in enumerate(malformed_states):
        result = graph.invoke(state, _config(f"tampered-{index}"))
        assert result["phase"] == "reconcile"
        assert result["decision_code"] == "CHECKPOINT_STATE_INVALID"
        assert "__interrupt__" not in result

    assert harness.target_store.dispatch_count == 0
    connection.close()
