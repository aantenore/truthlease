from __future__ import annotations

import json
import socket
from _thread import LockType
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any

import pytest
from conftest import ManualClock, SequentialIds

from truthlease.canonical import Digestable, canonical_bytes, digest_json
from truthlease.engine import TruthLease
from truthlease.models import (
    ArtifactDependency,
    AssuranceEvidence,
    AssuranceLevel,
    DependencyCompleteness,
    FenceCoverage,
    FencedAttemptStatus,
    FencedCommitRequest,
    FencedOutcome,
    ReplayAssurance,
    TargetPrecondition,
)
from truthlease.storage.sqlite import SQLiteStore
from truthlease.target import StrictHttpAdapter, execute_fenced


@dataclass
class _TargetState:
    lock: LockType = field(default_factory=Lock)
    etag: str = '"v1"'
    effects: int = 0
    mode: str = "normal"
    lose_first_response: bool = False
    raw_requests: list[bytes] = field(default_factory=list)
    attempt_headers: list[str] = field(default_factory=list)
    if_match_headers: list[str] = field(default_factory=list)
    results: dict[str, tuple[str, bytes]] = field(default_factory=dict)


class _TargetServer(ThreadingHTTPServer):
    daemon_threads = True


def _handler_for(state: _TargetState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            state.raw_requests.append(body)
            attempt_id = self.headers.get("Idempotency-Key", "")
            if_match = self.headers.get("If-Match", "")
            state.attempt_headers.append(attempt_id)
            state.if_match_headers.append(if_match)

            if state.mode == "redirect":
                self.send_response(307)
                self.send_header("Location", "/commit")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if state.mode.startswith("status:"):
                self._empty(int(state.mode.partition(":")[2]))
                return
            if state.mode == "large":
                self._json(200, {"padding": "x" * 512})
                return
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                self._empty(400)
                return
            if not isinstance(payload, dict):
                self._empty(400)
                return
            if state.mode.startswith("corrupt:"):
                field_name = state.mode.partition(":")[2]
                response = self._binding(payload, receipt_ref="target-receipt-corrupt")
                if field_name == "receipt_ref":
                    response.pop(field_name)
                else:
                    response[field_name] = "wrong"
                self._json(200, response)
                return
            if state.mode.startswith("time:"):
                variant = state.mode.partition(":")[2]
                response = self._binding(payload, receipt_ref="target-receipt-time")
                if variant == "missing":
                    response.pop("committed_at")
                elif variant == "malformed":
                    response["committed_at"] = "not-a-time"
                elif variant == "naive":
                    response["committed_at"] = "2026-07-20T12:00:01"
                self._json(200, response)
                return
            if state.mode == "malformed":
                self._json(200, {"attempt_id": attempt_id})
                return

            request_digest = payload.get("request_digest")
            if not isinstance(request_digest, str):
                self._empty(400)
                return
            lose_response = False
            status = 200
            response_body = b""
            with state.lock:
                previous = state.results.get(attempt_id)
                if previous is not None:
                    previous_digest, response_body = previous
                    if previous_digest != request_digest:
                        status = 409
                elif if_match != state.etag:
                    status = 412
                else:
                    state.effects += 1
                    state.etag = f'"v{state.effects + 1}"'
                    response = self._binding(
                        payload,
                        receipt_ref=f"target-receipt-{state.effects}",
                    )
                    response_body = canonical_bytes(response)
                    state.results[attempt_id] = (request_digest, response_body)
                    if state.lose_first_response:
                        state.lose_first_response = False
                        lose_response = True
            if lose_response:
                self.close_connection = True
                with suppress(OSError):
                    self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if status != 200:
                self._empty(status)
                return
            self._bytes(200, response_body, "application/json")

        def _binding(self, payload: dict[str, Any], *, receipt_ref: str) -> dict[str, Any]:
            return {
                "attempt_id": payload.get("attempt_id"),
                "request_digest": payload.get("request_digest"),
                "precondition_digest": payload.get("precondition_digest"),
                "scope_digest": payload.get("scope_digest"),
                "receipt_ref": receipt_ref,
                "committed_at": "2026-07-20T12:00:01Z",
            }

        def _json(self, status: int, payload: dict[str, Any]) -> None:
            self._bytes(status, canonical_bytes(payload), "application/json")

        def _empty(self, status: int) -> None:
            self._bytes(status, b"", "application/octet-stream")

        def _bytes(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if body:
                self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


@contextmanager
def _serve(state: _TargetState) -> Iterator[str]:
    server = _TargetServer(("127.0.0.1", 0), _handler_for(state))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/commit"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _request(
    target: str,
    *,
    expected_token: str = '"v1"',
    attempt_id: str = "attempt_0001",
    request_digest: str = "sha256:request-one",
) -> FencedCommitRequest:
    prepared_at = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
    effect: dict[str, Digestable] = {"action": "publish", "revision": 1}
    precondition = TargetPrecondition(
        adapter_id="adapter:http",
        mechanism="http-if-match",
        expected_token=expected_token,
        protected_observations=("policy",),
        replay_assurance=ReplayAssurance.DEDUPLICATED,
    )
    return FencedCommitRequest(
        attempt_id=attempt_id,
        lease_id="lease_0001",
        gate_receipt_id="receipt_0001",
        adapter_id="adapter:http",
        effect=effect,
        target=target,
        operation="publish",
        closure_digest="sha256:closure",
        effect_digest=digest_json(effect, domain=f"effect:publish:{target}"),
        precondition=precondition,
        precondition_digest="sha256:precondition",
        scope_digest="sha256:scope",
        request_digest=request_digest,
        coverage=FenceCoverage.FULL,
        prepared_at=prepared_at,
        expires_at=prepared_at + timedelta(seconds=30),
    )


def _adapter(target: str, **kwargs: Any) -> StrictHttpAdapter:
    return StrictHttpAdapter(
        adapter_id="adapter:http",
        allowed_targets=(target,),
        allow_insecure_localhost=True,
        **kwargs,
    )


def test_success_requires_bound_json_and_sends_canonical_request() -> None:
    state = _TargetState()
    with _serve(state) as target:
        request = _request(target)
        result = _adapter(target).commit(request)

    assert result.outcome is FencedOutcome.COMMITTED
    assert result.target_receipt_ref == "target-receipt-1"
    assert result.committed_at is not None
    assert state.effects == 1
    assert state.attempt_headers == [request.attempt_id]
    assert state.if_match_headers == [request.precondition.expected_token]
    decoded = json.loads(state.raw_requests[0])
    assert state.raw_requests[0] == canonical_bytes(decoded)
    assert decoded["effect"] == request.effect
    assert decoded["request_digest"] == request.request_digest


def test_precondition_mismatch_is_certainly_not_committed_and_weak_etag_is_rejected() -> None:
    state = _TargetState()
    with _serve(state) as target:
        adapter = _adapter(target)
        mismatch = adapter.commit(_request(target, expected_token='"stale"'))
        with pytest.raises(ValueError, match="strong quoted ETag"):
            adapter.commit(_request(target, expected_token='W/"v1"'))

    assert mismatch.outcome is FencedOutcome.NOT_COMMITTED
    assert state.effects == 0


def test_mutated_nested_effect_is_rejected_before_dispatch() -> None:
    state = _TargetState()
    with _serve(state) as target:
        request = _request(target)
        assert isinstance(request.effect, dict)
        request.effect["revision"] = 2
        with pytest.raises(ValueError, match="raw effect changed"):
            _adapter(target).commit(request)

    assert state.raw_requests == []
    assert state.effects == 0


def test_lost_response_retries_same_attempt_without_duplicate_effect() -> None:
    state = _TargetState(lose_first_response=True)
    with _serve(state) as target:
        request = _request(target)
        adapter = _adapter(target)
        first = adapter.commit(request)
        second = adapter.commit(request)

    assert first.outcome is FencedOutcome.INDETERMINATE
    assert second.outcome is FencedOutcome.COMMITTED
    assert second.target_receipt_ref == "target-receipt-1"
    assert state.effects == 1
    assert state.attempt_headers == [request.attempt_id, request.attempt_id]


def test_full_recovery_reuses_durable_attempt_and_records_one_fenced_receipt(
    db_path: Path,
    clock: ManualClock,
) -> None:
    state = _TargetState(lose_first_response=True)
    with _serve(state) as target:
        runtime = TruthLease(
            SQLiteStore(db_path),
            clock=clock,
            ids=SequentialIds(),
            trusted_evidence_authorities={"evidence:http"},
            trusted_target_adapters={"adapter:http"},
        )
        runtime.observe(key="policy", version="v1", value={"enabled": True})
        artifact = runtime.publish_artifact(
            artifact_id="plan",
            kind="plan",
            payload={"step": 1},
            dependencies=[ArtifactDependency.observation(runtime.get_observation("policy"))],
            completeness=DependencyCompleteness.OBSERVED,
        )
        effect: dict[str, Digestable] = {"action": "publish", "revision": 1}
        precondition = TargetPrecondition(
            adapter_id="adapter:http",
            mechanism="http-if-match",
            expected_token='"v1"',
            protected_observations=("policy",),
            replay_assurance=ReplayAssurance.DEDUPLICATED,
        )
        lease = runtime.acquire_lease(
            artifact=artifact.ref,
            effect=effect,
            target=target,
            operation="publish",
            required_assurance=AssuranceLevel.FENCED,
            target_precondition=precondition,
        )
        evidence = AssuranceEvidence(
            level=AssuranceLevel.REVALIDATED,
            authority="evidence:http",
            evidence_digest="sha256:evidence",
            observed_at=clock.now(),
            lease_id=lease.lease_id,
            closure_digest=lease.closure_digest,
            effect_digest=lease.effect_digest,
            target=lease.target,
            operation=lease.operation,
        )
        adapter = _adapter(target)

        first = execute_fenced(
            runtime,
            adapter,
            lease_id=lease.lease_id,
            effect=effect,
            target_precondition=precondition,
            evidence=evidence,
        )
        attempt_id = state.attempt_headers[0]
        first_attempt = runtime.get_fenced_attempt(attempt_id)
        second = execute_fenced(
            runtime,
            adapter,
            lease_id=lease.lease_id,
            effect=effect,
            target_precondition=precondition,
            evidence=evidence,
        )

    assert first is None
    assert first_attempt is not None
    assert first_attempt.status is FencedAttemptStatus.INDETERMINATE
    assert second is not None
    assert second.attempt_id == attempt_id
    assert second.assurance is AssuranceLevel.FENCED
    assert state.effects == 1
    assert state.attempt_headers == [attempt_id, attempt_id]


def test_same_attempt_with_changed_fingerprint_is_indeterminate_and_not_reapplied() -> None:
    state = _TargetState()
    with _serve(state) as target:
        adapter = _adapter(target)
        request = _request(target)
        assert adapter.commit(request).outcome is FencedOutcome.COMMITTED
        changed_effect: dict[str, Digestable] = {"action": "publish", "revision": 2}
        changed = replace(
            request,
            request_digest="sha256:different-request",
            effect=changed_effect,
            effect_digest=digest_json(
                changed_effect,
                domain=f"effect:{request.operation}:{request.target}",
            ),
        )
        conflict = adapter.commit(changed)

    assert conflict.outcome is FencedOutcome.INDETERMINATE
    assert state.effects == 1


@pytest.mark.parametrize(
    "mode",
    ["status:409", "status:500", "redirect", "malformed", "large"],
)
def test_ambiguous_status_redirect_and_malformed_or_oversized_success_are_indeterminate(
    mode: str,
) -> None:
    state = _TargetState(mode=mode)
    with _serve(state) as target:
        result = _adapter(target, max_response_bytes=128).commit(_request(target))

    assert result.outcome is FencedOutcome.INDETERMINATE
    assert state.effects == 0


@pytest.mark.parametrize(
    "field_name",
    [
        "attempt_id",
        "request_digest",
        "precondition_digest",
        "scope_digest",
        "receipt_ref",
        "committed_at",
    ],
)
def test_each_success_binding_is_mandatory(field_name: str) -> None:
    state = _TargetState(mode=f"corrupt:{field_name}")
    with _serve(state) as target:
        result = _adapter(target).commit(_request(target))

    assert result.outcome is FencedOutcome.INDETERMINATE


@pytest.mark.parametrize("variant", ["missing", "malformed", "naive"])
def test_target_reported_commit_time_must_be_rfc3339_and_timezone_aware(variant: str) -> None:
    state = _TargetState(mode=f"time:{variant}")
    with _serve(state) as target:
        result = _adapter(target).commit(_request(target))

    assert result.outcome is FencedOutcome.INDETERMINATE


def test_targets_are_allowlisted_exactly_and_plain_http_requires_loopback_opt_in() -> None:
    local = "http://127.0.0.1:9999/commit"
    with pytest.raises(ValueError, match="loopback"):
        StrictHttpAdapter(adapter_id="adapter:http", allowed_targets=(local,))
    adapter = StrictHttpAdapter(
        adapter_id="adapter:http",
        allowed_targets=(local,),
        allow_insecure_localhost=True,
    )
    with pytest.raises(ValueError, match="not allowlisted"):
        adapter.commit(_request("http://127.0.0.1:9999/other"))
    with pytest.raises(ValueError, match="HTTPS"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=("ftp://example.test/commit",),
        )


def test_header_injection_reserved_overrides_and_configuration_bounds_are_rejected() -> None:
    target = "https://example.test/commit"
    with pytest.raises(ValueError, match="reserved"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            headers={"If-Match": '"override"'},
        )
    with pytest.raises(ValueError, match="reserved"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            headers={"Transfer-Encoding": "chunked"},
        )
    with pytest.raises(ValueError, match="CR or LF"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            headers={"Authorization": "safe\r\nInjected: yes"},
        )
    with pytest.raises(ValueError, match="timeout_seconds"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            timeout_seconds=float("inf"),
        )
    with pytest.raises(ValueError, match="max_response_bytes"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            max_response_bytes=0,
        )
    with pytest.raises(ValueError, match="max_request_bytes"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            max_request_bytes=0,
        )


def test_oversized_request_is_certainly_not_dispatched() -> None:
    target = "https://example.test/commit"
    result = StrictHttpAdapter(
        adapter_id="adapter:http",
        allowed_targets=(target,),
        max_request_bytes=1,
    ).commit(_request(target))

    assert result.outcome is FencedOutcome.NOT_COMMITTED


def test_http_adapter_rejects_ambiguous_or_duplicate_configuration() -> None:
    target = "https://example.test/commit"
    with pytest.raises(ValueError, match="non-empty"):
        StrictHttpAdapter(adapter_id="", allowed_targets=(target,))
    with pytest.raises(ValueError, match="non-empty collection"):
        StrictHttpAdapter(adapter_id="adapter:http", allowed_targets=())
    with pytest.raises(ValueError, match="non-empty collection"):
        StrictHttpAdapter(adapter_id="adapter:http", allowed_targets=target)
    with pytest.raises(ValueError, match="duplicates"):
        StrictHttpAdapter(adapter_id="adapter:http", allowed_targets=(target, target))
    with pytest.raises(ValueError, match="method"):
        StrictHttpAdapter(adapter_id="adapter:http", allowed_targets=(target,), method="GET")
    with pytest.raises(ValueError, match="timeout_seconds"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            timeout_seconds=-1,
        )
    with pytest.raises(ValueError, match="max_response_bytes"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            max_response_bytes=2 * 1024 * 1024,
        )
    with pytest.raises(ValueError, match="duplicate header"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            headers={"X-Trace": "one", "x-trace": "two"},
        )
    with pytest.raises(ValueError, match="invalid HTTP header"):
        StrictHttpAdapter(
            adapter_id="adapter:http",
            allowed_targets=(target,),
            headers={"Bad Header": "value"},
        )


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("https://example.test/commit\nInjected", "control"),
        ("https://user:secret@example.test/commit", "credentials"),
        ("https:///commit", "host"),
        ("https://example.test/commit#fragment", "fragment"),
        ("https://example.test:invalid/commit", "invalid port"),
        ("http://example.test/commit", "loopback"),
    ],
)
def test_target_url_validation_fails_closed(target: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        StrictHttpAdapter(adapter_id="adapter:http", allowed_targets=(target,))


def test_request_adapter_and_mechanism_must_match_profile() -> None:
    target = "https://example.test/commit"
    adapter = _adapter(target)
    request = _request(target)
    with pytest.raises(ValueError, match="adapter_id"):
        adapter.commit(
            replace(
                request,
                adapter_id="adapter:other",
                precondition=replace(request.precondition, adapter_id="adapter:other"),
            )
        )
    with pytest.raises(ValueError, match="http-if-match"):
        adapter.commit(
            replace(
                request,
                precondition=replace(request.precondition, mechanism="sql-cas"),
            )
        )


def test_localhost_name_is_accepted_only_with_explicit_insecure_opt_in() -> None:
    target = "http://localhost:9999/commit"
    adapter = StrictHttpAdapter(
        adapter_id="adapter:http",
        allowed_targets=(target,),
        allow_insecure_localhost=True,
    )
    assert target in adapter.allowed_targets
