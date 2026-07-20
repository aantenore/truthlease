"""Target-side fencing adapters and the two-phase execution helper.

The strict HTTP adapter is intentionally a conformance profile for a
cooperating endpoint.  It never infers a fenced commit from a successful HTTP
status alone.
"""

from __future__ import annotations

import ipaddress
import json
import math
import re
import urllib.error
import urllib.request
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from email.message import Message
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import SplitResult, urlsplit

from truthlease.canonical import Digestable, canonical_bytes, digest_json
from truthlease.clock import parse_utc, utc_text
from truthlease.errors import ConflictError
from truthlease.models import (
    AssuranceEvidence,
    FenceCoverage,
    FencedAttemptStatus,
    FencedCommitRequest,
    FencedOutcome,
    FencedReceipt,
    FencedResult,
    ReplayAssurance,
    TargetPrecondition,
)

if TYPE_CHECKING:
    from http.client import HTTPMessage, HTTPResponse

    from truthlease.engine import TruthLease

_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_STRONG_ETAG = re.compile(r'^"[\x21\x23-\x7e\x80-\xff]*"$')
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
_RESERVED_HEADERS = frozenset(
    {
        "accept",
        "connection",
        "content-length",
        "content-type",
        "expect",
        "host",
        "idempotency-key",
        "if-match",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_MAX_CONFIGURED_TIMEOUT_SECONDS = 60.0
_MAX_CONFIGURED_REQUEST_BYTES = 1024 * 1024
_MAX_CONFIGURED_RESPONSE_BYTES = 1024 * 1024


@runtime_checkable
class TargetAdapter(Protocol):
    """A named target that atomically enforces its declared precondition.

    Implementations must independently recompute the raw effect digest before
    dispatch.  ``FencedCommitRequest`` is frozen, but nested JSON containers
    can still be mutable and must not be trusted merely because preparation
    succeeded earlier.
    """

    @property
    def adapter_id(self) -> str:
        """Stable identity allowlisted by the TruthLease runtime."""
        ...

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        """Attempt one bound target commit using ``request.attempt_id`` for replay."""
        ...


def execute_fenced(
    runtime: TruthLease,
    adapter: TargetAdapter,
    *,
    lease_id: str,
    effect: Digestable,
    target_precondition: TargetPrecondition,
    evidence: AssuranceEvidence | None = None,
) -> FencedReceipt | None:
    """Prepare locally, call the target outside SQLite, then record its result.

    Once preparation succeeds the attempt identity is immutable.  Adapter
    exceptions are therefore recorded as an indeterminate result for that same
    attempt; callers must never recover by acquiring a new attempt identity.
    """

    if adapter.adapter_id != target_precondition.adapter_id:
        raise ValueError("adapter_id does not match target_precondition before preparation")
    request = runtime.prepare_fenced_commit(
        lease_id=lease_id,
        effect=effect,
        target_precondition=target_precondition,
        evidence=evidence,
    )
    attempt = runtime.get_fenced_attempt(request.attempt_id)
    if attempt is None:
        raise RuntimeError("prepared fenced attempt is missing")
    if attempt.status is FencedAttemptStatus.COMMITTED:
        return runtime.get_fenced_receipt(request.attempt_id)
    if attempt.status is FencedAttemptStatus.NOT_COMMITTED:
        return None
    if attempt.status is FencedAttemptStatus.DISPUTED:
        raise ConflictError(
            "fenced attempt is disputed",
            details={"attempt_id": request.attempt_id},
        )
    try:
        if adapter.adapter_id != request.adapter_id:
            raise ValueError("adapter_id does not match the prepared request")
        result = adapter.commit(request)
    except Exception as error:
        result = _indeterminate_result(
            request,
            {
                "category": "adapter_exception",
                "type": f"{type(error).__module__}.{type(error).__qualname__}",
            },
        )
    if not isinstance(result, FencedResult) or (
        not isinstance(getattr(result, "outcome", None), FencedOutcome)
        or not isinstance(getattr(result, "replay_assurance", None), ReplayAssurance)
        or not isinstance(getattr(result, "coverage", None), FenceCoverage)
    ):
        result = _indeterminate_result(
            request,
            {
                "category": "invalid_adapter_result",
                "type": f"{type(result).__module__}.{type(result).__qualname__}",
            },
        )
    else:
        mismatches = _adapter_result_mismatches(request, result)
        if mismatches:
            result = _indeterminate_result(
                request,
                {
                    "category": "invalid_adapter_binding",
                    "mismatches": mismatches,
                },
            )
    return runtime.record_fenced_result(result)


def _adapter_result_mismatches(
    request: FencedCommitRequest,
    result: FencedResult,
) -> list[str]:
    expected: dict[str, object] = {
        "attempt_id": request.attempt_id,
        "lease_id": request.lease_id,
        "adapter_id": request.adapter_id,
        "request_digest": request.request_digest,
        "closure_digest": request.closure_digest,
        "effect_digest": request.effect_digest,
        "target": request.target,
        "operation": request.operation,
        "precondition_digest": request.precondition_digest,
        "scope_digest": request.scope_digest,
        "replay_assurance": request.precondition.replay_assurance,
        "coverage": request.coverage,
    }
    return sorted(name for name, value in expected.items() if getattr(result, name) != value)


@dataclass(frozen=True, slots=True)
class StrictHttpAdapter:
    """Strict, dependency-free HTTP target fencing conformance profile.

    ``request.target`` is the exact endpoint authorized by the lease.  HTTPS is
    mandatory except for an explicit loopback-only development opt-in.  A
    success must carry a target-reported RFC 3339 ``committed_at`` timestamp;
    it is audit evidence, not a synchronized-clock ordering guarantee.
    """

    adapter_id: str
    allowed_targets: Collection[str]
    method: str = "POST"
    timeout_seconds: float = 5.0
    max_request_bytes: int = 1024 * 1024
    max_response_bytes: int = 64 * 1024
    headers: Mapping[str, str] = field(default_factory=dict)
    allow_insecure_localhost: bool = False

    def __post_init__(self) -> None:
        _require_header_value("adapter_id", self.adapter_id)
        if isinstance(self.allowed_targets, str) or not self.allowed_targets:
            raise ValueError("allowed_targets must be a non-empty collection of exact URLs")
        targets: set[str] = set()
        for target in self.allowed_targets:
            _validated_target(target, self.allow_insecure_localhost)
            targets.add(target)
        if len(targets) != len(self.allowed_targets):
            raise ValueError("allowed_targets must not contain duplicates")
        method = self.method.upper()
        if method not in {"POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError("method must be POST, PUT, PATCH, or DELETE")
        if not math.isfinite(self.timeout_seconds) or not (
            0 < self.timeout_seconds <= _MAX_CONFIGURED_TIMEOUT_SECONDS
        ):
            raise ValueError("timeout_seconds must be finite and in the range (0, 60]")
        if not 0 < self.max_request_bytes <= _MAX_CONFIGURED_REQUEST_BYTES:
            raise ValueError("max_request_bytes must be in the range [1, 1048576]")
        if not 0 < self.max_response_bytes <= _MAX_CONFIGURED_RESPONSE_BYTES:
            raise ValueError("max_response_bytes must be in the range [1, 1048576]")
        normalized: dict[str, str] = {}
        for name, value in self.headers.items():
            _validate_header(name, value)
            lowered = name.lower()
            if lowered in _RESERVED_HEADERS:
                raise ValueError(f"header {name!r} is reserved by StrictHttpAdapter")
            if lowered in normalized:
                raise ValueError(f"duplicate header name {name!r}")
            normalized[lowered] = value
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "allowed_targets", frozenset(targets))
        object.__setattr__(self, "headers", MappingProxyType(normalized))

    def commit(self, request: FencedCommitRequest) -> FencedResult:
        """Execute one strong-ETag conditional request without redirecting."""

        if request.adapter_id != self.adapter_id:
            raise ValueError("request adapter_id does not match this adapter")
        if request.precondition.mechanism != "http-if-match":
            raise ValueError("StrictHttpAdapter requires mechanism 'http-if-match'")
        if request.target not in self.allowed_targets:
            raise ValueError("request target is not allowlisted by this adapter")
        current_effect_digest = digest_json(
            request.effect,
            domain=f"effect:{request.operation}:{request.target}",
        )
        if current_effect_digest != request.effect_digest:
            raise ValueError("raw effect changed after fenced commit preparation")
        etag = request.precondition.expected_token
        if not _STRONG_ETAG.fullmatch(etag):
            raise ValueError("If-Match requires a strong quoted ETag")
        _require_header_value("attempt_id", request.attempt_id)
        target = _validated_target(request.target, self.allow_insecure_localhost)
        payload = _request_payload(request)
        body = canonical_bytes(payload)
        if len(body) > self.max_request_bytes:
            return _result(
                request,
                FencedOutcome.NOT_COMMITTED,
                {
                    "category": "request_too_large",
                    "request_bytes": len(body),
                    "max_request_bytes": self.max_request_bytes,
                },
            )
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Idempotency-Key": request.attempt_id,
            "If-Match": etag,
            **dict(self.headers),
        }
        http_request = urllib.request.Request(
            target,
            data=body,
            headers=headers,
            method=self.method,
        )
        opener = urllib.request.build_opener(_NoRedirectHandler())
        try:
            with opener.open(http_request, timeout=self.timeout_seconds) as response:
                return self._from_response(request, response)
        except urllib.error.HTTPError as error:
            try:
                if error.code == 412:
                    return _result(
                        request,
                        FencedOutcome.NOT_COMMITTED,
                        {"category": "http_status", "status": 412},
                    )
                return _indeterminate_result(
                    request,
                    {"category": "http_status", "status": error.code},
                )
            finally:
                error.close()
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
            return _indeterminate_result(
                request,
                {
                    "category": "transport_error",
                    "type": f"{type(error).__module__}.{type(error).__qualname__}",
                },
            )

    def _from_response(
        self,
        request: FencedCommitRequest,
        response: HTTPResponse,
    ) -> FencedResult:
        status = int(response.status)
        if not 200 <= status < 300:
            return _indeterminate_result(
                request,
                {"category": "http_status", "status": status},
            )
        if response.geturl() != request.target:
            return _indeterminate_result(
                request,
                {"category": "redirected_response", "status": status},
            )
        if not _is_json_content_type(response.headers):
            return _indeterminate_result(
                request,
                {"category": "invalid_content_type", "status": status},
            )
        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                return _indeterminate_result(
                    request,
                    {"category": "invalid_content_length", "status": status},
                )
            if declared_length < 0 or declared_length > self.max_response_bytes:
                return _indeterminate_result(
                    request,
                    {"category": "response_too_large", "status": status},
                )
        body = response.read(self.max_response_bytes + 1)
        if len(body) > self.max_response_bytes:
            return _indeterminate_result(
                request,
                {"category": "response_too_large", "status": status},
            )
        try:
            decoded = body.decode("utf-8", errors="strict")
            value = json.loads(decoded, parse_constant=_reject_non_finite)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
            return _indeterminate_result(
                request,
                {"category": "malformed_json", "status": status},
            )
        if not isinstance(value, dict):
            return _indeterminate_result(
                request,
                {"category": "malformed_binding", "status": status},
            )
        expected = {
            "attempt_id": request.attempt_id,
            "request_digest": request.request_digest,
            "precondition_digest": request.precondition_digest,
            "scope_digest": request.scope_digest,
        }
        if any(value.get(name) != expected_value for name, expected_value in expected.items()):
            return _indeterminate_result(
                request,
                {"category": "malformed_binding", "status": status},
            )
        if "outcome" in value and value["outcome"] != FencedOutcome.COMMITTED.value:
            return _indeterminate_result(
                request,
                {"category": "malformed_binding", "status": status},
            )
        receipt_ref = value.get("receipt_ref")
        if (
            not isinstance(receipt_ref, str)
            or not receipt_ref.strip()
            or len(receipt_ref) > 2048
            or "\r" in receipt_ref
            or "\n" in receipt_ref
        ):
            return _indeterminate_result(
                request,
                {"category": "malformed_receipt_ref", "status": status},
            )
        committed_at_text = value.get("committed_at")
        if not isinstance(committed_at_text, str) or not _RFC3339.fullmatch(committed_at_text):
            return _indeterminate_result(
                request,
                {"category": "malformed_committed_at", "status": status},
            )
        try:
            committed_at = parse_utc(committed_at_text)
        except (ValueError, OverflowError):
            return _indeterminate_result(
                request,
                {"category": "malformed_committed_at", "status": status},
            )
        binding: dict[str, Digestable] = {
            "http_status": status,
            **expected,
            "receipt_ref": receipt_ref,
            "committed_at": utc_text(committed_at),
        }
        return _result(
            request,
            FencedOutcome.COMMITTED,
            binding,
            target_receipt_ref=receipt_ref,
            committed_at=committed_at,
        )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        return None


def _request_payload(request: FencedCommitRequest) -> dict[str, Digestable]:
    return {
        "attempt_id": request.attempt_id,
        "lease_id": request.lease_id,
        "gate_receipt_id": request.gate_receipt_id,
        "adapter_id": request.adapter_id,
        "effect": request.effect,
        "target": request.target,
        "operation": request.operation,
        "closure_digest": request.closure_digest,
        "effect_digest": request.effect_digest,
        "precondition_digest": request.precondition_digest,
        "scope_digest": request.scope_digest,
        "request_digest": request.request_digest,
    }


def _result(
    request: FencedCommitRequest,
    outcome: FencedOutcome,
    evidence: Mapping[str, Digestable],
    *,
    target_receipt_ref: str | None = None,
    committed_at: datetime | None = None,
) -> FencedResult:
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
        target_result_digest=digest_json(evidence, domain="target:fenced-result"),
        target_receipt_ref=target_receipt_ref,
        committed_at=committed_at,
    )


def _indeterminate_result(
    request: FencedCommitRequest,
    evidence: Mapping[str, Digestable],
) -> FencedResult:
    return _result(request, FencedOutcome.INDETERMINATE, evidence)


def _validated_target(target: str, allow_insecure_localhost: bool) -> str:
    if "\r" in target or "\n" in target:
        raise ValueError("target contains forbidden control characters")
    parts = urlsplit(target)
    _validate_url_parts(parts, allow_insecure_localhost)
    return target


def _validate_url_parts(parts: SplitResult, allow_insecure_localhost: bool) -> None:
    if parts.scheme not in {"https", "http"}:
        raise ValueError("target must use HTTPS")
    if parts.username is not None or parts.password is not None:
        raise ValueError("target URL must not contain credentials")
    if not parts.hostname:
        raise ValueError("target URL must include a host")
    if parts.fragment:
        raise ValueError("target URL must not include a fragment")
    try:
        _ = parts.port
    except ValueError as error:
        raise ValueError("target URL has an invalid port") from error
    if parts.scheme == "http" and not (
        allow_insecure_localhost and _is_loopback_host(parts.hostname)
    ):
        raise ValueError("plain HTTP is allowed only for explicitly enabled loopback targets")


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _validate_header(name: str, value: str) -> None:
    if not isinstance(name, str) or not _HEADER_NAME.fullmatch(name):
        raise ValueError(f"invalid HTTP header name {name!r}")
    _require_header_value(f"header {name!r}", value)


def _require_header_value(name: str, value: str) -> None:
    if not isinstance(value, str) or not value or "\r" in value or "\n" in value:
        raise ValueError(f"{name} must be a non-empty value without CR or LF")


def _is_json_content_type(headers: Message) -> bool:
    return headers.get_content_type().lower() == "application/json"


def _reject_non_finite(value: str) -> None:
    raise ValueError(f"non-finite JSON value {value!r} is forbidden")
