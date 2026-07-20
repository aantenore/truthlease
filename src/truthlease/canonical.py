"""Versioned, domain-separated canonical digests.

The encoder intentionally accepts only ordinary JSON values and rejects
non-finite numbers. It is stable inside TruthLease v1 but does not claim full
RFC 8785 compatibility.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

Digestable = None | bool | int | float | str | Sequence["Digestable"] | Mapping[str, "Digestable"]


def canonical_bytes(value: Digestable) -> bytes:
    normalized = _normalize_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def digest_json(value: Digestable, *, domain: str) -> str:
    if not domain or "\x00" in domain:
        raise ValueError("digest domain must be a non-empty string without NUL")
    payload = b"truthlease:v1\x00" + domain.encode("utf-8") + b"\x00" + canonical_bytes(value)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _normalize_json(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite numbers are not canonical JSON values")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")
