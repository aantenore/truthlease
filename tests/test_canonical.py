from __future__ import annotations

from collections import UserDict

import pytest

from truthlease.canonical import canonical_bytes, digest_json


def test_digest_is_order_independent_and_domain_separated() -> None:
    first = digest_json({"b": 2, "a": 1}, domain="artifact")
    second = digest_json({"a": 1, "b": 2}, domain="artifact")
    assert first == second
    assert first != digest_json({"a": 1, "b": 2}, domain="observation")
    assert canonical_bytes({"b": 2, "a": 1}) == b'{"a":1,"b":2}'


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_rejected(value: float) -> None:
    with pytest.raises(ValueError, match="non-finite"):
        canonical_bytes(value)


def test_unsupported_values_and_invalid_domains_are_rejected() -> None:
    with pytest.raises(TypeError, match="unsupported"):
        canonical_bytes(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="keys"):
        canonical_bytes({1: "value"})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="domain"):
        digest_json({}, domain="")


def test_generic_mappings_are_normalized_before_encoding() -> None:
    assert canonical_bytes(UserDict({"b": 2, "a": 1})) == b'{"a":1,"b":2}'
