"""Access to TruthLease's versioned public contract schema."""

from __future__ import annotations

import json
from importlib import resources
from typing import Any, Literal, cast

SchemaVersion = Literal["v1", "v2"]
_SCHEMA_VERSIONS = frozenset({"v1", "v2"})


def coherence_schema_text(version: SchemaVersion = "v1") -> str:
    """Return one bundled coherence schema as UTF-8 JSON text.

    ``v1`` remains the default so existing callers keep reading the exact
    contract published with TruthLease 0.1.
    """

    selected = _validate_version(version)

    return (
        resources.files("truthlease")
        .joinpath("schemas", selected, "coherence.schema.json")
        .read_text(encoding="utf-8")
    )


def load_coherence_schema(version: SchemaVersion = "v1") -> dict[str, Any]:
    """Load one bundled coherence schema as a mapping."""

    schema = json.loads(coherence_schema_text(version))
    if not isinstance(schema, dict):
        raise ValueError("the bundled coherence schema must be a JSON object")
    return cast(dict[str, Any], schema)


def _validate_version(version: str) -> SchemaVersion:
    if version not in _SCHEMA_VERSIONS:
        supported = ", ".join(sorted(_SCHEMA_VERSIONS))
        raise ValueError(f"unsupported coherence schema version {version!r}; choose {supported}")
    return cast(SchemaVersion, version)
