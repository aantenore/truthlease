"""Access to TruthLease's versioned public contract schema."""

from __future__ import annotations

import json
from importlib import resources
from typing import Any, cast


def coherence_schema_text() -> str:
    """Return the bundled v1 coherence schema as UTF-8 JSON text."""

    return (
        resources.files("truthlease")
        .joinpath("schemas", "v1", "coherence.schema.json")
        .read_text(encoding="utf-8")
    )


def load_coherence_schema() -> dict[str, Any]:
    """Load the bundled v1 coherence schema as a mapping."""

    schema = json.loads(coherence_schema_text())
    if not isinstance(schema, dict):
        raise ValueError("the bundled coherence schema must be a JSON object")
    return cast(dict[str, Any], schema)
