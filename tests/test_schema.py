from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


def test_public_contract_schema_is_valid_and_accepts_an_observation() -> None:
    schema = json.loads(Path("schemas/v1/coherence.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(
        {
            "key": "policy/release",
            "version": "v12",
            "digest": "sha256:abc123",
            "generation": 1,
            "observed_at": "2026-07-20T12:00:00Z",
            "valid_until": None,
            "status": "current",
            "source": "manual",
        }
    )
