from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from truthlease import coherence_schema_text, load_coherence_schema
from truthlease.cli import main

V1_SCHEMA_ID = (
    "https://raw.githubusercontent.com/aantenore/truthlease/main/schemas/v1/coherence.schema.json"
)
V2_SCHEMA_ID = (
    "https://raw.githubusercontent.com/aantenore/truthlease/main/schemas/v2/coherence.schema.json"
)
V1_SCHEMA_SHA256 = "48cedbc56886f37e606436ecc85b665338cba31c91d073800804d5556b84d281"


@pytest.mark.parametrize(
    ("version", "schema_id"),
    (("v1", V1_SCHEMA_ID), ("v2", V2_SCHEMA_ID)),
)
def test_public_and_packaged_schema_copies_are_identical(version: str, schema_id: str) -> None:
    public_copy = Path(f"schemas/{version}/coherence.schema.json").read_text(encoding="utf-8")
    packaged_copy = Path(f"src/truthlease/schemas/{version}/coherence.schema.json").read_text(
        encoding="utf-8"
    )

    assert public_copy == packaged_copy == coherence_schema_text(version)  # type: ignore[arg-type]
    assert load_coherence_schema(version)["$id"] == schema_id  # type: ignore[arg-type]


def test_v1_schema_is_byte_frozen_and_remains_the_default() -> None:
    public_bytes = Path("schemas/v1/coherence.schema.json").read_bytes()

    assert hashlib.sha256(public_bytes).hexdigest() == V1_SCHEMA_SHA256
    assert coherence_schema_text() == public_bytes.decode("utf-8")
    assert load_coherence_schema()["$id"] == V1_SCHEMA_ID


def test_schema_loader_rejects_unknown_versions() -> None:
    with pytest.raises(ValueError, match="unsupported coherence schema version"):
        coherence_schema_text("latest")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unsupported coherence schema version"):
        load_coherence_schema("../v1")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("arguments", "schema_id"),
    ((["schema"], V1_SCHEMA_ID), (["schema", "--version", "v2"], V2_SCHEMA_ID)),
)
def test_schema_cli_reads_the_package_resource_without_creating_a_database(
    arguments: list[str],
    schema_id: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    main(arguments)

    assert json.loads(capsys.readouterr().out)["$id"] == schema_id
    assert not (tmp_path / "truthlease.db").exists()
