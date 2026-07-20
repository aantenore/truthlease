from __future__ import annotations

import json
from pathlib import Path

import pytest

from truthlease import coherence_schema_text, load_coherence_schema
from truthlease.cli import main

PUBLIC_SCHEMA_ID = (
    "https://raw.githubusercontent.com/aantenore/truthlease/main/schemas/v1/coherence.schema.json"
)


def test_public_and_packaged_schema_copies_are_identical() -> None:
    public_copy = Path("schemas/v1/coherence.schema.json").read_text(encoding="utf-8")
    packaged_copy = Path("src/truthlease/schemas/v1/coherence.schema.json").read_text(
        encoding="utf-8"
    )

    assert public_copy == packaged_copy == coherence_schema_text()
    assert load_coherence_schema()["$id"] == PUBLIC_SCHEMA_ID


def test_schema_cli_reads_the_package_resource_without_creating_a_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    main(["schema"])

    assert json.loads(capsys.readouterr().out)["$id"] == PUBLIC_SCHEMA_ID
    assert not (tmp_path / "truthlease.db").exists()
