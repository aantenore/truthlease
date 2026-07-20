from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from truthlease.cli import main
from truthlease.demo import run_demo
from truthlease.storage.sqlite import SQLiteStore


def test_demo_proves_partial_reuse_and_store_integrity(tmp_path: Path) -> None:
    result = run_demo(tmp_path / "demo.db")
    assert result["old_plan"]["gate"] == "STALE_LEASE"  # type: ignore[index]
    assert result["new_plan"]["gate"] == "EFFECT_DISPATCHABLE"  # type: ignore[index]
    assert result["reusable"] == ["test-report@1"]
    assert result["integrity"]["ok"] is True  # type: ignore[index]


def test_demo_requires_explicit_reset(tmp_path: Path) -> None:
    path = tmp_path / "demo.db"
    run_demo(path)
    with pytest.raises(FileExistsError):
        run_demo(path)
    assert run_demo(path, reset=True)["integrity"]["ok"] is True  # type: ignore[index]


def test_cli_demo_and_doctor_emit_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    demo_path = tmp_path / "demo.db"
    main(["demo", "--path", str(demo_path)])
    output = json.loads(capsys.readouterr().out)
    assert output["invalidation"]["duplicate_event_was_noop"] is True

    main(["--db", str(demo_path), "doctor"])
    doctor = json.loads(capsys.readouterr().out)
    assert doctor["ok"] is True
    assert doctor["journal_mode"] == "delete"


def test_cli_reports_invalid_input(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["--db", str(tmp_path / "db"), "observe", "key", "v1", "--value", "{"])
    assert raised.value.code == 2
    problem = json.loads(capsys.readouterr().err)
    assert problem["code"] == "INVALID_INPUT"


def test_cli_complete_artifact_and_lease_flow(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = str(tmp_path / "flow.db")

    def invoke(arguments: list[str]) -> Any:
        main(["--db", database, *arguments])
        return json.loads(capsys.readouterr().out)

    invoke(["observe", "policy", "v1", "--value", '{"enabled":true}'])
    base = invoke(
        [
            "publish",
            "base",
            "--kind",
            "result",
            "--payload",
            '{"ok":true}',
            "--observation",
            "policy",
            "--completeness",
            "observed",
        ]
    )
    assert base["ref"]["revision"] == 1
    plan = invoke(
        [
            "publish",
            "plan",
            "--kind",
            "plan",
            "--payload",
            '{"step":1}',
            "--artifact",
            "base@1",
            "--completeness",
            "observed",
        ]
    )
    assert plan["status"] == "fresh"
    assert invoke(["show", "plan"])["ref"]["artifact_id"] == "plan"
    lease = invoke(
        [
            "lease",
            "plan",
            "--effect",
            '{"write":true}',
            "--target",
            "target",
            "--operation",
            "write",
        ]
    )
    assert invoke(["validate", lease["lease_id"]])["allowed"] is True
    assert invoke(["gate", lease["lease_id"], "--effect", '{"write":true}'])["allowed"]
    assert invoke(["reconcile", "policy"])["items"] == []
    assert len(invoke(["graph"])["artifacts"]) == 2
    assert len(invoke(["events"])) >= 5


def test_database_integrity_contract(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "db.sqlite")
    store.initialize()
    report = store.integrity_check()
    assert report["ok"] is True
    assert report["foreign_keys_enabled"] is True
    assert report["journal_mode"] == "delete"
    assert report["schema_version"] == 2


def test_cli_exposes_and_enforces_observation_generation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = str(tmp_path / "generation.db")
    main(["--db", database, "observe", "policy", "v1", "--value", '{"value":1}'])
    capsys.readouterr()
    main(["--db", database, "observation", "policy"])
    current = json.loads(capsys.readouterr().out)
    assert current["generation"] == 1

    with pytest.raises(SystemExit):
        main(["--db", database, "observe", "policy", "v2", "--value", '{"value":2}'])
    assert json.loads(capsys.readouterr().err)["code"] == "CONFLICT"

    main(
        [
            "--db",
            database,
            "observe",
            "policy",
            "v2",
            "--value",
            '{"value":2}',
            "--expected-generation",
            "1",
        ]
    )
    capsys.readouterr()
    main(["--db", database, "observation", "policy"])
    assert json.loads(capsys.readouterr().out)["generation"] == 2
