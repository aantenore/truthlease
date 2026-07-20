"""Zero-dependency JSON CLI for humans and agents."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from truthlease.config import Settings
from truthlease.demo import run_demo
from truthlease.engine import TruthLease
from truthlease.errors import TruthLeaseError
from truthlease.models import (
    ArtifactDependency,
    ArtifactRef,
    AssuranceLevel,
    DependencyCompleteness,
    ReconcileAction,
)
from truthlease.schema import load_coherence_schema
from truthlease.storage.sqlite import SQLiteStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="truthlease",
        description="Keep agent artifacts coherent with their versioned dependencies.",
    )
    parser.add_argument("--db", help="SQLite database path (or TRUTHLEASE_DB)")
    subcommands = parser.add_subparsers(dest="command", required=True)

    demo = subcommands.add_parser("demo", help="run the stale-plan demonstration")
    demo.add_argument("--path", default=".tmp/truthlease-demo.db")
    demo.add_argument("--reset", action="store_true")

    observe = subcommands.add_parser("observe", help="record a versioned observation")
    observe.add_argument("key")
    observe.add_argument("version")
    observe.add_argument("--value", required=True, help="JSON value")
    observe.add_argument("--source", default="manual")
    observe.add_argument("--event-id")
    observe.add_argument("--valid-seconds", type=int)
    observe.add_argument(
        "--expected-generation",
        type=int,
        help="required compare-and-swap generation when updating an existing key",
    )

    show_observation = subcommands.add_parser(
        "observation", help="show the latest observation and its CAS generation"
    )
    show_observation.add_argument("key")

    publish = subcommands.add_parser("publish", help="publish an immutable artifact revision")
    publish.add_argument("artifact_id")
    publish.add_argument("--kind", required=True)
    publish.add_argument("--payload", required=True, help="JSON value")
    publish.add_argument("--observation", action="append", default=[], help="observation key")
    publish.add_argument("--artifact", action="append", default=[], help="id or id@revision")
    publish.add_argument(
        "--completeness",
        choices=[item.value for item in DependencyCompleteness],
        default=DependencyCompleteness.DECLARED.value,
    )
    publish.add_argument(
        "--reconcile",
        choices=[item.value for item in ReconcileAction],
        default=ReconcileAction.REPLAN.value,
    )

    show = subcommands.add_parser("show", help="show an artifact revision")
    show.add_argument("artifact_id")
    show.add_argument("--revision", type=int)

    acquire = subcommands.add_parser("lease", help="acquire an effect-bound validity lease")
    acquire.add_argument("artifact")
    acquire.add_argument("--effect", required=True, help="JSON value")
    acquire.add_argument("--target", required=True)
    acquire.add_argument("--operation", required=True)
    acquire.add_argument("--ttl", type=int)
    acquire.add_argument(
        "--assurance",
        choices=[AssuranceLevel.DECLARED.value, AssuranceLevel.OBSERVED.value],
        default=AssuranceLevel.DECLARED.value,
    )

    validate = subcommands.add_parser("validate", help="validate a lease without consuming it")
    validate.add_argument("lease_id")

    gate = subcommands.add_parser("gate", help="validate and consume a lease for an exact effect")
    gate.add_argument("lease_id")
    gate.add_argument("--effect", required=True, help="JSON value")

    reconcile = subcommands.add_parser("reconcile", help="show the minimal reconciliation plan")
    reconcile.add_argument("observation_key")

    subcommands.add_parser("graph", help="export the artifact graph as JSON")
    subcommands.add_parser("events", help="export the append-only event history")
    subcommands.add_parser("doctor", help="verify SQLite integrity and safety settings")
    subcommands.add_parser("schema", help="print the bundled v1 contract schema")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            output = run_demo(Path(args.path), reset=args.reset)
        elif args.command == "schema":
            output = load_coherence_schema()
        else:
            settings = Settings.from_env()
            database_path = Path(args.db) if args.db else settings.database_path
            runtime = TruthLease(
                SQLiteStore(database_path, busy_timeout_ms=settings.busy_timeout_ms),
                max_graph_depth=settings.max_graph_depth,
                max_impact_nodes=settings.max_impact_nodes,
            )
            if args.command == "lease" and args.ttl is None:
                args.ttl = settings.default_lease_seconds
            output = _execute(runtime, args)
        _print_json(output)
    except (TruthLeaseError, ValueError, FileExistsError, json.JSONDecodeError) as error:
        if isinstance(error, TruthLeaseError):
            problem: dict[str, Any] = error.as_problem()
        else:
            problem = {"code": "INVALID_INPUT", "title": str(error)}
        _print_json(problem, stream=sys.stderr)
        raise SystemExit(2) from error


def _execute(runtime: TruthLease, args: argparse.Namespace) -> Any:
    if args.command == "observe":
        valid_for = (
            timedelta(seconds=args.valid_seconds) if args.valid_seconds is not None else None
        )
        return runtime.observe(
            key=args.key,
            version=args.version,
            value=json.loads(args.value),
            source=args.source,
            event_id=args.event_id,
            valid_for=valid_for,
            expected_generation=args.expected_generation,
        )
    if args.command == "observation":
        return runtime.get_observation(args.key)
    if args.command == "publish":
        dependencies = [
            ArtifactDependency.observation(runtime.get_observation(key)) for key in args.observation
        ]
        dependencies.extend(
            ArtifactDependency.artifact(runtime.get_artifact(ref.artifact_id, ref.revision))
            for ref in (_parse_ref(value, runtime) for value in args.artifact)
        )
        return runtime.publish_artifact(
            artifact_id=args.artifact_id,
            kind=args.kind,
            payload=json.loads(args.payload),
            dependencies=dependencies,
            completeness=DependencyCompleteness(args.completeness),
            reconcile_action=ReconcileAction(args.reconcile),
        )
    if args.command == "show":
        return runtime.get_artifact(args.artifact_id, args.revision)
    if args.command == "lease":
        return runtime.acquire_lease(
            artifact=_parse_ref(args.artifact, runtime),
            effect=json.loads(args.effect),
            target=args.target,
            operation=args.operation,
            required_assurance=AssuranceLevel(args.assurance),
            ttl=timedelta(seconds=args.ttl),
        )
    if args.command == "validate":
        return runtime.validate_lease(args.lease_id)
    if args.command == "gate":
        return runtime.gate_effect(lease_id=args.lease_id, effect=json.loads(args.effect))
    if args.command == "reconcile":
        return runtime.reconciliation_plan(args.observation_key)
    if args.command == "graph":
        return runtime.graph()
    if args.command == "events":
        return runtime.events()
    if args.command == "doctor":
        return runtime.store.integrity_check()
    raise RuntimeError(f"unhandled command: {args.command}")


def _parse_ref(value: str, runtime: TruthLease) -> ArtifactRef:
    if "@" in value:
        artifact_id, revision_text = value.rsplit("@", 1)
        return ArtifactRef(artifact_id, int(revision_text))
    artifact = runtime.get_artifact(value)
    return artifact.ref


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _print_json(value: Any, *, stream: Any = None) -> None:
    if stream is None:
        stream = sys.stdout
    json.dump(_jsonable(value), stream, indent=2, sort_keys=True)
    stream.write("\n")


if __name__ == "__main__":
    main()
