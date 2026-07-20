# Contributing

TruthLease is an alpha infrastructure project. Changes should preserve its narrow
boundary and deterministic core.

## Setup

```bash
uv sync --extra dev
uv run pytest
```

## Before submitting a change

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest --cov=truthlease --cov-report=term-missing
uv run python benchmarks/benchmark_invalidation.py --nodes 120 --partitions 12
uv build
```

For a bug, add a regression test that fails before the fix. For a new contract
field, update the v1 schema, protocol document, CLI output, and compatibility
notes together.

Keep model calls, provider SDKs, source watchers, and effect delivery outside the
domain core. New adapters must state their trust and failure semantics.
