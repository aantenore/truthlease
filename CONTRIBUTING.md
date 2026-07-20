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
uv run python benchmarks/benchmark_fence_correctness.py
uv build
```

For a bug, add a regression test that fails before the fix. For a new contract
field, preserve the byte-frozen v1 schema and update the latest schema, protocol
document, CLI output, and compatibility notes together.

Keep model calls, provider SDKs, source watchers, and network I/O outside the
domain transaction. New adapters must state their trust, atomic precondition,
deduplication, coverage, ambiguity, and recovery semantics.
