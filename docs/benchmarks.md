# Benchmark methodology

TruthLease measures correctness before speed. A fast under-invalidation result is
a failure.

## Included harness

```bash
uv run python benchmarks/benchmark_invalidation.py --nodes 1000 --partitions 20
```

The harness constructs independent artifact chains against real SQLite storage,
changes one source, and reports:

- total nodes;
- exact expected and actual impacted nodes;
- reusable nodes and work-reuse ratio;
- local invalidation latency;
- a machine-readable correctness flag.

Latency is environment-specific and is not committed as a universal project
claim. For graphs inside the configured traversal bounds, the stable assertion is
that the impacted set equals the dependency closure and that unrelated partitions
remain reusable. Overflow is a separate safety case: every artifact becomes
`unknown` and every active lease is invalidated, so no minimality or reuse claim
applies to that run.

## Comparative baselines

Future benchmark releases should compare:

1. full rerun of all artifacts;
2. full descendant recomputation;
3. TruthLease exact-token propagation with partial reuse.

Workloads should cover chains, stars, diamonds, layered DAGs, 1-20% source
changes, no-op content refreshes, and hot-source bursts. Use fixed seeds, cold
and warm runs, at least 30 repetitions, and report median plus confidence
intervals.

## Correctness metrics

- stale-effect escape count;
- under-invalidation count;
- false invalidation count;
- deterministic final-state equality with a full recomputation oracle;
- duplicate gate consumption;
- lost committed events.
- incorrect partial-success result after graph-limit overflow.

## Performance metrics

- weighted work reused;
- p50, p95, and p99 invalidation latency;
- p99 local gate latency;
- SQLite lock-wait rate;
- database bytes per artifact and event;
- recovery and projection-rebuild time.

No performance target becomes a README claim until a versioned environment and
raw machine-readable result are published.
