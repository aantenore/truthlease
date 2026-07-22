# Changelog

All notable changes to TruthLease are documented here. The project follows
[Semantic Versioning](https://semver.org/) once its public contract stabilizes.

## 0.3.0-alpha.1 - 2026-07-22

Stale-safe durable resume vertical slice:

- optional LangGraph `1.2.x` integration with persistent approval, authoritative
  revalidation, fenced execution, and explicit ambiguous-outcome recovery;
- digest-minimized checkpoint state containing references, bindings, durable
  IDs, phases, and stable decision codes instead of raw effects, target tokens,
  evidence payloads, or exception messages;
- SQLite schema v3 `lease_requests` binding for restart-safe, idempotent lease
  acquisition across workflow-node replay;
- exact lease-intent conflict detection across artifact, effect, target,
  operation, assurance, microsecond TTL, boot epoch, and target precondition;
- recovery of the original lease after consumption or invalidation so an
  existing target attempt is resolved rather than replaced;
- public read-only lease and lease-to-attempt accessors for framework adapters;
- real `SqliteSaver` process-restart tests covering policy drift, target-token
  drift, duplicate dispatch, response loss after commit, incomplete dependency
  capture, reference drift, strict approval types, and raw-value redaction;
- explicit recovery approval bound to the current durable attempt status and
  result digest, including pre-checkpoint crash tests;
- conservative same-attempt recovery when local persistence fails after a
  possible target commit, while an already persisted receipt returns without a
  redundant state read;
- durable terminal truth taking precedence over stale recovery responses and
  payload resolvers after an attempt is resolved by another worker;
- migration tests for populated v1 and v2 stores with no synthetic request
  backfill;
- architecture decision record, integration guide, updated threat model, and
  current competitive boundary.

The alpha does not turn TruthLease into a workflow engine, authorize arbitrary
LangGraph state, encrypt checkpoints, discover undeclared dependencies, or make
external effects exactly once without a cooperating deduplicating target.

## 0.2.0-alpha.1 - 2026-07-20

Target-fencing vertical slice:

- additive two-phase API that prepares a bound target attempt, executes outside
  SQLite, and records a post-commit result;
- target preconditions bound to adapter, mechanism, expected token, protected
  observation scope, and replay assurance;
- core-derived `fenced` receipts only for committed, deduplicated, full-closure
  results;
- explicit `not_committed`, `indeterminate`, and fail-closed disputed outcomes;
- stable attempt identity and idempotent result replay without persisting raw
  effects, target tokens, credentials, or response bodies;
- strict zero-dependency HTTP reference profile for strong ETag conditional
  commits with bounded responses and no redirects;
- transactional SQLite schema migration from v1 to v2 with no synthetic fenced
  backfill;
- self-contained JSON contract v2 while the bundled v1 contract remains
  byte-identical and the default;
- target correctness, migration, packaging, and protocol-conformance tests;
- CodeQL analysis and tag-triggered release smoke tests.

The alpha does not promise distributed transactions, external exactly-once
execution, multi-target atomicity, or freshness after the recorded commit.

## 0.1.0-alpha.1 - 2026-07-20

Initial executable alpha:

- version-bound observations and immutable artifact revisions;
- per-key generation compare-and-swap and ABA-safe dependency snapshots;
- exact minimal transitive invalidation;
- global fail-closed fallback when configured graph bounds are exceeded;
- explicit dependency-completeness and assurance levels;
- transitive completeness inheritance that prevents evidence laundering;
- short-lived, effect-bound, single-use validity leases;
- lease-bound, allowlisted, freshness-checked revalidation evidence;
- atomic local gate receipts and append-only event history;
- safe SQLite defaults and integrity diagnostics;
- zero-runtime-dependency Python API and JSON CLI;
- reproducible partial-reuse demo and benchmark;
- JSON Schema contract, architecture, threat model, and market boundaries.

The alpha deliberately does not issue `fenced` assurance because it does not
execute target effects. A target-side result contract is deferred.
