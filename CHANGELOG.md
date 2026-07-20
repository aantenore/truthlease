# Changelog

All notable changes to TruthLease are documented here. The project follows
[Semantic Versioning](https://semver.org/) once its public contract stabilizes.

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
