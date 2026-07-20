# Delivery contract: TruthLease 0.1 alpha

## Objective

Deliver an executable, provider-neutral coherence control plane that prevents an
application from dispatching an effect through a lease after a known dependency
of its source artifact changed.

## Must

- Persist versioned observations and immutable artifact revisions.
- Reject delayed observation writes with a per-key generation compare-and-swap,
  and bind that generation into dependencies and lease snapshots.
- Derive time-aware effective freshness from required dependency state in read,
  lease, and gate paths.
- Invalidate the smallest transitive artifact closure after a source change when
  the graph remains within configured bounds.
- On graph-limit overflow, fail closed globally by marking every artifact
  `unknown` and invalidating every active lease; never present a partial traversal
  as minimal invalidation.
- Invalidate active leases for affected artifacts in the same local transaction.
- Bind a lease to one artifact revision, dependency snapshot, effect digest,
  target, operation, assurance requirement, and expiration.
- Validate and consume one lease atomically with a receipt; keep denial results
  explicit without claiming every attempt is journaled.
- Bind revalidation evidence to the lease and proposed effect, enforce evidence
  freshness and an authority allowlist, and reserve cumulative `fenced` assurance
  for a future target-side commit result.
- Make guarantee and dependency-completeness limits visible.
- Cap derived and leased completeness at the weakest required transitive
  dependency.
- Run with no model or hosted service and no third-party Python runtime
  dependency.

## Should

- Expose a Python API and automation-friendly CLI.
- Demonstrate partial reuse after policy drift.
- Emit an append-only, deduplicated history of accepted lifecycle events.
- Ship architecture, protocol, threat-model, and benchmark documentation.

## Out of scope

- Workflow scheduling, model routing, memory retrieval, CDC, authorization,
  distributed transactions, effect delivery, and compensation.
- Inferring undeclared causal dependencies.
- Claiming source truth or end-to-end freshness without target fencing.
- Claiming cross-language digest identity before a versioned canonicalization
  profile and golden vectors exist.

## Acceptance threshold

- Unit, property, integration, typing, lint, package, and demo gates pass.
- In-bound generated DAG tests prove exact minimal invalidation; overflow tests
  prove the global fail-closed fallback.
- Duplicate observation events are no-ops.
- Expired, consumed, mismatched, and invalidated leases fail closed.
- Lease validation checks local currentness without consumption and denies when
  additional revalidation or fencing assurance is required.
- The published JSON Schema is identified as an alpha read-model subset, not the
  complete behavioral contract.
- SQLite uses rollback journaling, full synchronization, foreign keys, and a
  bounded busy timeout; integrity checks pass.
- A clean install can run the README demo.
- Public Git history uses only the canonical personal identity and the remote
  default branch is `main` with no unfinished publication branch.

## Delivery mode

Create a public repository under `aantenore`, consolidate the verified initial
release directly on `main`, and monitor CI to completion.
