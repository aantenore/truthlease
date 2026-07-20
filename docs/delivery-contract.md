# Delivery contract: TruthLease 0.2 alpha

## Objective

Deliver an executable, provider-neutral coherence control plane that prevents an
application from dispatching an effect through a lease after a known dependency
of its source artifact changed, and can produce a post-commit receipt when one
cooperating target atomically enforces the bound state.

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
- Bind revalidation evidence to the lease and proposed effect and enforce
  evidence freshness and an authority allowlist.
- Bind one target attempt to the lease, effect, adapter, precondition, replay
  contract, and exact protected observation scope without changing the v1 lease
  read model.
- Execute the target adapter only after the SQLite transaction closes.
- Derive cumulative `fenced` assurance only from a verified, deduplicated target
  commit that covers the full required observation closure.
- Keep certain rejection, ambiguous outcome, and conflicting result replay
  distinct and fail closed.
- Never redispatch an attempt already known as committed or not committed, and
  never let a late transport ambiguity erase a terminal fact.
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
- Preserve a byte-identical JSON v1 contract and ship a self-contained v2
  contract plus transactional SQLite v1-to-v2 migration.

## Out of scope

- Workflow scheduling, model routing, memory retrieval, CDC, authorization,
  distributed transactions, multi-target atomicity, and compensation.
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
- Concurrent prepare calls create at most one immutable attempt per lease.
- A changed target precondition produces a certain non-commit; a lost response
  remains indeterminate and is never promoted automatically.
- Identical result replay is idempotent; conflicting evidence marks the attempt
  disputed.
- Re-entering the high-level execution helper after a terminal result returns
  the known receipt or non-commit without another adapter call.
- A conformance fake target proves conditional update and deduplication in the
  same target-side critical section and one effect under ambiguous retry.
- A target commit test proves no SQLite write transaction remains open during
  adapter execution.
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
