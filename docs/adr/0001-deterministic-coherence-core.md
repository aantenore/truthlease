# ADR 0001: deterministic coherence core

- Status: accepted
- Date: 2026-07-20

## Context

Agent outputs may depend on mutable policies, resources, approvals, schemas, or
other artifacts. Durable execution protects progress after failure but does not
prove those inputs still authorize reuse.

## Decision

TruthLease uses a deterministic dependency graph, immutable artifact revisions,
exact version and digest comparison, short-lived leases, and a use-time gate.
LLMs may propose new artifact revisions through an external replanning adapter;
they never determine freshness or mutate the graph projection directly.

Read, lease, and gate paths evaluate effective status against the current time.
Graph traversal is minimal only within configured bounds; overflow activates a
global fail-closed fallback that marks all artifacts `unknown` and invalidates all
active leases. Revalidation evidence is fresh, authority-allowlisted, and bound to
the exact lease, closure, effect, target, and operation. `fenced` remains a
cumulative future target-result level.

The first storage adapter is a single-writer SQLite modular monolith. It uses
rollback journaling and `BEGIN IMMEDIATE`. Network calls are forbidden inside a
database transaction.

The initial digest format is an implementation-local Python profile. A
cross-language identity promise requires a separately versioned canonicalization
profile and shared golden vectors.

## Consequences

- Correctness can be tested with deterministic and property-based oracles.
- The runtime only knows declared or integration-observed dependencies.
- Accepted lifecycle events are journaled, but denial attempts are returned
  decisions and are not promised as event-log entries.
- Remote changes after the local gate remain possible unless the target enforces
  an atomic fencing precondition.
- High-volume CDC, durable scheduling, and effect delivery remain replaceable
  integrations rather than bundled subsystems.
