# ADR 0003: LangGraph stale-safe resume

- Status: accepted
- Date: 2026-07-22
- Release: `0.3.0-alpha.1`

## Context

LangGraph can checkpoint an interrupted workflow and restart its node later.
That durability does not imply that the policy, record, schema, approval, or
target token behind a saved action is still current. Its interrupt contract also
means the node restarts from the beginning, so side effects around interruption
must be idempotent.

TruthLease `0.2` could fence a target commit and reuse a prepared attempt, but a
workflow crash immediately after lease acquisition could rerun the node and
acquire a new lease before the lease ID itself reached the workflow checkpoint.
The framework integration therefore needs a durable identity below the graph
checkpoint boundary.

## Decision

Add two independent, composable pieces:

1. SQLite schema v3 stores a digest-only `lease_requests` binding from one
   application idempotency key to one lease-intent digest and one lease ID.
2. An optional LangGraph graph composes approval, authoritative revalidation,
   fenced lease acquisition, target execution, and same-attempt recovery.

The core remains free of mandatory third-party runtime dependencies. LangGraph
and its SQLite saver are installed through the `langgraph` extra.

## Lease acquisition identity

`TruthLease.acquire_lease()` accepts an optional algorithm-prefixed
`idempotency_key`. The stored intent digest binds the artifact reference, effect,
target, operation, assurance, TTL at microsecond precision, boot epoch, and
target-precondition descriptor.

- First use performs the normal freshness checks and atomically stores the lease
  and request binding.
- Identical reuse returns the exact original lease, including terminal state.
- Reuse for different intent fails closed with `ConflictError`.
- No raw effect, target token, or workflow label is stored in the binding.

Returning a consumed lease is deliberate: its durable attempt is the only safe
identity for resolving an ambiguous target outcome.

## Graph structure

The graph has four versioned nodes:

```text
START -> approval_v1 -> authorize_v1 -> execute_fenced_v1
                                      -> recover_indeterminate_v1
                                      -> execute_fenced_v1
```

- `approval_v1` has no side effect before `interrupt()`.
- `authorize_v1` re-reads authoritative sources, verifies resolved digests, and
  acquires the idempotent fenced lease. It performs no target effect.
- `execute_fenced_v1` resolves again, prepares one durable attempt, and invokes
  the target outside the database transaction.
- `recover_indeterminate_v1` requires an explicit boolean before retrying that
  same attempt.

Node names and state fields are a persistence contract. Incompatible evolution
requires versioned names and a checkpoint migration strategy.

## Checkpoint minimization

The graph state contains only references, digests, IDs, phase, approval, and
stable decision codes. Recovery approval is bound to a digest of the current
attempt status and result. Raw effects, opaque target tokens, evidence payloads,
and exception messages stay behind replaceable callbacks.

This is data minimization, not encryption or tamper resistance. The host and
checkpoint store remain trusted, references must not contain secrets, and
low-entropy identifiers need an application-owned keyed digest if guessing is a
concern.

## Crash boundaries

| Crash point | Recovery behavior |
| --- | --- |
| Before approval checkpoint | No lease and no target effect exist. |
| During authoritative read | The node reruns; source adapters must use their normal compare-and-swap discipline. |
| After lease acquisition but before graph checkpoint | The node reacquires by the same idempotency key and receives the original lease. |
| After attempt prepare but before target call | A prepared attempt is conservatively treated as potentially dispatched and requires recovery approval before the same request can be sent. |
| After target commit but before local result record | The prepared attempt is treated as potentially dispatched and requires recovery approval; the target then resolves the same attempt ID and request fingerprint. |
| After local receipt but before graph checkpoint | Re-execution returns the terminal receipt without redispatch. |

Recovery approval remains valid while resolving one unchanged durable attempt
basis, including across a crash of that approved node. If status or result digest
changes, the basis changes and the old approval is not reused. This authorizes
resolution of one logical attempt, not an unbounded sequence of new effect
identities; target-side deduplication remains required for repeated transport.
If the attempt becomes terminal while the recovery interrupt is paused, that
durable outcome takes precedence over the stale human response and is surfaced
without another effect/precondition resolution or target dispatch.

## Alternatives considered

### Store the full effect in LangGraph state

Rejected. It couples checkpoint retention to application payload sensitivity and
makes accidental prompt, credential, or target-token persistence likely.

### Trust the saved checkpoint without authoritative revalidation

Rejected. Durable position and current authority are different properties.

### Acquire a fresh lease on every node retry

Rejected. A crash between target commit and checkpoint could create a second
attempt and duplicate the durable effect.

### Put LangGraph in the core dependency set

Rejected. TruthLease is framework-neutral and its deterministic core remains
usable in small services and other workflow engines.

### Build a workflow engine in TruthLease

Rejected. Scheduling, general human-task management, compensation, and arbitrary
graph execution remain outside this project's boundary.

## Consequences

Positive:

- a real durable-orchestrator integration now exercises the commit-time contract;
- pause/restart and ambiguous-response recovery preserve one lease and attempt;
- framework checkpoints avoid raw effects and target tokens;
- callbacks and checkpointers remain replaceable.

Costs and limitations:

- callers must keep immutable effect and precondition references resolvable for
  the full recovery horizon;
- TTL, boot epoch, and every other intent component must remain identical during
  recovery;
- target deduplication retention must outlive that recovery horizon;
- SQLite schema version advances from 2 to 3;
- paused state version 1 and node names now require compatibility discipline;
- the initial adapter is synchronous and covers one fenced effect, not a general
  multi-target workflow.
