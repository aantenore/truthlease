# ADR 0002: Two-phase target fencing with post-commit receipts

- Status: accepted for `0.2.0-alpha.1`
- Date: 2026-07-20

## Context

TruthLease `0.1` can prove that an artifact still matches the observations
committed to its local store when a lease is consumed. It cannot close the gap
between that local transaction and a later remote effect. Durable workflow
engines also cannot remove this gap by themselves: the target must participate
through an atomic precondition, and ambiguous retries require target-side
deduplication.

HTTP already provides the primitive used by the reference adapter. RFC 9110
requires an origin server to evaluate `If-Match` immediately before applying a
state-changing method and to reject a false condition with `412`. The
`Idempotency-Key` header remains an IETF work item rather than a final RFC, so
TruthLease treats its useful semantics as an adapter contract, not as a
universal protocol guarantee.

The differentiated responsibility is therefore not generating an ETag or
retrying a request. It is preserving a verifiable binding between:

- the immutable artifact and dependency closure;
- the exact effect, target, and operation;
- the target precondition and the dependency scope it claims to protect;
- one stable attempt identity used for target deduplication;
- the target result and a post-commit receipt.

## Decision

TruthLease uses three phases:

```text
prepare and consume the local lease
    -> execute one target adapter outside SQLite
        -> record the bound target result and derive a receipt
```

`gate_effect()` remains local and never emits `fenced` assurance. The new path
is additive and does not change its existing signature or behavior.

### Target precondition

A `TargetPrecondition` declares:

- the allowlisted adapter identity;
- an opaque mechanism such as `http-if-match` or `sql-cas`;
- the expected target token;
- the observation keys protected by the same atomic target commit;
- the target's declared replay assurance.

The core computes, rather than accepts, the precondition and scope digests. The
scope digest covers the exact version, generation, and content digest of every
protected observation in the lease snapshot.

For a lease that requires `fenced` assurance, dependency completeness must be
`observed`, replay must be declared deduplicated, and protected observation keys
must equal the full required observation closure. A precondition that protects
only part of the closure is recorded as partial coverage and cannot satisfy a
globally fenced lease.

The adapter assertion is not self-authenticating. The embedding application
must allowlist and secure adapters, and must describe the protected scope
honestly. A normal resource ETag does not automatically protect unrelated
policy, approval, or schema observations.

### Local preparation

`prepare_fenced_commit()` runs in one SQLite write transaction. It:

1. verifies the exact effect and all normal lease conditions;
2. verifies fresh, allowlisted revalidation evidence when cumulative assurance
   requires it;
3. verifies the persisted target-precondition binding;
4. consumes the single-use lease into one immutable attempt;
5. writes a lower-assurance gate receipt stating that no target result exists
   yet;
6. returns a `FencedCommitRequest` containing the original effect and target
   precondition plus core-derived binding digests.

Repeated preparation with identical inputs returns the same attempt. Different
inputs for the consumed lease are a conflict.

Raw effect payloads, target tokens, credentials, and response bodies are not
stored. Recovery callers resupply effect and precondition values; the core
accepts them only when their recomputed digests match the persisted attempt.

### Adapter execution

`TargetAdapter.commit()` receives the prepared request and runs only after the
SQLite transaction has closed. The attempt ID is the target idempotency key.

The strict HTTP reference adapter:

- accepts only a strong quoted ETag for `If-Match`;
- sends canonical JSON and the persisted attempt identity;
- rejects redirects rather than silently changing origin;
- bounds timeout and response size;
- rejects header injection and reserved-header overrides;
- treats `412` as a safe terminal precondition failure;
- treats timeouts, connection loss, malformed success evidence, `409`, and
  server errors as indeterminate;
- requires a success response to echo the permit and request binding plus a
  stable target receipt reference.

It is a conformance profile for a cooperating endpoint, not a generic HTTP
client that infers safety from a status code alone.

### Result and receipt

`FencedResult` never contains an assurance field. The core validates the
adapter, attempt, request, precondition, scope, and evidence bindings, then
derives the result.

Outcomes are:

- `committed`: the target reports that the conditional commit completed;
- `not_committed`: the target atomically rejected the precondition or another
  certain rejection occurred;
- `indeterminate`: the effect might have completed and must be resolved with
  the same attempt identity.

Only a committed result whose enforced scope equals the full required closure
can create assurance `fenced`. The resulting `FencedReceipt` is a historical
post-commit fact. It is not authority to execute again and does not assert that
the target or dependencies are still current when the receipt is later read.

An identical result replay returns the same receipt. A conflicting result for
the same attempt changes the attempt to `disputed` and fails closed. An
indeterminate attempt never creates a receipt; recovery reuses the same attempt
and request fingerprint. A fresh attempt after an ambiguous outcome is not
allowed.

### Persistence and compatibility

- Python package version becomes `0.2.0a1`.
- JSON contract `v1` remains byte-identical and the schema loader continues to
  default to it.
- A self-contained JSON contract `v2` adds the target-fencing read models.
- SQLite schema version `2` adds fence bindings, attempts, target results, and
  post-commit receipts.
- Migration `1 -> 2` is one transaction, preserves all existing rows, and
  performs no synthetic fenced backfill.
- Existing `0.1` fenced-required leases have no target binding and are rejected
  by the new path rather than promoted.

The existing `gate_receipts` table remains the local pre-dispatch boundary. A
separate receipt table records confirmed target commits; the two histories are
never merged or overwritten.

## Rejected alternatives

### Call the target inside `gate_effect()`

Rejected because it would hold a SQLite writer transaction over unbounded
network I/O, couple the core to transport failures, and still not create a
distributed transaction.

### Mark a successful HTTP status as fenced

Rejected because a status alone does not prove precondition enforcement,
request binding, stable deduplication, or full dependency coverage.

### Retry with a new attempt after a timeout

Rejected because the original effect might already have committed. Recovery
must use the same target idempotency identity or stop as indeterminate.

### Extend the JSON v1 objects in place

Rejected because the published objects use closed schemas. A new version keeps
v1 consumers deterministic and offline.

### Build a workflow engine or transaction coordinator

Rejected because durable scheduling, compensation, and multi-target atomicity
are separate products. TruthLease supplies a portable coherence and receipt
contract that those systems can call.

## Consequences

The implementation closes the remote target race only for the scope atomically
enforced by a trusted adapter. It deliberately does not promise external
exactly-once execution, global atomicity across targets, or ongoing freshness
after commit. Those limits remain visible in every decision, receipt, benchmark,
and README example.
