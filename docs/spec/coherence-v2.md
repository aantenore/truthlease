# TruthLease Coherence Contract v2

- Status: alpha
- Contract identifier: `dev.truthlease.coherence.v2`
- Read-model schema:
  [`schemas/v2/coherence.schema.json`](../../schemas/v2/coherence.schema.json)
- Fencing decision:
  [ADR 0002](../adr/0002-target-fencing-two-phase.md)

Contract v2 is a backward-compatible superset of the closed v1 read models. It
adds a two-phase target-fencing contract without changing observation, artifact,
lease, revalidation-evidence, or gate-decision objects from v1.

The JSON Schema is self-contained and validates portable read models. Runtime
transactions, adapter authentication, network behavior, migrations, and state
transitions remain behavioral requirements rather than claims made by JSON
validation alone.

Normative terms `MUST`, `MUST NOT`, `SHOULD`, and `MAY` are used in their usual
standards sense.

## Compatibility

- The published v1 schema MUST remain byte-identical.
- Every valid v1 read model MUST remain valid under v2.
- Existing Python schema-loader and CLI calls MUST continue to select v1 unless
  a caller explicitly requests v2.
- New fencing documents are v2-only; a v1 validator is expected to reject them.
- Existing digest domains and v1 lease identities MUST NOT be rewritten during
  migration.

The bundled loader is version-selectable:

```python
from truthlease import load_coherence_schema

v1 = load_coherence_schema()
v2 = load_coherence_schema("v2")
```

The equivalent CLI commands are:

```bash
truthlease schema
truthlease schema --version v2
```

## Assurance model

The cumulative assurance ordering remains:

```text
declared < observed < revalidated < fenced
```

`gate_effect()` remains a local boundary and MUST NOT emit `fenced`. A fenced
receipt is derived only after the core has prepared one bound attempt, the
adapter has executed outside the SQLite transaction, and the core has validated
the returned post-commit result.

Fenced assurance means that a trusted target adapter proved atomic enforcement
for its declared protected scope. It does not prove that undeclared inputs were
captured, that unrelated external facts were frozen, that multiple targets
committed atomically, or that the receipt is still current when later read.

## Enums

### Replay assurance

```text
deduplicated | one_shot | unknown
```

- `deduplicated`: the target declares stable attempt-identity replay semantics.
- `one_shot`: the target accepts one attempt but cannot safely resolve an
  ambiguous retry.
- `unknown`: the adapter cannot establish target replay behavior.

Only `deduplicated` replay assurance is eligible for a fenced receipt.

### Coverage

```text
full | partial
```

- `full`: protected observation keys equal the complete required observation
  closure of the lease.
- `partial`: the target precondition protects only a strict subset.

Only full coverage is eligible for a fenced receipt. An empty closure can have
full coverage when the protected-observation set is also empty.

### Target outcome

```text
committed | not_committed | indeterminate
```

- `committed`: the adapter reports a completed conditional target commit with
  bound success evidence.
- `not_committed`: the adapter reports a certain rejection, including an
  atomically failed precondition.
- `indeterminate`: the effect may have committed and recovery must retain the
  same attempt identity.

### Attempt status

```text
prepared | committed | not_committed | indeterminate | disputed
```

Attempt status is monotone. Conflicting evidence for one attempt produces
`disputed`; it MUST NOT overwrite a prior fact or create a receipt.

## TargetPrecondition

A target precondition declares the adapter contract supplied at preparation:

```json
{
  "adapter_id": "http-etag:v1",
  "mechanism": "http-if-match",
  "expected_token": "\"release-41\"",
  "protected_observations": ["deployment/current"],
  "replay_assurance": "deduplicated"
}
```

The fields are:

- `adapter_id`: application-allowlisted adapter identity;
- `mechanism`: opaque conditional-commit mechanism;
- `expected_token`: target token supplied to the adapter but never trusted as a
  core-derived digest;
- `protected_observations`: unique observation keys claimed to be covered by
  the same atomic target commit;
- `replay_assurance`: declared target deduplication behavior.

The core MUST derive the precondition digest and scope digest. Callers and
adapters MUST NOT select those digests directly.

## FencedCommitRequest

`prepare_fenced_commit()` returns one immutable request after it has validated
and consumed the local lease. The request binds:

- attempt, lease, and lower-assurance gate receipt identities;
- adapter identity;
- original JSON effect, target, and operation;
- closure and effect digests;
- target precondition plus its core-derived digest;
- protected-scope and complete-request digests;
- derived coverage;
- preparation and expiry times.

The `attempt_id` is the stable target idempotency identity. Re-preparing
identical inputs MUST return the same request. Different inputs for the same
consumed lease MUST conflict.

The original effect and target token appear in the in-memory request so the
adapter can execute them. Persistence stores only their bound digests and safe
metadata; recovery callers must resupply values whose recomputed digests match.

## FencedAttempt

`FencedAttempt` is the durable recovery and audit view created during
preparation. It contains attempt, lease, gate-receipt, and adapter identities;
request, precondition, and scope digests; replay assurance and coverage; the
monotone attempt status; preparation and optional completion times; and an
optional result digest.

This read model is deliberately digest-only. It MUST NOT expose the original
effect or raw target token. A `prepared` attempt has no known target outcome;
callers must recover it using the same attempt identity rather than preparing a
new effect. JSON Schema validation closes the object and validates its value
domains, while lifecycle transitions remain a core persistence invariant.

## FencedResult

An adapter returns a `FencedResult` containing:

- attempt, lease, and adapter identities;
- target outcome;
- request, closure, effect, precondition, and scope digests;
- target and operation;
- replay assurance and coverage asserted by the adapter result;
- target-result digest;
- optional stable target receipt reference and commit time.

`FencedResult` deliberately has no assurance field. The adapter cannot award
TruthLease assurance; only the core can validate all bindings and derive it.

For `committed`, `target_receipt_ref` and `committed_at` MUST both be non-null.
For `not_committed` and `indeterminate`, both MUST be null. A timeout, connection
loss, malformed success response, or incomplete binding MUST produce
`indeterminate`, never an inferred success.

An identical replay is idempotent. While an attempt is `indeterminate`, a
different result for the same attempt MAY be appended to resolve it as
`committed` or `not_committed`; the attempt identity and request bindings MUST
remain unchanged. A later `indeterminate` transport observation cannot
contradict an already known terminal fact and MUST NOT replace it. After an
attempt reaches either terminal state, a different terminal target-result
digest for that attempt MUST make it disputed and fail closed.

## FencedReceipt

Only a validated committed result with `deduplicated` replay assurance and
`full` coverage can create a `FencedReceipt`.

The receipt binds:

- receipt, attempt, lease, and local gate-receipt identities;
- assurance fixed to `fenced`;
- adapter, target, and operation;
- request, closure, effect, precondition, scope, and target-result digests;
- replay assurance fixed to `deduplicated`;
- coverage fixed to `full`;
- stable target receipt reference;
- target commit and local recording times;
- an explicit guarantee statement.

A receipt is an append-only historical fact. It MUST NOT authorize another
effect and MUST NOT claim that its dependencies or target remain current after
the recorded commit. If later conflicting terminal evidence makes the attempt
`disputed`, the historical row is retained for investigation but MUST NOT be
returned as valid fenced assurance.

## Preparation and recording invariants

1. Preparation MUST run in one local write transaction and perform no network
   I/O.
2. The lease MUST be fresh, active, effect-bound, and complete enough for the
   requested cumulative assurance.
3. Fenced-required leases MUST use observed completeness, deduplicated replay,
   and full protected-observation coverage.
4. The prepared request MUST be immutable and single-attempt.
5. Adapter execution MUST begin only after the preparation transaction closes.
6. A failed target precondition MUST NOT be retried with a newer token under the
   same lease.
7. An indeterminate attempt MUST retain the same attempt identity for recovery;
   a fresh attempt is forbidden.
8. Only the core may derive `fenced` after validating every returned binding.
9. Result replay MUST be idempotent; conflicting terminal replay MUST become
   disputed, while late transport ambiguity MUST preserve a known terminal fact.
10. Multiple targets require separate receipts and never imply global atomicity.

## Strict HTTP profile

The reference HTTP adapter is a conformance profile for a cooperating target,
not a generic interpretation of status codes.

- `If-Match` MUST contain one strong quoted ETag; weak ETags are rejected.
- Redirects MUST be rejected rather than silently changing origin.
- The attempt ID MUST be used as the target idempotency identity.
- For a known attempt ID, the target MUST verify the original request
  fingerprint and return its recorded result before evaluating the current
  precondition. A fingerprint mismatch MUST be a conflict, not a new effect.
- Request size, response size, and timeout MUST be bounded.
- Reserved headers and header injection MUST be rejected.
- `412` is a terminal `not_committed` precondition failure only for an unseen
  attempt whose effect was not applied.
- Timeout, connection loss, malformed success evidence, `409`, and server errors
  are `indeterminate` unless the adapter contract proves otherwise.
- A success response MUST echo the attempt and request binding and include a
  stable target receipt reference.

HTTP success alone is not fenced evidence. The target must atomically enforce
the declared precondition and deduplication contract.

## Persistence compatibility

SQLite schema version 2 adds target bindings, immutable attempts, target
results, and post-commit receipts. Migration from version 1 MUST occur in one
transaction, preserve every existing row, and create no synthetic fenced
receipt. Existing v1 fenced-required leases have no target binding and remain
ineligible for the new path.

The local `gate_receipts` history remains unchanged. Fenced receipts are stored
separately so a post-commit fact never overwrites a pre-dispatch decision.
