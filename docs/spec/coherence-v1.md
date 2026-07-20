# TruthLease Coherence Contract v1

- Status: alpha
- Contract identifier: `dev.truthlease.coherence.v1`
- Read-model schema:
  [`schemas/v1/coherence.schema.json`](../../schemas/v1/coherence.schema.json)

This document defines the application semantics implemented by the TruthLease
Python alpha. It is not a transport standard. The linked JSON Schema validates a
subset of exported observation, artifact, lease, and gate-decision read models;
it does not encode the whole protocol, transactions, lifecycle-event semantics,
revalidation policy, or reconciliation behavior.

Normative terms `MUST`, `MUST NOT`, `SHOULD`, and `MAY` are used in their usual
standards sense.

## Identifiers and digests

- Observation keys and artifact IDs MUST be non-empty UTF-8 strings scoped by
  the embedding application.
- Revisions MUST be positive integers and immutable.
- Version tokens are opaque strings and MUST use exact comparison.
- Each observation has a positive, store-local generation. Updating an existing
  key MUST compare-and-swap the generation captured before the authoritative
  read. A generation mismatch MUST reject the delayed write.
- Content fingerprints MUST identify their algorithm, for example
  `sha256:<hex>`.
- The v1 Python implementation uses versioned, domain-separated canonical JSON
  digests. It does not claim general RFC 8785 compatibility or cross-language
  digest identity.
- Digest equality is an implementation-local guarantee for the same Python
  profile and version. A future cross-language profile MUST define its supported
  value domain, byte serialization, numeric rules, and shared golden vectors
  before claiming interoperable artifact identity.

## Observation states

```text
current | disputed | unknown | revoked
```

- `current`: the latest authoritative state known locally.
- `disputed`: the same version identity arrived with different content.
- `unknown`: the source cannot currently establish validity.
- `revoked`: the source state is terminally unusable.

The `0.1` public Python API and CLI commit authoritative `current` observations
and derive `disputed` conflicts. `unknown` and `revoked` are reserved contract
states for future source/admin adapters; they are not public transition commands
in this alpha. Time-bounded uncertainty must use `valid_until`.

A watcher notification MUST NOT promote an observation to `current` without an
authoritative read. Duplicate `(source, event id)` pairs with identical payload
are no-ops. Reusing the identity with different content MUST fail.
Observation dependency edges and lease snapshots MUST bind both the opaque
source version and the local generation. This prevents an ABA transition back to
the same version and digest from silently reviving an invalidated artifact.

## Artifact states

```text
fresh | stale | disputed | unknown | revoked
```

Clients MUST NOT set artifact state. The runtime derives it from dependency
state and explicit administrative revocation. Once an immutable revision is
stale, a producer MUST publish a new revision after reconciliation.

Read APIs MUST report effective status using the current time as well as the
stored projection. A persisted `fresh` projection MUST NOT cause an artifact to
be reported effectively fresh after a required observation's `valid_until` has
passed. Lease acquisition and lease validation MUST apply the same rule.

## Dependency completeness

```text
unknown | declared | observed
```

- `unknown` artifacts MUST NOT receive effect leases.
- `declared` means the producer listed dependencies but capture was not observed
  by the integration boundary.
- `observed` means an adapter or SDK captured reads used to produce the artifact.

Completeness does not prove that no hidden dependency exists.
An artifact's effective completeness MUST be no higher than the minimum
completeness of every required artifact in its transitive closure.

## Assurance

```text
declared < observed < revalidated < fenced
```

- `declared`: listed dependencies match local state.
- `observed`: captured dependencies match local committed observations.
- `revalidated`: an allowlisted adapter reports a fresh authoritative re-read
  bound to the exact lease and proposed effect.
- `fenced`: a target result proves atomic enforcement of the bound precondition
  in addition to satisfying all applicable lower assurance levels.

Pre-gate revalidation evidence MUST include its authority, evidence digest,
level, and observation time, and MUST bind the lease ID, closure digest, effect
digest, target, and operation. Its authority MUST appear in the embedding
application's configured allowlist and its observation time MUST fall within the
configured freshness window. Evidence statements are not self-authenticating;
the embedding application remains responsible for authenticating the adapter.

Assurance is cumulative. A future `fenced` result MUST preserve the underlying
local-currentness decision and any required revalidation evidence, then add a
target result bound to the same operation. The `0.1` local gate MUST NOT emit
`fenced`: it does not execute the target effect and therefore cannot observe
atomic target enforcement. That level is reserved for a later post-commit result
contract.

## Lease invariants

1. A lease MUST be issued only from a `fresh` artifact with non-unknown
   dependency completeness.
2. Its closure digest MUST bind the artifact, required dependency vector,
   effect digest, target, operation, assurance requirement, and completeness.
3. A lease MUST NOT authorize a different effect payload.
4. A lease MUST fail after expiry, invalidation, consumption, revocation, or an
   application-selected boot-epoch change.
5. Gate validation and lease consumption MUST share one local transaction.
6. An unfenced gate MUST state the residual source/target race explicitly.
7. Non-consuming lease validation MUST check local currentness and MUST deny when
   the lease requires revalidation or fencing that local state cannot establish.

## Bounded invalidation

Within configured graph-depth and impact-node bounds, invalidation MUST equal the
smallest required transitive closure. If traversal exceeds either bound, the
runtime MUST NOT return a partial closure as a successful minimal result. It MUST
fail closed in the same local transaction by setting all artifacts to `unknown`
and invalidating all active leases. This global fallback deliberately gives up
partial reuse until the graph can be inspected or rebuilt.
An incomplete invalidation result MUST NOT expose a partial ID list as the full
impact set. It MUST expose `complete=false`, `fail_closed=true`, a diagnostic,
and aggregate invalidated-lease count. The `0.1` fault is sticky; supported
recovery is an inspected rebuild into a fresh store, never a blind clear that
could reactivate prior authority.

## Gate result

```json
{
  "allowed": false,
  "code": "STALE_LEASE",
  "lease_id": "lease_...",
  "assurance": "observed",
  "guarantee": "No effect is authorized by this decision (STALE_LEASE)...",
  "changed_dependencies": ["policy/release"]
}
```

For denials, `assurance` is contextual only: it reports the dependency-capture
or evidence level established before denial. It does not assert currentness and
never authorizes an effect.

Stable denial codes include:

```text
STALE_LEASE
LEASE_EXPIRED
LEASE_CONSUMED
BOOT_EPOCH_CHANGED
COHERENCE_FAULT
EFFECT_MISMATCH
DEPENDENCY_COMPLETENESS_INSUFFICIENT
ADDITIONAL_ASSURANCE_REQUIRED
ASSURANCE_INSUFFICIENT
ASSURANCE_EVIDENCE_REQUIRED
ASSURANCE_EVIDENCE_INVALID
ASSURANCE_UNSUPPORTED
```

## Event envelope

The accepted-lifecycle event log uses CloudEvents-compatible identity concepts
while remaining a local projection in v1:

```json
{
  "source": "urn:truthlease:runtime",
  "id": "evt_...",
  "type": "dev.truthlease.artifact.published.v1",
  "subject": "artifact/release-plan/revision/2",
  "time": "2026-07-20T12:00:00Z",
  "payload_digest": "sha256:...",
  "data": {}
}
```

Transport adapters MAY wrap this representation in CloudEvents 1.0 structured
mode, but MUST preserve `source + id` deduplication and the payload digest.
Structured denial decisions are returned to the caller but are not guaranteed to
be present in this event log. An embedding application that requires a complete
attempt audit MUST persist denials separately.

## Interoperability

- An A2A artifact MAY carry the TruthLease artifact ID, revision, and closure
  digest in extension metadata.
- An MCP resource update SHOULD be treated as a hint and trigger an
  authoritative observation refresh.
- A workflow checkpoint MAY persist a lease ID, but resumption MUST validate the
  lease again before dispatch.
- A provenance system MAY map an artifact dependency to W3C PROV derivation,
  but provenance alone does not satisfy the use-time gate.
