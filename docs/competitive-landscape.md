# Competitive landscape

Reviewed on 2026-07-20 using primary project documentation, standards, and
original papers.

## Finding

TruthLease is defensible only as a narrow cross-framework coherence plane. It
must not grow into another workflow engine, memory store, CDC system, or general
transaction runtime.

| Existing system | What it already solves | TruthLease boundary |
| --- | --- | --- |
| [Restate](https://docs.restate.dev/ai/patterns/durable-agents), [Temporal](https://docs.temporal.io/activity-definition#idempotency), [LangGraph](https://docs.langchain.com/oss/python/langgraph/persistence) | Durable steps, checkpoints, recovery, and application-level idempotency patterns. | Validate whether the external premises of a saved artifact still match, then bind that lineage to a target commit; the target still owns effect idempotency. |
| [HTTP `If-Match`](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.1.1), [Google AIP-154](https://google.aip.dev/154), [Kubernetes `resourceVersion`](https://kubernetes.io/docs/reference/kubernetes-api/definitions/object-meta-v1-meta/) | Established optimistic-concurrency tokens and conditional writes. | Bind a target token to the artifact dependency closure, exact effect, stable attempt, coverage, and post-commit receipt rather than inventing a new token. |
| [`Idempotency-Key` Internet-Draft](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/) | Proposed request-key, fingerprint, replay, and conflict semantics; it is not a final RFC. | Use those semantics as an explicit adapter contract and keep ambiguous outcomes distinct from confirmed commits. |
| [Drasi](https://drasi.io/concepts/) | Change detection, continuous queries, and reactions across data sources. | Map a source change onto dependent agent artifacts, leases, and reconciliation scope. |
| [MCP resources](https://modelcontextprotocol.io/specification/draft/server/resources) | Resource reads and update subscriptions. | Treat updates as hints, re-observe authoritatively, then propagate artifact invalidation. |
| [Bazel Skyframe](https://bazel.build/reference/skyframe), [Salsa](https://salsa-rs.github.io/salsa/reference/algorithm.html) | Dependency tracking and incremental recomputation for controlled computations. | Model remote, expiring, disputed inputs and effect-bound validity leases. |
| [Drools truth maintenance](https://docs.drools.org/8.36.0.Final/drools-docs/docs-website/drools/rule-engine/index.html) | Retract logical facts after their justifications disappear. | Apply the dependency idea to versioned artifacts produced outside one rule engine. |
| [Graphiti](https://github.com/getzep/graphiti) | Temporal facts, provenance, and retrieval-oriented knowledge graphs. | Remain outside memory retrieval and track operational artifacts and use-time eligibility. |
| [STALE](https://arxiv.org/abs/2605.06527), [MemStrata](https://arxiv.org/abs/2606.26511) | Detect or benchmark obsolete facts in agent memory and RAG. | Cover plans, reports, decisions, approvals, and effect eligibility beyond memory retrieval. |
| [MemLineage](https://arxiv.org/abs/2605.14421) | Derivation lineage and action gating against untrusted memory ancestry. | Track temporal version validity and minimal reconciliation rather than source trust alone. |
| [Commit-time authorization](https://arxiv.org/abs/2607.10487) | Fresh, causally bound authority at the durable-effect boundary. | Implement a general artifact coherence runtime with explicit non-security dependencies and reuse. |
| [W3C PROV-O](https://www.w3.org/TR/prov-o/) | Standard provenance vocabulary. | Enforce currentness and single-use leases at runtime. |
| [CloudEvents](https://github.com/cloudevents/spec) | Portable event envelope and identity. | Define observation, artifact, invalidation, and lease semantics inside those events. |

## Build versus integrate

Build here:

- immutable artifact revisions and explicit dependencies;
- exact version vectors and dependency completeness;
- minimal reverse-closure invalidation within configured bounds, with a global
  fail-closed fallback on overflow;
- short-lived effect-bound leases;
- local use-time gate and receipts for accepted consumption;
- immutable target attempts that bind closure, effect, precondition, protected
  scope, and replay contract;
- core-derived post-commit receipts with explicit coverage and ambiguity.

Integrate later:

- CloudEvents for transport;
- W3C PROV for lineage export;
- MCP, Drasi, or Debezium for source change hints;
- Restate or Temporal for durable reconciliation work;
- provider-specific conditional HTTP or datastore compare-and-set adapters;
- [AgenticStrata](https://github.com/aantenore/AgenticStrata) for wider execution
  contracts and evidence;
- [PauseMesh](https://github.com/aantenore/pausemesh) for durable human-approval
  suspension and exactly-once resumption;
- [StageFabric](https://github.com/aantenore/stagefabric) for policy-aware runtime
  placement.

Do not build here:

- a scheduler, message bus, policy language, vector database, model router,
  multi-agent framework, IAM system, or universal compensation engine.

The differentiation is architectural composition, not an unsupported claim that
no adjacent primitive exists.

The alpha digest profile is intentionally Python-implementation-local. Portable
cross-language artifact identity remains future work until a canonicalization
profile and shared golden vectors are published.
