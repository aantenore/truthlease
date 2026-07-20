# Security policy

## Supported versions

TruthLease is pre-release software. Security fixes are applied to the latest
alpha on `main`; no long-term support promise exists yet.

## Reporting

Please use GitHub private vulnerability reporting for this repository. Do not
open a public issue containing an exploit, secret, or sensitive deployment data.

Include the affected version, reproduction, expected invariant, impact, and any
known workaround. Operational traces should be minimized and redacted.

## Boundaries

TruthLease does not authenticate external sources, sandbox Python adapters,
provide tenant isolation, or create a distributed transaction with a target.
The embedding application must allowlist adapter identities and exact endpoints,
secure target credentials, and verify that declared precondition coverage matches
the target implementation. See the
[threat model](docs/threat-model.md) before using the alpha near durable effects.
