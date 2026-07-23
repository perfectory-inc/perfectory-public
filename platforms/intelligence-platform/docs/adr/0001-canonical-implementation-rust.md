# ADR-0001: Rust is the canonical intelligence-platform implementation; the Python service is retired

- **Status:** Accepted
- **Date:** 2026-07-08
- **Deciders:** Platform owner
- **Architecture:** [Intelligence Platform Architecture](../architecture.md)

## Context

Two implementations of the intelligence platform coexisted:

- `intelligence-platform-rs` (Rust + Axum, now renamed to `intelligence-platform`) — the stated production direction, with inbound auth, an admission stack, a durable Postgres outbox with lease-based claiming, Idempotency-Key submission, and CI against a live Postgres.
- the former `intelligence-platform` Python + FastAPI tree — described in its own README as the "production" service and elsewhere as a "reference contract."

The audit confirmed this dual track was actively harmful:

- **Contract drift (G1/RC7):** the two clients spoke different wire contracts to the same Foundation intake — different request shapes, status enums (`queued|rejected` vs `submitted|accepted|rejected|queued`), idempotency-key formulas (4-field vs 3-field), default paths, and header sets. No process artifact declared which was authoritative.
- **The Python service was not deployable or safe:** zero inbound authentication with `tenant_id` trusted from the request body (an unauthenticated cross-tenant write surface, rated C0); a hardcoded stub `/v1/rag/query`; a SHA-256 hash used as the embedding; synchronous clients blocking the async event loop; per-request client construction; a non-functional in-memory outbox; no Dockerfile; no CI; no dependency lockfile; and `.pyc` artifacts built on CPython 3.14 while its own tooling targeted 3.12.

Maintaining both meant every contract change had to land in two places or drift; the Python surface added attack surface and false capability with no path to production.

## Decision

**The Rust workspace at the repository root is the single canonical implementation and source of truth for the platform boundary.** Its former `intelligence-platform-rs` path is historical only. The retired Python project that previously occupied `intelligence-platform/` is **removed from the repository** and is no longer part of the deployable estate, the contract-reference set, or CI.

The wire contract to the Foundation Platform is defined solely by the Rust client and its schemas under `schemas/`.

## Consequences

- The unauthenticated cross-tenant write surface (C0), the stub RAG path, the hash-embedding, the event-loop blocking, and the Python-side contract drift are eliminated by removal rather than by parallel maintenance.
- Any capability that existed only in the Python prototype is now a Rust work item, tracked in the master hardening plan. Notable items still to be built in Rust (not lost, just relocated): real retrieval/RAG wiring, an embedding port with a non-toy adapter, and source-authority ordering in the core (see plan Waves P1/P2 and the earlier RAG design docs). *(2026-07-20 note: that hardening plan and the RAG design docs were not migrated into this monorepo — they exist only in pre-absorption history/archives. A fresh design doc is required before the RAG work item can restart.)*
- Documentation that referenced the Python service as production or as a reference contract is corrected.

## Recovery

The retired Python tree and unique pre-cutover experiments are retained in the private transition
archive governed by [root ADR-0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md),
not in the public canonical history. Inspect any recovery snapshot read-only outside the live
repository and never restore it over the canonical Rust path. Port only individually reviewed
capabilities through a new design and normal pull request.

## Related

- Current module and platform boundaries: `docs/architecture.md`
