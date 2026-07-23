# ADR-0045: ADR placement and cross-area governance

| | |
|---|---|
| Date | 2026-06-20 |
| Status | **Superseded by root ADR-0001** |
| Decision owner | perfectoryinc (platform owner) |

## Context

Before monorepo consolidation, an architecture decision could affect multiple independently managed
codebases, which made its canonical home ambiguous. The original decision used Gongzzang as the
temporary home for shared ADRs.

That rule no longer matches the repository topology. Keeping it would create two governance homes
inside one monorepo.

## Current decision

- A decision scoped to one product or platform area lives in that area's `docs/adr/` directory.
- A decision that governs multiple areas or repository-wide mechanics lives in root `docs/adr/`.
- Area ADRs may point to a root ADR but must not duplicate its normative contract.
- External products or consumers, including Dawneer, integrate through published contracts; their
  existence does not create another architecture SSOT.

Root ADR-0001 and the root `AGENTS.md` are authoritative when this historical ADR conflicts with the
current monorepo layout.

## Consequences

- Decision placement follows ownership and scope.
- Cross-area rules have one discoverable root home.
- A separate governance repository is unnecessary while this monorepo remains the code SSOT.

## References

- [Root ADR-0001](../../../../docs/adr/0001-monorepo-governance-and-conventions.md)
- [Root AGENTS.md](../../../../AGENTS.md)
