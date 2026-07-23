# ADR 0021 - Adopt Horizontal Platform Architecture

| Field | Value |
|---|---|
| Date | 2026-07-02 |
| Status | Accepted |
| Governing ADR | `../../../../products/gongzzang/docs/adr/0048-horizontal-platform-redefinition.md` |

## Context

Shared data, identity, and intelligence have different security boundaries, scaling patterns, and
release lifecycles. Keeping them in one platform would blur ownership and invite direct database
coupling.

## Decision

The final architecture has three horizontal platforms:

```text
foundation-platform
identity-platform
intelligence-platform
```

- `foundation-platform` owns Catalog, collection, lakehouse, canonical public/reference data,
  lineage, quality, and normalization proposal governance.
- `identity-platform` owns staff identity, service identity, authentication policy, and
  authorization.
- `intelligence-platform` owns model execution, proposal generation, and vector/RAG processing.

Gongzzang and future products consume published APIs, events, and immutable artifacts. Cross-platform
direct database access and compatibility aliases are forbidden.

## Naming Rules

- System slugs use `foundation-platform`, `identity-platform`, and `intelligence-platform`.
- Brand display uses `Foundation Platform`, `Identity Platform`, and `Intelligence Platform`.
- Fresh databases, buckets, services, packages, environment variables, and events use final names
  only.
- Contract versions such as `/v1`, `.v1`, and `schema_version: 1` remain where they express a real
  public API, event, or schema compatibility boundary.

## Consequences

- Each platform can deploy and scale independently.
- Identity data is no longer stored in or foreign-keyed from Foundation.
- AI remains a proposal producer; Foundation remains the canonical decision authority.
- Prelaunch migration helpers and compatibility names are removed rather than carried forward.
