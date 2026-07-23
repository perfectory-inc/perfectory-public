# ADR-0050: Dawneer Workbench and Internal Admin Surface

| Field | Value |
|---|---|
| Date | 2026-07-17 |
| Status | Accepted |
| Decision owner | perfectoryinc |
| Related | [ADR-0048](./0048-horizontal-platform-redefinition.md), [ADR-0049](./0049-identity-platform-contract-design.md) |

## Context

Dawneer already serves the B2B industrial-complex management and site-authoring workflow. The concrete
shared staff workflow today is human review of normalization proposals before Foundation Platform
applies canonical-data changes. Creating a separate admin application for that workflow, or moving its
domain ownership into Dawneer, would duplicate product work before launch and blur the horizontal
platform boundaries established by ADR-0048.

## Decision

Dawneer remains the B2B industrial-complex workbench and is the default and only shared staff-facing
admin composition surface while the current product and team boundaries hold. This is a deployment
default, not an irreversible global singleton. Dawneer owns presentation and workbench state only:
navigation, layouts, interaction state, and workflow composition. It consumes versioned, published
contracts and does not become the system of record for the domains it presents.

Domain ownership remains unchanged:

- Foundation Platform owns canonical catalog data, lakehouse, and collection APIs.
- Identity Platform owns staff and service identity, authentication, authorization, and policy APIs.
- Intelligence Platform owns AI runtime and proposal-generation APIs.
- Gongzzang owns its B2C product domains and APIs, including Gongzzang B2C users.

Staff authentication and authorization come from Identity Platform. Current authorization uses RBAC
and explicit capability checks. We will not preselect or deploy OpenFGA, SpiceDB, or another ReBAC
engine. Revisit that engine decision only when actual hierarchical or per-object delegation exists.
Reconsider splitting the admin surface only when a real security boundary or independently owned team
requires separate deployment and access control.

## Consequences

- Staff get one composition surface without duplicating domain data or business rules.
- Each owning service remains independently responsible for its API, policy enforcement, and audit
  records; Dawneer cannot grant authority by UI state alone.
- This decision adds no new authorization engine, infrastructure, domain migration, or speculative
  admin framework before a user-facing workflow requires it.
