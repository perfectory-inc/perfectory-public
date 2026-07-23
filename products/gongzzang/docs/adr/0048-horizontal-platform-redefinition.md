# ADR-0048: Horizontal Platform Redefinition

| Field | Value |
|---|---|
| Date | 2026-07-02 |
| Status | Accepted |
| Decision owner | perfectoryinc |
| Supersedes | ADR-0030/0031 core-centered platform naming, where it conflicts with this ADR |
| Related | ADR-0030, ADR-0031, ADR-0032, ADR-0034, ADR-0045, foundation implementation ADR-0021 |

## Context

The previous cross-repo model used a single shared core as the internal hub for
Catalog, Workforce/Authz, lakehouse, collection, and governance. That model
helped extract common data from product services, but it also created a vertical
gravity well: any shared capability tended to move into one increasingly broad
core service.

The target architecture is now a horizontal platform architecture. Shared
capabilities are not grouped under one generic core. They are grouped by durable
platform responsibility.

## Decision

Adopt the following top-level platform names and ownership boundaries:

```text
foundation-platform
identity-platform
intelligence-platform
```

The old core name is retired as a platform name. It must not be used for new
external resources, runtime labels, contracts, service names, package names, or
architecture documents except when explicitly referring to historical migration
evidence.

### foundation-platform

`foundation-platform` owns canonical shared data and data infrastructure:

- public/canonical Catalog data
- industrial complex, parcel, building, manufacturer, spatial, and map anchor facts
- Bronze/Silver/Gold lakehouse data
- R2/Iceberg/Trino/Spark lakehouse integration
- source catalogs, raw lineage, collection ledger, Bronze commit protocol
- canonical normalization proposal inbox for data it owns
- approved canonical apply commands for data it owns
- data governance, retention, lineage, and promotion policy

The current Catalog, lakehouse, collection, and pipeline responsibilities move
to foundation-platform.

### identity-platform

`identity-platform` owns shared identity, authorization, and principal policy:

- staff identity
- service identity and service tokens
- session verification
- role/permission/policy model
- cross-service authorization contracts
- audit principal resolution
- identity-related outbox/events

The current Workforce/Authz responsibilities move to identity-platform.

Product-user identity remains product-owned unless explicitly moved. For
example, Gongzzang B2C users remain `gongzzang` owned; staff/admin identity is
identity-platform owned.

### intelligence-platform

`intelligence-platform` owns AI execution and proposal generation:

- model calls and model routing
- embeddings/vector indexing and retrieval
- prompt/model/policy profiles
- normalization proposal generation
- validation/evaluation of AI-generated candidates
- AI retry/outbox state
- developer UI integrations such as Open WebUI, when used for development

`intelligence-platform` does not own canonical data. It may submit proposals to
foundation-platform or other owner services through approved APIs. It must not
write owner databases, Silver/Gold tables, or canonical records directly.

## Naming Rules

New platform-level resources use the final platform names:

```text
foundation-platform-*
identity-platform-*
intelligence-platform-*
```

Examples:

```text
foundation-platform-lakehouse-prod
foundation-platform-r2
foundation-platform.catalog.*
source_system = foundation-platform-r2
```

Legacy core prefixes must not be used for new resources. Existing resources may
remain only as migration inputs, must be marked legacy, and must be retired after
verified migration.

## Boundary Rule

The platform that owns the data owns the approval gate for that data.

```text
AI proposes.
The owning platform governs.
Humans approve when required.
The owning platform command writes canonical state.
```

Therefore:

- foundation-platform owns Catalog normalization proposal inboxes for foundation
  canonical data.
- identity-platform owns identity-policy approval gates.
- product services own product-specific gates such as listing moderation or
  site presentation approval.

There is no single universal approval service at this scale.

## Migration Strategy

1. Record this ADR as the cross-repo decision source.
2. Add thin pointer ADRs in affected repos.
3. Treat the current core repository/path as a legacy implementation location
   only until it is renamed or replaced.
4. Rename documentation, resource prefixes, environment variables, and runtime
   labels when touched.
5. Create new external resources with final names instead of renaming in place
   when the provider does not support rename, such as R2 buckets.
6. Move identity responsibilities behind identity-platform contracts before
   physically splitting repositories.
7. Move or rename physical repositories only after contracts, CI, deployment
   names, and data migration are stable.

## Non-Goals

- No immediate forced repository move in this ADR.
- No direct database sharing across platforms.
- No Kafka/Kubernetes requirement is introduced by this naming decision.
- No AI service receives canonical write permission.
- No product-specific semantics move into foundation-platform.

## Consequences

- Positive: shared capabilities stop accumulating under one vertical core
  umbrella.
- Positive: data, identity, and AI responsibilities are separated at the
  platform level.
- Positive: future services can depend on horizontal contracts rather than one
  overloaded core service.
- Cost: existing documentation and resource names must be migrated carefully.
- Cost: legacy physical paths may remain temporarily, but they are not valid
  platform names.

## Reassessment Triggers

- If repository count or platform count grows enough that Gongzzang is no
  longer appropriate as cross-repo governance home, create a dedicated
  architecture/governance repository as described in ADR-0045.
- If identity-platform becomes deployable independently, create a repo-local ADR
  for its physical extraction plan.
- If foundation-platform is physically renamed, supersede this ADR with the
  final path/resource migration evidence.
