# ADR 0033 - Focused Architecture Enforcement

| Field | Value |
|---|---|
| Date | 2026-05-11; amended 2026-07-15 |
| Status | Accepted with product-first scope |
| Architecture | [ADR 0048](./0048-horizontal-platform-redefinition.md) |

## Context

The original decision proposed seven broad architecture guardrails. Experience
showed that broad registries and self-validating evidence systems added ceremony
without protecting product behavior. The product-first rules in `AGENTS.md`
supersede that approach.

## Decision

Keep only focused enforcement tied to a demonstrated failure mode:

| Enforcement | Real failure prevented |
|---|---|
| Cargo/package dependency direction | Product or domain code importing another platform's internals |
| Foundation ownership boundary check | Catalog clients, ETL, or canonical tables returning to Gongzzang |
| Fresh migration smoke | Deleted legacy tables or missing final tables entering a new deployment |
| API/event contract tests | Producer and consumer silently disagreeing on a published wire contract |
| gitleaks and dependency audits | Secrets or known-vulnerable dependencies entering source control |
| formatter, clippy, typecheck, and focused tests | Build and behavior regressions in changed code |
| file-size limit | Unreviewable source files growing beyond the repository rule |

No guard may exist only to validate another guard, registry, checklist, or
evidence bundle. A new guard requires one sentence naming the real incident it
prevents.

## Current Enforcement Sources

- `AGENTS.md` defines repository-wide product-first and boundary rules.
- `docs/architecture/foundation-platform-boundary.v1.json` defines Foundation
  ownership for the focused boundary check.
- Cargo manifests define compile-time package dependencies.
- SQL migrations define the database schema.
- API/event schemas and consumer tests define published contracts.

## Consequence

Architecture remains machine-enforced where a bypass can damage product data,
security, or runtime behavior, while obsolete governance ceremony is deleted
instead of renamed.
