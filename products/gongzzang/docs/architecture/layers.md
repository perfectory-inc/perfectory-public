# Layers

This document describes Gongzzang's current dependency direction.

## 1. Layer Rule

Dependency direction:

```text
apps / services
  -> crates/gongzzang-persistence, adapters, route DTOs
  -> crates/*-domain ports and value objects
```

Domain crates must not depend on runtime frameworks, databases, HTTP clients, provider SDKs, or UI code.

## 2. Domain Layer

Domain layer owns business meaning and compile-time rules.

Current examples:

- `crates/listing-domain`
- `crates/listing-photo-domain`
- `crates/user-domain`
- `crates/shared-kernel`
- `crates/real-transaction-domain`
- `crates/court-auction-domain`
- `crates/{bookmark,search-history,analysis-report,notification}-domain`
- `crates/{audit-log,outbox-event}-domain`

Allowed dependencies:

- shared value objects
- repository ports
- pure domain errors
- serializable DTOs when they are domain-owned

Forbidden dependencies:

- `reqwest`
- `sqlx`
- Axum
- Next.js
- provider-specific response structs

## 3. Adapter Layer

Adapters translate between domain ports and infrastructure.

Current examples:

- `crates/gongzzang-persistence`
- `services/gongzzang-api/src/foundation_parcel_lookup.rs`
- `services/gongzzang-api/src/building_reader.rs`
- `services/gongzzang-api/src/photo_upload.rs`
- `services/gongzzang-outbox-publisher/src/foundation_lakehouse_registry.rs`

Adapters may use `reqwest`, `sqlx`, S3/R2 clients, or Redis clients when the owning boundary requires them.

## 4. Service Layer

Services compose repositories, adapters, route state, middleware, and startup policy.

Current services:

- `services/gongzzang-api`
- `services/gongzzang-outbox-publisher`

## 5. App Layer

Frontend apps own user interaction and product UI.

Current app of record:

- `apps/web`

Important frontend boundaries:

- user-facing strings should go through typed i18n;
- public API access should go through approved proxy/client paths;
- Foundation Platform event receiver is a narrow integration route, not a general Catalog client.

## 6. Policy And Registry Layer

Cross-cutting rules are registered in JSON/policy files and checked by scripts.

Important registries:

- `docs/architecture/traffic-auth-policy-registry.v1.json`
- `docs/architecture/foundation-platform-boundary.v1.json`
- `docs/architecture/platform-integration/index.v1.json`

Generated or derived runtime files must follow those registries.

## 7. Build/Verification Layer

`cargo` (Rust) and `pnpm` + `Turborepo` (frontend) are the build, test, and
verification SSOT (ADR-0002; ADR-0044 reversed the abandoned Bazel transition).

Current state:

- Rust is built/tested/linted with `cargo` (`cargo build`, `cargo test`, `cargo clippy`);
- the frontend is built/tested with `pnpm` + `turbo` (`turbo run build`, `turbo run test`, `turbo run typecheck`);
- off-the-shelf tools (gitleaks, lefthook, cargo-deny) and a small Rust `repo-guard` cover repo-specific guardrails.

The goal is reproducible verification through the native toolchains; there is no
Bazel build graph and no transition ratchet (both removed per ADR-0044).

## 8. Guardrails

Layer changes must preserve the Foundation Platform dependency boundary and the
platform-integration policy. The Foundation Platform catalog boundary is enforced by
`scripts/lefthook/foundation-ownership-boundary.sh` and the boundary contract
`docs/architecture/foundation-platform-boundary.v1.json`.
