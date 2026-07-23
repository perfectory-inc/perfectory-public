# ADR 0001: Monorepo governance and conventions

> Operational plans and dated implementation evidence referenced by this decision are retained in
> the private transition archive under [ADR-0007](./0007-public-code-private-operations-boundary.md).

- Status: Accepted
- Date: 2026-07-19

## Context

`products/gongzzang` and `platforms/{foundation,identity,intelligence}-platform` were
separate repositories before being absorbed into this monorepo during the private transition.
The public canonical repository starts from a reviewed publication snapshot; pre-publication
history remains in the private transition archive governed by ADR-0007. The consolidation moved
files but not the operating model: every area kept its own `.github/` (inert — GitHub reads
workflows only from the repo root), toolchain pin, and birth-era conventions. The resulting
drift affected health endpoints, migration names, Rust toolchains, package names, security
scanning, and documentation paths. Area ADRs (gongzzang 0048, foundation 0021) still
describe a multi-repo world.

## Decision

1. **This repo is the monorepo SSOT** for all four areas. The multi-repo layout described in
   area ADRs is historical. Area paths: `products/gongzzang`,
   `platforms/{foundation,identity,intelligence}-platform`.
2. **GitHub config lives only at root `.github/`.** Area workflows use path filters +
   `defaults.run.working-directory`. Subdirectory `.github/workflows/` are forbidden
   (guard: `scripts/guard/no-subdir-github.sh`).
3. **One Rust toolchain monorepo-wide: 1.96.0** (ONE root `rust-toolchain.toml` —
   rustup resolves it by parent-directory walk-up; area-local toolchain files are
   forbidden as they would shadow it; `rust-version` stays per workspace manifest).
   Bumps update all four in one commit (guard: `scripts/guard/toolchain-consistency.sh`).
4. **axum 0.8** in every workspace that serves HTTP.
5. **Health endpoints:** liveness `/healthz`, readiness `/readyz`, metrics `/metrics`.
   Dependency-specific diagnostics nest under `/readyz/<dep>` (guard:
   `scripts/guard/health-route-conformance.sh`).
6. **Route namespace:** platform-native HTTP APIs mount under `/<area>/v1/...`
   (`/catalog/v1` and `/map/v1` are foundation's published segments and stay).
   Recorded exception: the OpenAI-compatible surface (`/v1/chat/completions`,
   `/v1/models`) keeps its ecosystem-mandated paths.
7. **Migrations:** `YYYYMMDDHHMMSS_<snake_case>.sql` (14-digit UTC), sqlx default, in each
   area's `migrations/` (guard: `scripts/guard/migration-naming.sh`). Pre-launch renames of
   existing files are permitted once; local databases are recreated.
8. **OpenAPI artifacts are JSON** at `docs/openapi/<name>.v<major>.json` per area.
9. **PostgreSQL 17** everywhere; container images SHA-pinned (inherits gongzzang ADR-0028).
10. **Cargo package names are globally unique across the monorepo**; generic lib names take
    an `<area>-` prefix (guard: `scripts/guard/unique-package-names.sh`).
11. **Env var prefixes:** each area namespaces its variables (`FOUNDATION_*`, `IDENTITY_*`,
    `INTELLIGENCE_*`, gongzzang legacy unprefixed vars grandfathered); cross-area consumption
    uses the owning area's prefix.
12. **Supply chain:** one root worktree secret scan (gitleaks, root `.gitleaks.toml`,
    `.github/workflows/secret-scan.yml`) and per-workspace cargo-deny (`deny.toml`) run in CI
    for all four areas. One dependency bot: root dependabot (area-local Renovate retired).
13. **No pre-merge sibling paths** (`../<former-sibling>/...`, `<local-home>/<repo>`)
    in tracked files (guard: `scripts/guard/no-stale-sibling-paths.sh`).

## Guard policy (gongzzang product-first rule applies monorepo-wide)

Every guard must identify the concrete failure mode it prevents. New guards require
reproducible evidence of that failure mode, not speculation; operational evidence stays in
the private operations system under ADR-0007.

## Consequences

- The completed one-time alignment covered toolchain bumps, axum 0.8, health/route renames,
  migration renames, crate renames, and docs rewiring.
- Area docs describing cross-REPO flows now mean cross-AREA; wording updated opportunistically.
- Renamed migrations invalidate previously-applied local sqlx histories; recreate local DBs.
