---
name: respond-to-guard-failure
description: Use when scripts/guard/monorepo-guard.sh (or one of its seven guards) fails in pre-push or CI — what each failure means, the fix pattern, and what you must not do to make it pass.
---

# Respond to a monorepo guard failure

Run all seven: `bash scripts/guard/monorepo-guard.sh`.
Run one: `bash scripts/guard/<name>.sh`.
Each script's header comment documents the real incident it prevents — read it
before "fixing" anything.

## The seven guards

### 1. no-subdir-github

- Failing means: a `.github/workflows/` exists inside `products/` or
  `platforms/`. GitHub never executes those — on 2026-07-19 all four areas'
  pipelines were silently dead this way.
- Fix: move the workflow into root `.github/workflows/` with a path filter +
  `defaults.run.working-directory: <area>`, then delete the subdir copy.
  (Note: agents must not edit `.github/` workflows without explicit approval.)

### 2. toolchain-consistency

- Failing means: root `rust-toolchain.toml` drifted from the ADR-0001 pin
  (1.96.0), an area-local `rust-toolchain`/`rust-toolchain.toml` exists
  (rustup prefers the closest file — it would silently shadow the root SSOT),
  or a Dockerfile `FROM rust:` image doesn't match `rust:1.96.0-*`.
- Fix: delete area-local toolchain files; align Dockerfile base images to the
  pin. A deliberate toolchain bump updates the root pin + every Dockerfile +
  the `PIN` variable in `scripts/guard/toolchain-consistency.sh` in ONE commit.

### 3. migration-naming

- Failing means: a file in an area `migrations/` doesn't match
  `YYYYMMDDHHMMSS_<snake_case>.sql` (ADR-0001 §7; observed 3 naming schemes
  pre-merge caused sqlx version-ordering chaos).
- Fix: rename to the 14-digit form (see the `add-migration` skill; prefer
  `sqlx migrate add --timestamp`). Renaming an already-applied file
  invalidates local sqlx history — recreate local DBs.

### 4. unique-package-names

- Failing means: two workspaces declare the same `[package] name` (observed:
  `outbox-publisher`, `normalization-domain` duplicated — blocks workspace
  unification/publishing).
- Fix: rename the generic crate with an `<area>-` prefix (e.g.,
  `foundation-outbox-publisher`), update all dependents and any docs/commands
  that name the old crate.

### 5. no-stale-sibling-paths

- Failing means: a LIVE doc/config/code file references pre-merge sibling
  paths (`../<area>-platform` sibling-repo forms, `Desktop/<old-repo>`
  absolute paths). Observed: gongzzang AGENTS.md routed agents to a `../`
  sibling foundation-platform path, which no longer exists.
- Fix: rewrite to monorepo paths (`products/gongzzang`,
  `platforms/<area>-platform`; from inside an area use
  `../../platforms/...`). Archive/history paths (`docs/superpowers/**`,
  `*/docs/adr/*`, `*/docs/migration/*`, `*/docs/research/*`) are excluded by
  the guard — do not "fix" history that the guard doesn't flag.

### 6. health-route-conformance

- Failing means: a service registers `/health`, `/health/live`,
  `/health/ready`, `/ready`, or `/healthz/ready` routes (observed 4 styles
  across 4 areas; probes/monitors must not need per-area knowledge).
- Fix: liveness `/healthz`, readiness `/readyz`, metrics `/metrics`;
  dependency-specific diagnostics nest under `/readyz/<dep>` (ADR-0001 §5).

### 7. no-adhoc-cargo-lint

- Failing means: a `.github/workflows/` file runs a raw `cargo clippy` or
  `cargo fmt` outside `cargo xtask`. That is exactly how the flags drifted
  across areas (clippy `--all-features`/`--locked` mismatches) and broke
  local/CI parity — the incident behind ADR-0004.
- Fix: replace the raw `cargo fmt`/`cargo clippy`/`cargo test` trio with a
  single `cargo xtask verify <area>` step (run from the repo root, i.e.
  `working-directory: ${{ github.workspace }}` when the job defaults to an
  area). Verification policy is data in `tools/xtask`, never re-hand-rolled
  in YAML.

## Never do this

- Do not push with `--no-verify` / skip hooks to get past a red guard.
- Do not edit the guard script to make your case pass. Guards encode ADR-0001
  decisions; if the guard itself is wrong, that is an ADR change (guard
  policy: every guard must answer "what real incident does failing prevent?").
