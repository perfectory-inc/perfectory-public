# Public Object-Storage Tiles Integration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the validated Martin/PostGIS/PMTiles tile slice onto Public `main` without importing private Git history.

**Architecture:** Start from Public's parentless `main`; transfer only the tile branch's file-level changes. Do not merge the private-based branch because its history is intentionally unrelated. Preserve Public publication guards and integrate through a protected PR.

**Tech Stack:** Rust 1.96.0 in Docker, Postgres/PostGIS, Martin, PMTiles, Cloudflare R2 or local fallback, Bash, Cargo xtask, pnpm.

## Global Constraints

- `perfectory-inc/perfectory-public` is the canonical origin; never push development work to the private legacy repository.
- Never force-push or commit directly to Public `main`; use a PR and required checks.
- Do not copy private Git history, secrets, production R2 objects, or real collected data.
- Keep existing Postgres catalog and listing ST_AsMVT serving intact.
- Verification SSOT is `cargo xtask verify <area>` in Docker with Rust 1.96.0.

---

### Task 1: Create a Public-based feature branch and freeze the transfer allowlist

**Files:**
- Create: `docs/superpowers/plans/2026-07-23-public-object-storage-tiles-integration.md`
- Source reference only: `private-backup/feat/object-storage-first-tiles-slice`

**Interfaces:**
- Consumes: Public `main` SHA `569ec1513fa68116021f35a064ceba429a01628e` and the tile branch diff against `private-backup/main`.
- Produces: `feat/public-object-storage-tiles` based on Public `main`.

- [ ] **Step 1: Prove the histories must not be merged.** Run `git merge-base main private-backup/feat/object-storage-first-tiles-slice`; expected output is empty because Public is parentless.
- [ ] **Step 2: Create the branch.** Run `git switch -c feat/public-object-storage-tiles`; expected `git status --short` is empty.
- [ ] **Step 3: Capture the allowlist.** Run `git diff --name-status private-backup/main...private-backup/feat/object-storage-first-tiles-slice`; expected paths are limited to the audited tile slice: `.github/workflows/foundation-ci.yml`, `.github/workflows/gongzzang-frontend.yml`, `docs/adr/0006-object-storage-first-serving.md`, `docs/superpowers/plans/2026-07-21-object-storage-first-tiles-slice.md`, Foundation tile tests/seeds/runbook, Gongzzang vector manifest/tests, `scripts/tiles/*`, and `scripts/verify/integration.sh`.

---

### Task 2: Transfer tile files while preserving Public guards

**Files:**
- Modify/create only the Task 1 allowlist.
- Preserve: required workflow terminal `working-directory: .`, identity strict-schema fixture, publication and secret guards.

**Interfaces:**
- Consumes: the allowlisted diff and Public versions of overlapping files.
- Produces: Martin configs, proof harness, manifest, runbook, and contract tests on the Public branch.

- [ ] **Step 1: Apply new tile-only files.** Use the allowlist and `git diff --binary private-backup/main...private-backup/feat/object-storage-first-tiles-slice -- <path>`; inspect every new file before staging.
- [ ] **Step 2: Reconcile modified files manually.** For each overlap, compare the Public file with the tile diff. Keep Public's workflow checkout/root-directory protections and add only the tile behavior; never take an entire private branch file.
- [ ] **Step 3: Run guards.** Run `bash scripts/guard/monorepo-guard.sh`, `bash scripts/guard/check-workflow-policy.sh`, and `git diff --check`; expected all pass.

---

### Task 3: Prove dynamic and static lanes on Public

**Files:**
- Test: `scripts/tiles/tiles-slice-proof.sh`
- Test: Foundation tile contract/decoder/harness tests
- Test: `products/gongzzang/apps/web/tests/unit/map/vector-tile-manifest.test.ts`

**Interfaces:**
- Consumes: PostGIS fixture, pinned Compose images, Martin configs, local manifest, and R2 environment variables when present.
- Produces: decoded dynamic/static feature evidence and manifest-to-source-layer evidence.

- [ ] **Step 1: Run `bash scripts/tiles/tiles-slice-proof.sh`.** Expected exit 0, dynamic HTTP 200/non-empty decoded MVT, matching static decoded features, and explicit real-R2 or local-fallback evidence.
- [ ] **Step 2: Run `cargo xtask verify foundation` in the pinned Docker environment.** Expected Foundation fmt, clippy, tests, tile contracts, and disposable DB checks pass.
- [ ] **Step 3: Run `cargo xtask verify gongzzang` and `pnpm -C products/gongzzang/apps/web test`.** Expected manifest and existing map/listing tests pass.
- [ ] **Step 4: Run `bash scripts/guard/monorepo-guard.sh`, `bash scripts/github/validate-public-repository-identity.sh`, and `bash scripts/github/check-publication-authority.sh`.** Expected no private identity, secret, production-data, or policy drift.

---

### Task 4: Publish through Public PR

**Files:**
- Modify only the allowlisted implementation files and verification-required generated artifacts.

**Interfaces:**
- Consumes: green evidence from Task 3.
- Produces: a Public-origin PR and a protected Public `main` result.

- [ ] **Step 1: Review and commit.** Run `git status --short`, `git diff --stat main...HEAD`, `git diff --check`, then commit with `feat: publish object-storage-first tile slice`; expected one focused commit and no private-history merge.
- [ ] **Step 2: Push only Public.** Run `git push -u origin feat/public-object-storage-tiles`; expected no push to `private-backup`.
- [ ] **Step 3: Merge only after required contexts succeed.** Required contexts are `required/docs`, `required/foundation`, `required/gongzzang-core`, `required/gongzzang-e2e`, `required/gongzzang-frontend`, `required/gongzzang-migrations`, `required/gongzzang-sqlx`, `required/identity`, `required/intelligence`, `required/repository`, and `required/secrets`.
- [ ] **Step 4: Verify canonical state.** Run `git fetch origin main`, `git status --short --branch`, and `gh api repos/perfectory-inc/perfectory-public/commits/main --jq '{sha,parents:(.parents|length),tree:.commit.tree.sha}'`; expected clean Public worktree, parentless Public `main`, and no development push path to private.
