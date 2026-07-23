---
name: verify-area
description: Use when you need to run the CI-equivalent Rust verification (fmt + clippy + tests) for one monorepo area — the scripts/verify/cargo-verify.sh docker harness, its area slugs, cache volumes, Git Bash path caveat, and when to use it instead of waiting for CI.
---

# Verify one area (docker harness)

## Command

From the repo root, in Git Bash:

```bash
bash scripts/verify/cargo-verify.sh <area-dir> [extra cargo test args]
```

`<area-dir>` is one of: `products/gongzzang`, `platforms/foundation-platform`,
`platforms/identity-platform`, `platforms/intelligence-platform`.
Called with no args it fails fast with `area dir required, e.g. products/gongzzang`.

## What it runs

Inside `rust:1.96.0-bookworm` (exactly the root `rust-toolchain.toml` pin),
with `SQLX_OFFLINE=true`:

1. `cargo fmt --all -- --check`
2. `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`
3. Tests:
   - gongzzang (two-stage, mirrors gongzzang-ci): DB-feature tests are excluded
     workspace-wide — `cargo test --locked --workspace --all-features
     --exclude gongzzang-persistence`, then the persistence crate's non-DB
     suite `cargo test --locked -p gongzzang-persistence`.
   - other areas: `cargo test --locked --workspace --all-features` (+ your
     extra args).

It also apt-installs `cmake` + `libsasl2-dev` (needed by intelligence's
rdkafka; harmless elsewhere).

## Caches (named docker volumes — do not delete casually)

- `perfectory-cargo-registry` — shared across areas
- `perfectory-rustup` — toolchain install
- `perfectory-target-<slug>` — per-area target dir; slug = area dir with `/`
  replaced by `-` (e.g., `perfectory-target-products-gongzzang`)

Windows bind mounts are too slow for `target/`, hence the volumes. First run
per area is slow (apt + component install + cold cache); later runs reuse the
volumes.

## Git Bash caveat

The script sets `MSYS_NO_PATHCONV=1` and uses `pwd -W` itself so Git Bash does
not rewrite `/work/...` container paths into `C:/Program Files/Git/...`. Run
the script as-is; if you hand-craft your own `docker run` from it, keep
`MSYS_NO_PATHCONV=1`.

## When to use vs CI

- Use BEFORE pushing Rust changes: pre-push lefthook runs guards, not builds —
  this harness is the only local run of the exact CI steps.
- Use when Windows-host `cargo` results are suspect (CI runs Linux; this
  reproduces it).
- CI still runs the same steps per area on PR; this harness does not replace
  CI, it front-runs it.
