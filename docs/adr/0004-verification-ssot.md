# ADR 0004: Verification SSOT (`cargo xtask verify`)

- Status: Accepted
- Date: 2026-07-20

## Context

The monorepo's first real CI run surfaced a recurring class of failure: work
verified green locally still failed in CI, and fixing one CI job only revealed the
next. Root cause — **there was no single definition of "how an area is verified".**
The same fmt/clippy/test logic was hand-rolled, and had drifted, across five places:

| verifier | `--all-features` | `--locked` | native deps (cmake) |
|---|---|---|---|
| `scripts/verify/cargo-verify.sh` (local) | yes | yes | installs cmake+sasl |
| `foundation-ci.yml` | yes | yes | (none) |
| `gongzzang-ci.yml` | yes | no | (none) |
| `identity-ci.yml` | varies | no | (none) |
| `intelligence-ci.yml` | **no** | no | **none** (rdkafka needs cmake) |

Because "green" meant something different in each place, local green could not
imply CI green. Every surprise came from this gap (clippy flag drift; missing cmake
for rdkafka; different test scopes). This violates the project's top principle
(root `AGENTS.md`): fix root causes, leave excellent structure (SSOT), make the
same problem impossible to recur.

## Decision

**There is exactly one definition of area verification: `cargo xtask verify <area>`.**

- `tools/xtask` (a standalone Rust crate, invoked via the root `.cargo/config.toml`
  alias) owns the canonical, per-area verification: required native deps, and the
  ONE fmt/clippy/test command policy.
- **Both** local (`scripts/verify/cargo-verify.sh`, inside Docker) **and** CI
  (`.github/workflows/*-ci.yml` rust jobs) call `cargo xtask verify <area>`. Neither
  hand-rolls cargo commands.
- Canonical policy (one variant, no drift):
  - `cargo fmt --all -- --check`
  - `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`
  - `cargo test --locked --workspace --all-features`
    (gongzzang: DB-feature tests excluded workspace-wide, then the persistence crate's
    non-DB suite runs separately — this two-stage contract lives in xtask, not in YAML.)
  - Native deps declared per area in xtask (only `intelligence` needs cmake+libsasl2
    for rdkafka); xtask installs them (apt, Debian-family; sudo when not root).
- **Guard:** `scripts/guard/no-adhoc-cargo-lint.sh` fails if any workflow contains a
  raw `cargo clippy` or `cargo fmt` outside `cargo xtask`. Drift cannot re-enter.

Rust (not bash) per the language policy and the enterprise-repo research
(oxidecomputer/omicron's `cargo xtask`). `xtask` is dependency-free (std only).

## Scope (phased)

- **Phase 1 (this ADR):** the fmt/clippy/test trio — the drift that caused the
  incident — is unified. This is the rust-quality job of every area.
- **Phase 2 (follow-up):** extend `xtask verify --full` to also orchestrate ephemeral
  Postgres for the `--ignored` DB-integration tests and compose-smoke, so the LOCAL
  harness runs the *entire* CI surface (closing the remaining coverage gap: local
  currently skips DB-integration because it runs `SQLX_OFFLINE` without a database).
  Until then, DB-integration/compose/frontend jobs keep their commands and are exempt
  from the guard.

## Consequences

- Adding a new area or changing the verification policy is a one-line data/edit in
  `tools/xtask` — every consumer updates at once. No YAML edits, no drift.
- `cargo xtask verify all` reproduces the rust-quality of the whole monorepo locally.
- New standalone crate `xtask` (globally-unique name; not a member of any area
  workspace). Invoke from the repo root.
