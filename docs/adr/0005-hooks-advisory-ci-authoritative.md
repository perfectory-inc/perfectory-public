# ADR 0005: Git hooks are advisory; CI is authoritative

- Status: Accepted
- Date: 2026-07-20

## Context

A pre-push hook (`markdown-links`) called `cargo xtask docs`, but this repository
builds Rust only inside Docker — the dev host has no `cargo`. Result:
`cargo: command not found` → **every push blocked** on a `git push` that had nothing
to do with docs. The reflex fix ("add a guard forbidding host-only tools in hooks")
was researched against how serious orgs actually work, and rejected as non-standard.

Primary sources point one way — mature orgs do NOT make local hooks an authoritative
gate:

- **Pro Git** (git-scm.com/book, Customizing Git — Git Hooks): client-side hooks are
  *"not copied when you clone a repository"*; *"if your intent … is to enforce a
  policy, you'll probably want to do that on the server side"*; and any hook can be
  *"bypass[ed] … with `git commit --no-verify`."* A bypassable, non-propagating hook
  cannot be a source of truth.
- **Software Engineering at Google** (ch.23): presubmit should contain *"only fast,
  reliable ones"*; the real test execution happens on server infrastructure (Forge/TAP),
  not the developer's machine.
- Real repos: **facebook/react** ships no git-hook tooling at all; **rust-lang/rust**
  keeps `src/etc/pre-push.sh` as an *opt-in* sample ("Copy this script to .git/hooks to
  activate"); **oxidecomputer/omicron** has no git hooks in-tree (CI + an optional Nix
  devShell). None gate on heavy local hooks.

## Decision

**CI (`.github/workflows/*`) is the single authoritative verification gate. Git hooks
(lefthook) are a fast, local, advisory convenience.** Concretely:

1. A hook MUST skip — never fail — when a required host tool is absent. Host-tool hooks
   probe the concrete command they invoke in a Lefthook-native `skip.run`, from the
   command's configured `root:`. Checking only a launcher such as `pnpm` or `cargo` is
   insufficient: the requested package binary or optional Cargo component can still be
   unavailable. This repo runs Rust only in Docker, so a missing or unusable host
   subtool must not wedge a commit or push.
2. Anything heavy or toolchain-dependent (full `cargo xtask verify`, whole-repo lychee,
   DB-integration) is enforced in CI. Its hook copy, if any, is Docker-wrapped and
   skip-guarded (e.g. `scripts/ci/lychee-docs.sh` skips when Docker is down).
3. Hooks are never a place to *enforce policy that CI doesn't also enforce*. If a check
   matters, it lives in CI; the hook only front-runs it for fast feedback.

Explicitly NOT adopted: a blanket ban on host-only tools in hook configs. Fast local
tools remain useful. The narrow `lefthook-advisory-policy` guard instead derives the
required availability probe from each direct package-tool command and its `root:`; its
mutation tests reject launcher-only, wrong-subtool, wrong-root, missing, and indirect
or ambiguous probes, compound commands, unsupported YAML shapes, and duplicate keys.
Heavy host Cargo commands are rejected because their authoritative Docker/CI gate is
already the SSOT. This enforces the advisory contract without duplicating hook tools.

## Consequences

- `LEFTHOOK_EXCLUDE=…` manual workarounds for the cargo-less host disappear: cargo hooks
  self-skip.
- On a fully-provisioned machine, where each concrete invoked subtool runs from its
  configured root, all hooks run as before. A launcher on PATH alone is not sufficient.
- The authoritative guarantee lives in CI, which the verification SSOT (ADR-0004) already
  makes reproducible.
