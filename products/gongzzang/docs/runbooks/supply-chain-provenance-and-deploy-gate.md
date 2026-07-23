# Supply-Chain Source Gates

## Current Contract

The public repository verifies source and dependency integrity; it does not
contain a production deployment admission path.

- `.github/workflows/gongzzang-ci.yml` runs the `cargo-deny` dependency-policy
  gate and the Gongzzang verification/guardrail jobs.
- `.github/workflows/secret-scan.yml` runs gitleaks against the worktree and
  Git history with the root `.gitleaks.toml` configuration.
- Third-party Actions are pinned to immutable commit SHAs and reviewed through
  dependency update pull requests.
- `cargo xtask verify gongzzang` is the local and CI verification entry point.

The machine-readable policy is
[`supply-chain-policy.v1.json`](../architecture/platform-integration/supply-chain-policy.v1.json).

## Production Promotion

Release provenance, SBOM attestation, signing, and production deployment
admission were intentionally removed before launch by
[ADR 0044](../adr/0044-bazel-transition-reconciliation.md). A future production
promotion gate must be designed from the actual deployment target and threat
model. It requires a new ADR, protected environment, least-privilege identity,
artifact identity contract, rollback procedure, and verification evidence.

Historical workflow or script names are not an executable runbook and must not
be copied back into the repository.
