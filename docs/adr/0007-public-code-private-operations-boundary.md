# ADR 0007: Public code SSOT and private operations boundary

- Status: Accepted
- Date: 2026-07-23
- Amends: ADR-0002, ADR-0003

## Context

The existing private monorepo history contains dated plans, reviews, handoffs, live operational
evidence, provider/account bindings, and historical build output. Making that repository public would
publish every reachable commit, including material deleted from the current tree. Private GitHub
Actions were not inherently unavailable, but the account's billing and quota policy made private
Actions unsuitable as the authoritative CI gate for this repository.

A recurring mirror with an exclusion list would create two writable code sources and a second taxonomy
that could drift from the repository tree. Rewriting the existing history would be destructive and
would still require every historical binary and workflow artifact to be certified.

## Decision

`perfectory-inc/perfectory-public` is the canonical repository for source code, pull requests, and CI
while public. It starts with one audited root commit made from a publication-safe tracked tree. No
private commit, tag, pull request, issue, workflow run, artifact, or Git metadata is migrated.

The public code tree excludes `**/docs/archive/**`, `**/docs/review/**`, agent-memory snapshots, dated
handoffs, current resource inventory, host/deployment evidence, provider usernames, account
identifiers, account-specific endpoints, public-storage host bindings, and credentials. Current
contracts belong in maintained ADRs, specifications, runbooks, or code.

Stable logical resource namespace labels may remain public when they are deliberate fail-closed
application or schema contracts. Their account, endpoint, credential, host, and current-state bindings
remain private.

The immutable numeric and node IDs of the canonical public repository and its owning organization are
a narrow exception. They are public control-plane invariants used to reject rename/transfer,
deletion-and-name-reuse, wrong-owner, and wrong-host publication targets; they are not runtime account
bindings. Personal GitHub logins, numeric IDs, node IDs, and numeric `noreply` addresses remain private.
Canonical public CI requires the strict checked-in positive identity and matches its numeric repository
and owner IDs to `GITHUB_REPOSITORY_ID` and `GITHUB_REPOSITORY_OWNER_ID`. Private, fork, and local
structural checks permit the deliberate unset repository pair only as a non-positive exception; it
does not authorize publication. Node IDs are format-checked, and the configurator performs a full live
GitHub API identity readback before any mode proceeds.
The parentless public root commit uses the neutral deterministic identity `Perfectory
<public-root@perfectory.invalid>` and is not attributed to a maintainer's GitHub account.

The existing private repository is a transition archive, not a second code source. After migration it
is archived read-only. New operational evidence belongs in a separate private operations repository or
external evidence store. Secret-bearing live workflows and self-hosted runners stay on that private
side; the public repository contains reproducible scripts but only secretless GitHub-hosted CI.

First-party source code is public for inspection but remains proprietary under the repository's
canonical All Rights Reserved license file. Public visibility does not create an open-source grant.
GitHub does not provide a switch that prevents the public from creating pull requests. External code
contributions are therefore not accepted until a written contribution/assignment process exists, as
stated in `CONTRIBUTING.md`; an opened pull request is not acceptance or a license grant. Third-party
assets retain their own notices and REUSE annotations.

That source-code statement does not classify captured public data, sample records, fixtures, fonts, or
other assets as first-party. Publication is blocked for any real-looking data fixture until it is either
replaced with unmistakably synthetic data or backed by recorded provenance and redistribution terms.
Unknown provenance is a failure, not an implied permission.

`tools/github/legal-identity.json` is the public legal-identity and human-attestation SSOT. The wider
legal/licensing contract also exact-pins the canonical root and proprietary license bodies, the full
REUSE annotation allowlist, and `tools/github/third-party-artifact-policy.json`. That guarded registry
fixes the SHA-256 digests of `.gitattributes`, `THIRD_PARTY_NOTICES.md`, both OFL copies, the Pretendard
CSS, and its hash manifest; public-tree safety uses that manifest to enforce the exact tracked WOFF2
set and hashes. The root `.gitattributes` is the sole attributes SSOT, and the deletion of the former
`products/gongzzang/.gitattributes` duplicate travels with this registry. Before any public repository
is created, configured, or published, every first-party/proprietary file must have a private
provenance, ownership, and assignment review. The recorded `copyright_holder` must be the actual
legally supportable rights holder and must match the
canonical proprietary license and root REUSE annotation. Supporting evidence and signoff remain
private. The `first_party_ownership_or_assignment_confirmed` boolean may become `true` only after that
review; it is a fail-closed human self-attestation, not legal proof. Strict validation gates `bootstrap`,
`prepublish`, publication preparation and therefore the publisher, and canonical public CI, so changing
a confirmed `true` value back to `false` is rejected.

Publication and continued operation fail closed through:

- tracked-tree guards for forbidden evidence/artifact paths and unsafe file types;
- one proprietary package-license and non-publishability contract;
- full-SHA Action and container-digest enforcement;
- no variable/self-hosted runner or repository secret in public workflows;
- narrow gitleaks exceptions plus worktree and full-history scans;
- GitHub secret scanning, push protection, read-only workflow tokens, Action allowlists, and protected
  `main`;
- private vulnerability reporting as the security-disclosure channel while public issues are disabled.

The GitHub-side desired state is versioned under `tools/github/` and is applied and read back by
`scripts/github/configure-public-repository.sh`. `main` has no bypass actor, permits squash merge only,
requires pull requests with resolved conversations, rejects deletion and non-fast-forward updates,
and accepts only the stable `required/*` checks produced by GitHub Actions. While there is only one
maintainer, the approval count is zero; requiring an approval from the pull-request author would make
the repository impossible to merge. The count must be raised when a second maintainer joins.

Required workflows run for every pull request without top-level path filters. This is deliberate:
GitHub does not create a successful required check when a whole workflow is skipped by a path filter,
which can leave a pull request permanently pending. Pushes may retain path filters. Each multi-job
workflow collapses its internal results into one stable `required/<area>` terminal check, so internal
job names can evolve without changing branch protection.

Repository-owned scripts run the Lychee and REUSE tools from digest-pinned container images. A wrapper
Action pinned by commit is not sufficient when that Action downloads an unchecked executable or builds
from a mutable base image. The public guard therefore reconciles direct third-party Action references,
their exact-SHA GitHub allowlist, pinned verification images, required-check names, and the ruleset
payload as one contract.

## GitHub Actions cost boundary

Public visibility does not make every Actions resource unlimited. GitHub documents standard
GitHub-hosted runner execution for public repositories as free, while larger runners are always billed.
The workflow policy therefore requires the literal standard runner label `ubuntu-24.04`; variables,
self-hosted labels, runner groups, and larger-runner labels fail verification.

Artifact storage is a separate pooled allowance shared with GitHub Packages. GitHub Free for
organizations currently includes 500 MB of artifact storage, so a public repository can still incur
storage charges after the allowance is exhausted when billing is enabled. The repository sets artifact
and log retention to seven days through `tools/github/artifact-retention.json`, and the GitHub policy
guard plus configurator apply and read back that value. Artifact-producing jobs must keep individual
retention at seven days or inherit the repository value; CI output is diagnostic evidence, not durable
release storage.

The spending ceiling is verified from GitHub's read-only organization Budgets API, not from a human
environment-variable attestation or a point-in-time usage total. Publication requires exactly one
organization-scoped budget with `budget_amount: 0` and `prevent_further_usage: true` for each of
`ProductPricing/actions` and `SkuPricing/actions_cache_storage`. The verifier requires a complete
single-page inventory and fails closed on missing or duplicate targets, a wrong scope/type/SKU, nonzero
amount, disabled hard stop, malformed data, or `has_next_page: true`. Every configurator mode performs
this read before its mode-specific mutation. Bootstrap and publication never create, update, or delete
budgets; an organization owner must establish a missing budget separately and then rerun verification.

Actions dependency-cache storage is a different SKU and is not part of the artifact/GitHub Packages
pool. The configurator therefore performs a separate, read-only repository cache-capability check with
GitHub REST API version `2026-03-10`; it never raises a cache limit with `PUT`. When the storage and
retention endpoints return `200`, publication requires `max_cache_size_gb <= 10` and
`max_cache_retention_days <= 7`. The only admitted alternative is the current fail-closed state where
the organization plan is exactly `free` and GitHub returns HTTP `402` with the exact message `Please
ensure your account has a valid payment method on file to access this service.`. This proves that the
account cannot opt in to paid cache capacity without first changing its billing state. Any other
response, message, plan, or larger configured limit blocks publication.

Cache usage is only a point-in-time measurement and cannot prove a future spending ceiling. It must
not replace either the verified budgets or the configured-limit/capability check. A Free-plan exact
`402` cache-limit response proves that paid cache-limit opt-in is currently unavailable, but it does not
substitute for the two required zero-dollar budget records.

The organization owner must enable included-usage alerts and monitor Actions artifact/Packages and
cache storage separately. If either exact zero-dollar hard-stop budget cannot be created or verified,
publication is **NO-GO**; a positive or merely low budget is not an accepted fallback. The configurator
reads these organization-owned live settings during both bootstrap and prepublication so billing-plan
or budget drift fails closed.

Sources: [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions),
[larger-runner billing](https://docs.github.com/en/actions/concepts/runners/larger-runners),
[artifact retention](https://docs.github.com/en/organizations/managing-organization-settings/configuring-the-retention-period-for-github-actions-artifacts-and-logs-in-your-organization),
[budgets and hard stops](https://docs.github.com/en/billing/how-tos/set-up-budgets),
[Budgets REST API](https://docs.github.com/en/rest/billing/budgets?apiVersion=2026-03-10),
[Actions cache REST API](https://docs.github.com/en/rest/actions/cache?apiVersion=2026-03-10), and
[cache usage limits and eviction policy](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#usage-limits-and-eviction-policy).

The public repository may be changed to private before launch. That change stops new anonymous access
but cannot retract clones or public forks created while it was public.

## Publication and feature-transfer procedure

Publication is a one-way, exact-tree operation. The required order is:

1. Before creating or configuring any public repository or running the publisher, privately review
   provenance, ownership, and assignment for every first-party/proprietary file. Keep the evidence and
   review signoff private. Only after that review, set `tools/github/legal-identity.json`'s
   `copyright_holder` to the actual legally supportable rights holder and
   `first_party_ownership_or_assignment_confirmed` to `true`; do not infer or invent either value. The
   boolean is a fail-closed human self-attestation, not legal proof. The strict validator exact-pins
   that JSON file, the canonical root `LICENSE` and `LICENSES/LicenseRef-Proprietary.txt` bodies, the
   complete REUSE annotation allowlist, and the guarded third-party digest registry. The registry
   covers `.gitattributes`, `THIRD_PARTY_NOTICES.md`, both OFL copies, the Pretendard CSS, and its hash
   manifest; public-tree safety uses the manifest to enforce the exact WOFF2 set and hashes. The
   proprietary license body cannot carry extra grants, and its holder/year must match the root REUSE
   annotation. The strict validator must pass without `--allow-unconfirmed`:

   ```bash
   bash scripts/github/validate-legal-publication.sh
   ```

   Publication is NO-GO until it passes. Review and atomically stage the legal identity, both license
   files, `REUSE.toml`, the digest registry, all six directly hashed registry artifacts, and the
   manifest-listed WOFF2 files with `git diff`, `git add`, and `git diff --cached` over the same path
   set. Include the deletion of `products/gongzzang/.gitattributes` in that path set because the root
   `.gitattributes` is the sole SSOT. Commit the public registry on the private readiness branch and
   require a clean worktree before step 2 or `bootstrap`; private evidence and signoff are never staged
   or committed. Canonical public CI uses the same strict mode and therefore rejects a later `true` to
   `false` change.
2. Create `perfectory-inc/perfectory-public` as an empty public repository with `main` as its default;
   do not initialize a README, license, `.gitignore`, branch, or tag.
3. Run the read-only `show-public-repository-identity.sh`, review the canonical GitHub.com repository
   and owner IDs, apply its canonical output to `tools/github/repository-identity.json`, and commit it.
   The placeholder ID/node ID is an intentional NO-GO state. Immutable IDs pin the repository across
   rename and reject transfer, deletion/name reuse, wrong-owner, and wrong-host targets. Before the
   commit, run strict checked-in validation:

   ```bash
   bash scripts/github/validate-public-repository-identity.sh
   ```

   Canonical public CI additionally matches the checked-in numeric repository and owner IDs to
   `GITHUB_REPOSITORY_ID` and `GITHUB_REPOSITORY_OWNER_ID`. Private, fork, and local structural checks
   permit the deliberate placeholder only as the non-positive exception; that does not authorize
   publication. Node IDs are format-checked, and the configurator performs the full live API identity
   readback.
4. Apply `configure-public-repository.sh bootstrap`. It first reads and verifies the two exact
   organization zero-dollar hard-stop budgets without mutating billing state. It then installs a
   zero-bypass `main` policy that permits its one creation but denies deletion and subsequent updates,
   blocks every non-`main` branch and tag, leaves automated security fixes disabled, and verifies the
   empty state.
5. Use the clean identity-pinned source commit as an input tree; none of its private ancestry is
   publishable. Invoke the single trusted publisher entry point:

   ```bash
   source_commit="$(git rev-parse HEAD)"
   bash scripts/github/publish-public-root.sh "$(git rev-parse --show-toplevel)" "$source_commit"
   ```

   The publisher requires the source and control worktree to be the publisher's own repository root.
   It creates a private temporary bare snapshot, calls `prepare-public-root.sh` internally to run every
   full monorepo/publication gate, and deterministically computes and rechecks the one-commit parentless
   `main` root from the exact source tree and checked-in neutral identity. It targets only the literal
   canonical URL and never adds a public remote to the private repository. It fresh-publishes only an
   exactly empty canonical remote, or resumes only when the complete remote state is exactly `HEAD` as
   a symref to `main` plus `HEAD` and `main` at the expected root SHA with no other ref. Every mismatch
   fails closed. On the fresh path, `prepublish` is followed by another sole-writer authority check
   immediately before the first explicit-URL push and `lock`. Both fresh and exact-resume paths verify
   an independent clone before `activate`. Activation installs the final Dependabot-only non-`main`
   firewall and enables automated security fixes while leaving the `main` update deny intact. The
   publisher removes the private temporary snapshot on exit and reports the root in its final
   `OK public-root-publisher mode=<fresh|resume> commit=<root-sha> ...` line; the canonical `main` ref
   may also be read after successful publication.
6. Wait for every required check on the reported root commit to succeed. Then apply `protect` and finally
   `verify`. `protect` confirms the expected parentless root and pinned-App green checks, atomically
   replaces the bootstrap `main` rule with the full pull-request policy, and rechecks the root SHA.

The canonical repository rejects user-created non-`main` branches and every tag. Bootstrap has no
bypass at all. After the parentless root is independently verified, `activate` grants the sole non-`main` branch
firewall bypass to the Dependabot integration; normal feature work—including maintainer work—uses a
fork and opens a pull request back to canonical `main`. The tag firewall has no bypass. The bootstrap
`main` update deny remains active from the first push through root CI. Only final `protect` replaces it
after the required checks have reported successfully, with expected-SHA checks on both sides.

`scripts/github/import-private-feature-diff.sh` is the only supported bridge for work already present
on a private branch. It applies the private base-to-feature tree diff to a clean named branch made from
public `origin/main`, runs public-tree guards, and leaves changes unstaged for review. It does not copy
commit objects. Push that reviewed branch to a fork remote, not to the canonical repository. Never add
the public repository as a remote of the private worktree, never add or fetch a private remote inside a
public clone, and never use alternates, bundles, grafts, replacement refs, or cherry-picks to move
private history across the boundary.

## Consequences

- Code and PRs have one SSOT and can use standard public GitHub-hosted Actions without minute charges;
  larger runners and storage beyond the pooled allowance are not included in that statement.
- Private operational evidence remains available without making code history public.
- Historical plans are intentionally not linkable from the public tree; maintained ADRs and code must
  carry every current contract.
- Existing private feature work is transferred only as a reviewed tree diff onto a fork branch based
  on public `main`, never by pushing or fetching private ancestors.
- Before launch, billing and feature differences must be reassessed when repository visibility changes
  to private.
