# Public GitHub repository policy

This directory is the desired-state SSOT for `perfectory-inc/perfectory-public`.
The payloads map directly to GitHub's repository, Actions-permission, retention,
fork-approval, and repository-ruleset APIs. Do not edit live settings without
changing this directory and running the verifier.

## First publication

Use this order exactly. The configurator never creates a repository or changes
visibility. Do not create or configure the public repository, or run the
publisher, until the legal precondition in step 1 passes.

Making the repository public is irreversible disclosure: changing it back to
private stops new anonymous access but cannot retract existing clones or public
forks. Treat the warning in [ADR 0007](../../docs/adr/0007-public-code-private-operations-boundary.md)
as a publication precondition, not a later rollback plan.

1. Complete the first-party legal review privately. Review provenance,
   ownership, and assignment for every file that will be published as
   first-party/proprietary. Keep the supporting evidence and review signoff in
   the private operations/evidence system; do not add them to the public tree.

   Only after that review, set `copyright_holder` in
   `tools/github/legal-identity.json` to the actual legally supportable rights
   holder and set `first_party_ownership_or_assignment_confirmed` to `true`.
   Do not infer or invent either conclusion. The boolean is a fail-closed human
   self-attestation that the review occurred, not legal proof by itself.

   Strict validation treats the complete public legal/licensing registry as
   one contract. It checks the legal identity, the exact canonical bodies of
   the root `LICENSE` and `LICENSES/LicenseRef-Proprietary.txt`, and the full
   `REUSE.toml` annotation allowlist. `REUSE.toml` may contain only its canonical
   version plus the ordered root-proprietary and Pretendard-OFL annotations.
   The proprietary license body cannot be modified to add extra grants, and
   its holder/year must match the root REUSE annotation.

   The guarded `tools/github/third-party-artifact-policy.json` registry fixes
   the exact path allowlist and SHA-256 digest of `.gitattributes`,
   `THIRD_PARTY_NOTICES.md`, both `LICENSES/OFL-1.1.txt` and
   `products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt`, the
   Pretendard CSS, and `pretendard-v1.3.9.sha256`. The two OFL copies must also
   be byte-identical. The hash manifest transitively fixes the exact tracked
   WOFF2 set and hashes through `public-repository-safety.sh`; a manifest-only
   or font-only change fails the publication gates. The root `.gitattributes`
   is the sole attributes SSOT; the removed `products/gongzzang/.gitattributes`
   duplicate is part of the reviewed path set so its deletion is staged with
   the registry rather than discovered by the final clean-worktree check.
   Validation without `--allow-unconfirmed` must pass:

   ```bash
   bash scripts/github/validate-legal-publication.sh
   ```

   Review and commit the public legal SSOT on the private readiness branch
   before creating the repository or running `bootstrap`:

   ```bash
   legal_registry_paths=(
     tools/github/legal-identity.json
     LICENSE
     LICENSES/LicenseRef-Proprietary.txt
     REUSE.toml
     tools/github/third-party-artifact-policy.json
     .gitattributes
     products/gongzzang/.gitattributes
     THIRD_PARTY_NOTICES.md
     LICENSES/OFL-1.1.txt
     products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt
     products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css
     products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256
     'products/gongzzang/apps/web/public/fonts/**/*.woff2'
   )
   git status --short -- "${legal_registry_paths[@]}"
   git diff -- "${legal_registry_paths[@]}"
   git add -- "${legal_registry_paths[@]}"
   git diff --cached -- "${legal_registry_paths[@]}"
   git commit -m "chore: confirm legal publication identity"
   test -z "$(git status --porcelain=v1 --untracked-files=all)"
   ```

   Review and stage that complete public registry atomically. Never include
   private evidence, including provenance records or review signoff. A dirty
   readiness worktree remains **NO-GO** even when the strict validator passed
   against its uncommitted contents.

   Publication is **NO-GO** while the attestation is `false`, the holder
   drifts, or strict validation fails. Canonical public CI runs this same strict
   validation, so a later `true` to `false` change is rejected. The
   `--allow-unconfirmed` mode used for structural checks outside the canonical
   public repository does not authorize publication.

2. Create `perfectory-inc/perfectory-public` as an empty **public** repository.
   Its default branch must be `main`; do not initialize a README, license,
   `.gitignore`, branch, or tag. Confirm the organization billing gate below.

3. Pin the repository's immutable GitHub identity before any configurator or
   publication command. The checked-in `repository_id: 0` and
   `repository_node_id: UNSET_AFTER_REPOSITORY_CREATION` are deliberate
   fail-closed placeholders; both `bootstrap` and the publisher's internal
   preparation reject them.

   ```bash
   identity_candidate="$(mktemp)"
   bash scripts/github/show-public-repository-identity.sh >"$identity_candidate"
   cat "$identity_candidate"
   ```

   Review `hostname`, `full_name`, `repository_id`, `repository_node_id`, and
   the owner's `login`, `id`, and `node_id` against the newly created repository
   and organization. Only after that review, apply the canonical output and
   commit it as part of the publication-safe tree:

   ```bash
   cp -- "$identity_candidate" tools/github/repository-identity.json
   bash scripts/github/validate-public-repository-identity.sh
   git diff -- tools/github/repository-identity.json
   git add tools/github/repository-identity.json
   git commit -m "chore: pin public repository identity"
   rm -f -- "$identity_candidate"
   test -z "$(git status --porcelain=v1 --untracked-files=all)"
   ```

   The read-only helper forces `github.com`, queries only the canonical target,
   and validates the immutable owner. The strict wrapper requires a checked-in
   positive repository ID and valid repository node ID; the owner ID/node ID
   remain exact canonical invariants. Canonical public CI requires that strict
   positive identity and also matches the numeric `GITHUB_REPOSITORY_ID` and
   `GITHUB_REPOSITORY_OWNER_ID` values to the checked-in repository and owner
   IDs. Repository and owner node IDs are format-checked, while the configurator
   performs a full live GitHub API readback of the repository and owner identity.

   Private repositories, forks, and local structural checks may use
   `validate-public-repository-identity.sh --allow-unset`. It accepts a valid
   positive identity or, as its only non-positive exception, the deliberate
   `0`/`UNSET_AFTER_REPOSITORY_CREATION` placeholder pair; that exception never
   authorizes configuration or publication. The checked-in IDs prevent a later
   repository rename, transfer, deletion/name reuse, or wrong-host login from
   silently becoming the publication target. Publication is **NO-GO** until
   the real identity is reviewed, strictly validated, committed, and the
   worktree is clean.

4. Bootstrap the identity-pinned empty repository:

   ```bash
   bash scripts/github/configure-public-repository.sh bootstrap
   ```

   `bootstrap` reads the organization's GitHub budget inventory and requires
   exactly one organization-scoped USD 0 hard-stop for each of
   `ProductPricing/actions` and `SkuPricing/actions_cache_storage`. Missing,
   duplicate, malformed, or paginated budget data is **NO-GO**. The
   configurator never creates, updates, or deletes a budget. If either budget
   is absent, an organization owner must create it in GitHub Billing (or with a
   separately reviewed administrative API call) and rerun `bootstrap`.

   Bootstrap installs a zero-bypass `main` policy that permits its first
   creation but denies deletion and every later update. It also blocks every
   non-`main` branch and tag with zero bypass. Automated security fixes remain
   disabled, so no integration can create a branch before the audited root is
   published and independently verified.

5. Publish a history-free audited root from the exact clean identity-pinned
   commit through the single trusted entry point:

   ```bash
   test -z "$(git status --porcelain=v1 --untracked-files=all)"
   source_commit="$(git rev-parse HEAD)"
   bash scripts/github/publish-public-root.sh "$(git rev-parse --show-toplevel)" "$source_commit"
   ```

   The publisher accepts only the control worktree itself and its exact clean
   `HEAD`. It creates a private temporary bare snapshot, calls
   `prepare-public-root.sh` within the same publisher invocation, and runs the full
   monorepo/publication gates. It deterministically computes the one-commit,
   parentless `main` root from the source tree and checked-in neutral identity,
   then independently rechecks the tree, metadata, refs, object closure, and
   absence of remotes. A dirty worktree, a different `HEAD`, symlink, gitlink,
   extra object, or failed gate aborts publication.

   The destination is the literal canonical URL
   `https://github.com/perfectory-inc/perfectory-public.git`; it is not a caller
   argument. After preparation, the publisher fresh-publishes only when that
   remote is exactly empty. It resumes only when `HEAD` is a symref to `main`,
   both `HEAD` and `refs/heads/main` equal the deterministically expected root,
   and no other ref exists. Every other remote state fails closed. On the fresh
   path it runs `prepublish`, reruns the sole-writer publication-authority check
   immediately before the first push, pushes through the explicit URL, and
   runs `lock`. Both the fresh and exact-resume paths verify a new independent
   clone before `activate`. The publisher never adds a public remote to the
   private repository.

   A successful invocation ends with
   `OK public-root-publisher mode=<fresh|resume> commit=<root-sha> ...`. The
   internal snapshot is private temporary state and is removed on exit, so do
   not try to read `root_sha` from it. After success, obtain the same SHA from
   the locked canonical `main` ref and require exactly one 40-hex result:

   ```bash
   root_sha="$(
     bash scripts/github/safe-git-transport.sh --no-repository \
       ls-remote --exit-code \
       https://github.com/perfectory-inc/perfectory-public.git \
       refs/heads/main \
       | awk 'NR == 1 && $2 == "refs/heads/main" && length($1) == 40 && $1 !~ /[^0-9a-f]/ { sha = $1 }
              END { if (NR != 1 || sha == "") exit 1; print sha }'
   )"
   test "${#root_sha}" -eq 40
   ```

   `lock` checks that remote `HEAD`/`main` equal the expected parentless root,
   that no other ref exists, and that the bootstrap update-deny policy is still
   active. Activation replaces only the zero-bypass non-`main` firewall with
   the final Dependabot-only firewall and enables automated security fixes; the
   bootstrap `main` update deny remains unchanged through root CI.

6. Wait for all workflows on the root commit, then enable final protection and
   read the whole policy back:

   ```bash
   gh run list --repo perfectory-inc/perfectory-public --commit "$root_sha"
   gh run list --repo perfectory-inc/perfectory-public --commit "$root_sha" \
     --json databaseId --jq '.[].databaseId' \
     | while IFS= read -r run_id; do
         gh run watch --repo perfectory-inc/perfectory-public \
           "$run_id" --exit-status
       done
   PERFECTORY_EXPECTED_PUBLIC_ROOT="$root_sha" \
     bash scripts/github/configure-public-repository.sh protect
   bash scripts/github/configure-public-repository.sh verify
   ```

   `protect` verifies the expected parentless root and every configured
   `required/*` result from the pinned GitHub Actions App, atomically replaces
   the bootstrap update-deny policy with the full pull-request policy, and then
   verifies the root SHA again. An absent workflow, a stale success, a moved
   `main`, or an early invocation fails closed.

`integration_id: 15368` is the GitHub Actions App and can be independently read
from `gh api /apps/github-actions`. Bootstrap has no bypass actor. After
`activate`, the branch-firewall bypass actor is only the Dependabot integration
(`integration_id: 29110`). The tag firewall and final `main` ruleset have no
bypass actor.

Bootstrap enables dependency alerts, secret scanning with push protection, and
private vulnerability reporting. `activate` enables automated security fixes
only after the root is locked and independently cloned. Reports use the private
advisory channel in the root `SECURITY.md`, never a public issue.

## Feature work after publication

The canonical non-`main` branch firewall means ordinary feature branches live
in forks. Clone the canonical public repository so `origin/main` is authoritative,
create a local branch at that exact commit, add a separate fork remote for the
eventual push, and open a pull request from the fork. Do not weaken the firewall
for normal development.

For a feature that currently exists only in private history, use the existing
tree-only bridge:

```bash
git clone https://github.com/perfectory-inc/perfectory-public.git public-clone
git -C public-clone switch -c feature/example origin/main
git -C public-clone remote add fork git@github.com:YOUR-FORK/perfectory-public.git
bash scripts/github/import-private-feature-diff.sh \
  /path/to/private-perfectory PRIVATE_BASE PRIVATE_FEATURE public-clone
git -C public-clone diff --check
git -C public-clone status --short
```

The importer computes a binary tree diff in the private repository, applies it
through a temporary index, runs public guards and a tree secret scan, and leaves
the result unstaged. Review it, commit in the public clone, and push only to the
`fork` remote. It never imports private Git objects.

Never add the public repository as a remote of a private worktree. Never add or
fetch a private remote in a public clone. Never use alternates, bundles, grafts,
replacement refs, cherry-picks, or a shared object store across the boundary.

GitHub cannot disable pull-request creation on a public repository. Under
`CONTRIBUTING.md`, external code contributions are not accepted until a written
contribution or assignment agreement exists; an unsolicited pull request does
not change that policy.

## Organization billing gate

The repository configurator cannot own organization billing. Before enabling
CI, an organization owner must:

1. enable the 90% and 100% included-usage alerts for Actions artifact storage;
2. create exactly one organization-scoped `ProductPricing/actions` budget with
   `budget_amount: 0` and `prevent_further_usage: true`;
3. create exactly one organization-scoped
   `SkuPricing/actions_cache_storage` budget with the same zero-dollar hard
   stop; the Actions product budget is not a substitute for this SKU budget;
4. confirm artifact/log retention is seven days after bootstrap; and
5. monitor artifact/Packages storage and dependency-cache storage separately.

Standard `ubuntu-24.04` runner execution is free for a public repository, but
larger runners and storage beyond the plan allowance are not. The workflow
policy mechanically rejects every runner label except literal `ubuntu-24.04`;
the billing budget is the hard stop for metered overage.

Every configurator mode, including `bootstrap` and `prepublish`, verifies both
budgets through the read-only organization Budgets API before any mode-specific
mutation. It requires one complete, non-paginated response and rejects a missing
or duplicate target, the wrong scope/type/SKU, a nonzero amount,
`prevent_further_usage` other than `true`, malformed data, or
`has_next_page: true`. It never calls a budget mutation endpoint. A small current
billing-usage result is not an alternative proof of a future ceiling.

Dependency-cache storage is not the artifact/GitHub Packages storage pool. The
configurator only reads the repository cache storage and retention limits with
GitHub REST API version `2026-03-10`; it never sends a cache-limit `PUT`. A
`200` response is accepted only when `max_cache_size_gb <= 10` and
`max_cache_retention_days <= 7`. The sole fail-closed no-payment exception is
an organization whose plan is exactly `free` and whose limit request returns
HTTP `402` with the exact message `Please ensure your account has a valid
payment method on file to access this service.`. That state prevents paid
cache-limit opt-in. Every other status, body, plan, or larger limit is a
publication failure.

Current cache usage is an observation, not a configured upper bound: a small
usage result cannot prove that future writes are capped. Both verified budget
records and the limit/capability check are required. See the
[Budgets REST API](https://docs.github.com/en/rest/billing/budgets?apiVersion=2026-03-10),
[Actions cache REST API](https://docs.github.com/en/rest/actions/cache?apiVersion=2026-03-10)
and [cache usage limits and eviction policy](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#usage-limits-and-eviction-policy).

Top-level pull-request path filters are forbidden because GitHub can leave a
required check pending when it skips the entire workflow. Push filters remain
allowed. `scripts/guard/public-github-policy.sh` reconciles this directory with
workflow job names and exact third-party Action references.
