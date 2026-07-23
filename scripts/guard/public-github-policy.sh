#!/usr/bin/env bash
# Reconciles the checked-in GitHub desired state, workflow trust boundary, and
# history-free publication machinery as one fail-closed contract.
set -euo pipefail
control_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$control_root"

required_files=(
  .gitattributes
  THIRD_PARTY_NOTICES.md
  tools/github/main-ruleset.json
  tools/github/bootstrap-main-ruleset.json
  tools/github/bootstrap-non-main-branch-firewall.json
  tools/github/non-main-branch-firewall.json
  tools/github/tag-firewall.json
  tools/github/actions-policy.json
  tools/github/selected-actions.json
  tools/github/repository-settings.json
  tools/github/workflow-permissions.json
  tools/github/artifact-retention.json
  tools/github/actions-cache-policy.json
  tools/github/billing-budget-policy.json
  tools/github/fork-pr-approval.json
  tools/github/public-root-identity.json
  tools/github/repository-identity.json
  tools/github/legal-identity.json
  tools/github/third-party-artifact-policy.json
  tools/actionlint.env
  tools/gitleaks.env
  SECURITY.md
  scripts/ci/gitleaks-scan.sh
  scripts/ci/actionlint.sh
  scripts/ci/materialize-public-candidate-tree.sh
  scripts/ci/materialize-public-candidate-tree.py
  scripts/ci/require-successful-needs.sh
  scripts/github/github-policy-json.py
  scripts/github/validate-legal-publication.sh
  scripts/github/validate-public-repository-identity.sh
  scripts/github/check-actions-cache-controls.sh
  scripts/github/check-billing-budgets.sh
  scripts/github/check-publication-authority.sh
  scripts/github/safe-git-transport.sh
  scripts/github/show-public-repository-identity.sh
  scripts/github/configure-public-repository.sh
  scripts/github/build-public-root.sh
  scripts/github/prepare-public-root.sh
  scripts/github/publish-public-root.sh
  scripts/github/import-private-feature-diff.sh
  scripts/guard/import-private-feature-diff-self-test.sh
  scripts/guard/actions-cache-controls-self-test.sh
  scripts/guard/billing-budgets-self-test.sh
  scripts/guard/publication-authority-self-test.sh
  scripts/guard/check-tracked-blob-sizes.sh
  scripts/guard/tracked-blob-sizes-self-test.sh
  scripts/guard/cargo-verify-isolation-self-test.sh
  scripts/guard/public-tree-snapshot-self-test.sh
  scripts/guard/frontend-test-isolation-self-test.sh
  scripts/guard/safe-git-transport-self-test.sh
  scripts/guard/repository-identity-capture-self-test.sh
  scripts/guard/check-public-root-publisher.py
  scripts/guard/check-repository-identity-prepare.py
  scripts/guard/check-legal-publication-prepare.py
  scripts/guard/public-root-publisher-self-test.sh
  scripts/guard/legal-publication-ci.sh
  scripts/guard/legal-publication-self-test.sh
  scripts/guard/third-party-artifact-policy-self-test.sh
  scripts/guard/repository-identity-ci.sh
  scripts/guard/repository-identity-policy-self-test.sh
)
for required_file in "${required_files[@]}"; do
  if [ ! -f "$required_file" ]; then
    echo "FAIL public-github-policy: missing $required_file" >&2
    exit 1
  fi
done
for command_name in python3 grep; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-github-policy: missing command '$command_name'" >&2
    exit 1
  }
done

bash scripts/guard/check-workflow-policy.sh \
  .github/workflows tools/github/main-ruleset.json tools/github/selected-actions.json

python3 - <<'PY'
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


def load(name: str):
    with Path("tools/github", name).open(encoding="utf-8") as handle:
        return json.load(handle)


def require(condition: bool, message: str) -> None:
    if not condition:
        print(f"FAIL public-github-policy: {message}", file=sys.stderr)
        raise SystemExit(1)


expected_gitattributes = """* text=auto eol=lf
*.[cC][mM][dD] text eol=crlf
*.[bB][aA][tT] text eol=crlf
*.png binary
*.jpg binary
*.jpeg binary
*.gif binary
*.ico binary
*.pdf binary
*.zip binary
*.tar.gz binary
*.woff binary
*.woff2 binary
"""
require(Path(".gitattributes").read_text(encoding="utf-8")
        == expected_gitattributes,
        "root checkout-byte attributes drifted")
candidate_paths = subprocess.run(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    check=True,
    capture_output=True,
).stdout.decode("utf-8").split("\0")
candidate_gitattributes = sorted(
    path for path in candidate_paths
    if path and Path(path).name == ".gitattributes" and Path(path).is_file()
)
require(candidate_gitattributes == [".gitattributes"],
        "root .gitattributes must be the checkout-byte SSOT")

third_party_policy = load("third-party-artifact-policy.json")
expected_third_party_paths = {
    ".gitattributes",
    "LICENSES/OFL-1.1.txt",
    "THIRD_PARTY_NOTICES.md",
    "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt",
    "products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256",
    "products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css",
}
require(isinstance(third_party_policy, dict)
        and set(third_party_policy) == {"version", "artifacts"}
        and type(third_party_policy["version"]) is int
        and third_party_policy["version"] == 1
        and isinstance(third_party_policy["artifacts"], dict)
        and set(third_party_policy["artifacts"]) == expected_third_party_paths,
        "third-party artifact policy schema or exact path allowlist drifted")
for artifact_path, expected_hash in third_party_policy["artifacts"].items():
    require(re.fullmatch(r"[0-9a-f]{64}", expected_hash) is not None
            and hashlib.sha256(Path(artifact_path).read_bytes()).hexdigest()
            == expected_hash,
            f"third-party artifact policy hash drifted: {artifact_path}")

settings = load("repository-settings.json")
require("visibility" not in settings and "private" not in settings,
        "repository policy must never change visibility")
require(settings.get("allow_merge_commit") is False
        and settings.get("allow_rebase_merge") is False
        and settings.get("allow_squash_merge") is True,
        "only squash merge may be enabled")

actions = load("actions-policy.json")
require(actions == {
    "enabled": True,
    "allowed_actions": "selected",
    "sha_pinning_required": True,
}, "Actions policy must be enabled, selected-only, and SHA-pinned")
workflow_permissions = load("workflow-permissions.json")
require(workflow_permissions == {
    "default_workflow_permissions": "read",
    "can_approve_pull_request_reviews": False,
}, "workflow token policy must remain read-only")
require(load("artifact-retention.json") == {"days": 7},
        "artifact retention must remain seven days")
require(load("actions-cache-policy.json") == {
    "max_cache_size_gb": 10,
    "max_cache_retention_days": 7,
    "unavailable_http_status": 402,
    "unavailable_message": "Please ensure your account has a valid payment method on file to access this service.",
    "required_owner_plan_when_unavailable": "free",
}, "Actions cache policy must remain bounded or prove no paid opt-in")
require(load("billing-budget-policy.json") == {
    "owner": "perfectory-inc",
    "required_budgets": [
        {
            "budget_type": "ProductPricing",
            "budget_product_sku": "actions",
            "budget_scope": "organization",
            "budget_entity_name": "perfectory-inc",
            "budget_amount": 0,
            "prevent_further_usage": True,
        },
        {
            "budget_type": "SkuPricing",
            "budget_product_sku": "actions_cache_storage",
            "budget_scope": "organization",
            "budget_entity_name": "perfectory-inc",
            "budget_amount": 0,
            "prevent_further_usage": True,
        },
    ],
}, "billing policy must hard-stop Actions and cache storage at USD 0")
require(load("fork-pr-approval.json") == {"approval_policy": "all_external_contributors"},
        "every external fork workflow must require approval")

main = load("main-ruleset.json")
require(main.get("name") == "perfectory-public-main"
        and main.get("target") == "branch"
        and main.get("enforcement") == "active"
        and main.get("bypass_actors") == [],
        "main ruleset identity/enforcement/bypass drift")
require(main.get("conditions") == {
    "ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}
}, "main ruleset must target only the default branch")
rules = {rule["type"]: rule for rule in main.get("rules", [])}
require(set(rules) == {
    "deletion", "non_fast_forward", "required_linear_history",
    "pull_request", "required_status_checks",
}, "main ruleset has missing or unexpected rule types")
pr = rules["pull_request"].get("parameters")
require(pr == {
    "allowed_merge_methods": ["squash"],
    "dismiss_stale_reviews_on_push": True,
    "dismissal_restriction": {"allowed_actors": [], "enabled": False},
    "require_code_owner_review": False,
    "require_last_push_approval": False,
    "required_approving_review_count": 0,
    "required_review_thread_resolution": True,
    "required_reviewers": [],
}, "pull-request rule contains hidden reviewers, dismissal actors, or weakened gates")
status = rules["required_status_checks"].get("parameters", {})
checks = status.get("required_status_checks", [])
contexts = [check.get("context") for check in checks]
require(status.get("strict_required_status_checks_policy") is True
        and status.get("do_not_enforce_on_create") is True
        and contexts
        and len(contexts) == len(set(contexts))
        and all(re.fullmatch(r"required/[A-Za-z0-9._/-]+", value or "") for value in contexts)
        and all(check.get("integration_id") == 15368 for check in checks),
        "required checks must be unique GitHub-Actions-owned required/* contexts")

bootstrap_main = load("bootstrap-main-ruleset.json")
require(bootstrap_main == {
    "name": "perfectory-public-main",
    "target": "branch",
    "enforcement": "active",
    "bypass_actors": [],
    "conditions": {
        "ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}
    },
    "rules": [
        {"type": "deletion"},
        {"type": "update"},
    ],
}, "bootstrap main ruleset must allow one creation then deny every update")

branch = load("non-main-branch-firewall.json")
require(branch == {
    "name": "perfectory-public-non-main-branch-firewall",
    "target": "branch",
    "enforcement": "active",
    "bypass_actors": [{
        "actor_id": 29110,
        "actor_type": "Integration",
        "bypass_mode": "always",
    }],
    "conditions": {
        "ref_name": {"include": ["~ALL"], "exclude": ["~DEFAULT_BRANCH"]}
    },
    "rules": [
        {"type": "creation"},
        {"type": "update"},
    ],
}, "non-main firewall must deny humans and bypass only Dependabot App 29110")

bootstrap_branch = load("bootstrap-non-main-branch-firewall.json")
expected_bootstrap_branch = dict(branch)
expected_bootstrap_branch["bypass_actors"] = []
require(bootstrap_branch == expected_bootstrap_branch,
        "prepublication branch firewall must have zero bypass actors")

tags = load("tag-firewall.json")
require(tags == {
    "name": "perfectory-public-tag-firewall",
    "target": "tag",
    "enforcement": "active",
    "bypass_actors": [],
    "conditions": {"ref_name": {"include": ["~ALL"], "exclude": []}},
    "rules": [{"type": "creation"}],
}, "tag creation must remain denied without bypass")

identity = load("public-root-identity.json")
require(identity.get("author_name") == "Perfectory"
        and identity.get("author_email") == "public-root@perfectory.invalid"
        and identity.get("commit_message") == "chore: publish audited source snapshot"
        and re.fullmatch(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
                         identity.get("commit_date_utc", "")),
        "public root identity must be the neutral project identity")

configurator = Path("scripts/github/configure-public-repository.sh").read_text(encoding="utf-8")
for mode in ("bootstrap", "prepublish", "lock", "activate", "protect", "verify"):
    require(re.search(rf"^  {mode}\)$", configurator, re.MULTILINE) is not None,
            f"configurator is missing {mode} mode")
require('expected_target="perfectory-inc/perfectory-public"' in configurator,
        "configurator target is not canonical")
require("env -u GH_HOST gh api --hostname github.com" in configurator,
        "configurator must pin GitHub.com")
require('repository_identity_validator="$root/scripts/github/validate-public-repository-identity.sh"'
        in configurator
        and "--allow-unset" not in configurator,
        "configurator must use the strict control-root repository identity wrapper")
repository_identity_function = configurator.split(
    "verify_repository_identity() {", 1)[1].split("\n}", 1)[0]
require(repository_identity_function.index('bash "$repository_identity_validator"')
        < repository_identity_function.index('actual="$(mktemp)"')
        < repository_identity_function.index('api "repos/$target"'),
        "repository identity must be strict before the first live repository read")
require('legal_validator="$root/scripts/github/validate-legal-publication.sh"'
        in configurator
        and 'bash "$legal_validator"' in configurator
        and "--allow-unconfirmed" not in configurator,
        "configurator must use the strict control-root legal validator")
first_live_repository_read = configurator.index("\nverify_repository_identity\n")
legal_preflight = configurator[:first_live_repository_read]
require(re.search(
    r'if \[ "\$mode" = bootstrap \] \|\| \[ "\$mode" = prepublish \]; then\s+'
    r'bash "\$legal_validator"\s+fi',
    legal_preflight,
) is not None,
        "bootstrap/prepublish must validate legal identity before any live GitHub read")
require("require_empty_remote" in configurator
        and "require_bootstrap_ruleset_subset" in configurator
        and "require_published_root" in configurator
        and "verify_effective_ruleset_set" in configurator,
        "configurator lacks empty/published/effective-ruleset gates")
require("--jq '.enabled'" in configurator
        and "private_reporting_enabled" in configurator,
        "private vulnerability reporting must be read back as enabled=true")
require('apply_ruleset "$branch_firewall"' in configurator
        and 'apply_ruleset "$bootstrap_branch_firewall"' in configurator
        and 'apply_ruleset "$tag_firewall"' in configurator,
        "publication state machine must install bootstrap/final branch and tag firewalls")
require('api --method DELETE "repos/$target/automated-security-fixes"' in configurator
        and 'verify_base_settings disabled' in configurator
        and 'api --method PUT "repos/$target/automated-security-fixes"' in configurator,
        "security-update branch creation must stay disabled until activation")
require("check-actions-cache-controls.sh" in configurator
        and "check-billing-budgets.sh" in configurator
        and "check-publication-authority.sh" in configurator
        and '"$budget_checker" "$budget_policy"' in configurator
        and "BUDGET_ZERO_CONFIRMED" not in configurator,
        "publication must read back cache controls and two USD-zero hard stops")
require(configurator.index('"$budget_checker" "$budget_policy"')
        < configurator.index('case "$mode" in'),
        "USD-zero budgets must be proven before any configurator mode can mutate GitHub")
bootstrap_block = configurator.split("  bootstrap)", 1)[1].split("    ;;", 1)[0]
prepublish_block = configurator.split("  prepublish)", 1)[1].split("    ;;", 1)[0]
lock_block = configurator.split("  lock)", 1)[1].split("    ;;", 1)[0]
activate_block = configurator.split("  activate)", 1)[1].split("    ;;", 1)[0]
protect_block = configurator.split("  protect)", 1)[1].split("    ;;", 1)[0]
require(bootstrap_block.index('apply_ruleset "$bootstrap_branch_firewall"')
        < bootstrap_block.index('apply_ruleset "$bootstrap_main_policy"')
        < bootstrap_block.index('"$cache_checker"')
        < bootstrap_block.rindex('--input "$policy_dir/actions-policy.json"'),
        "zero-bypass, main lock, and cache controls must precede Actions/publication")
require(bootstrap_block.index('"$authority_checker"')
        < bootstrap_block.index("require_empty_remote")
        and prepublish_block.index('"$authority_checker"')
        < prepublish_block.index("require_empty_remote"),
        "bootstrap and prepublish must re-read sole-writer authority before publication")
require("require_published_root" in lock_block
        and 'verify_ruleset "$bootstrap_main_policy"' in lock_block
        and "apply_ruleset" not in lock_block
        and "$baseline_policy" not in configurator,
        "post-push lock must preserve the update-deny bootstrap policy")
require(activate_block.index('apply_ruleset "$branch_firewall"')
        < activate_block.index('automated-security-fixes'),
        "Dependabot-only firewall must precede enabling security-update branches")
require('verify_ruleset "$bootstrap_main_policy"' in activate_block
        and 'apply_ruleset "$main_policy"' not in activate_block,
        "activation must not weaken the update-deny main policy")
first_expected_root = protect_block.index("require_expected_main")
green_checks = protect_block.index("require_green_contexts")
full_policy = protect_block.index('apply_ruleset "$main_policy"')
second_expected_root = protect_block.index(
    "require_expected_main", first_expected_root + 1)
require(first_expected_root < green_checks < full_policy < second_expected_root
        and 'verify_ruleset_one_of "$bootstrap_main_policy" "$main_policy"'
        in protect_block
        and configurator.count('apply_ruleset "$main_policy"') == 1
        and 'lock|activate|protect)' in configurator,
        "only protect may swap update-deny for full policy after green root checks")
require('if [ "$head_sha" != "${PERFECTORY_EXPECTED_PUBLIC_ROOT:-}" ]' in configurator,
        "required checks must be tied to the audited root SHA")
require(re.search(r"^\s*jq\s", configurator, re.MULTILINE) is None,
        "configurator must not depend on an unpinned jq executable")

cache_checker = Path("scripts/github/check-actions-cache-controls.sh").read_text(
    encoding="utf-8")
for endpoint in (
    "actions/cache/storage-limit",
    "actions/cache/retention-limit",
    "actions/cache/usage",
):
    require(endpoint in cache_checker,
            f"Actions cache checker is missing endpoint: {endpoint}")
require("unavailable_http_status" in cache_checker
        and "unavailable_message" in cache_checker
        and "required_owner_plan_when_unavailable" in cache_checker
        and "--method PUT" not in cache_checker,
        "cache checker must fail closed without opting into paid storage")

budget_checker = Path("scripts/github/check-billing-budgets.sh").read_text(
    encoding="utf-8")
require("settings/billing/budgets?per_page=100" in budget_checker
        and 'env -u GH_HOST gh api --hostname github.com' in budget_checker
        and "has_next_page" in budget_checker
        and "prevent_further_usage" in budget_checker
        and "--method" not in budget_checker,
        "billing checker must be read-only, complete, and fail closed")

authority_checker = Path("scripts/github/check-publication-authority.sh").read_text(
    encoding="utf-8")
for endpoint in (
    "orgs/$organization/members?filter=all&role=all&per_page=100",
    "orgs/$organization/installations?per_page=100",
    "repos/$target/collaborators?affiliation=direct&per_page=100",
    "repos/$target/keys?per_page=100",
):
    require(endpoint in authority_checker,
            f"publication authority inventory is missing: {endpoint}")
require("--paginate --slurp" in authority_checker
        and 'env -u GH_HOST gh api --hostname github.com' in authority_checker
        and "--method" not in authority_checker,
        "publication authority proof must be complete and read-only")

builder = Path("scripts/github/build-public-root.sh").read_text(encoding="utf-8")
require("commit-tree" in builder and "--bare --initial-branch=main" in builder,
        "root builder must create one parentless bare main")
require("safe-git-transport.sh" in builder
        and '"$git_transport" --no-repository init' in builder,
        "root builder must use the isolated Git/config transport")

transport = Path("scripts/github/safe-git-transport.sh").read_text(encoding="utf-8")
require("GIT_*|HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY" in transport
        and "GIT_CONFIG_NOSYSTEM=1" in transport
        and "GIT_CONFIG_GLOBAL=/dev/null" in transport
        and "credential.helper=" in transport
        and "url\\..*\\.(insteadof|pushinsteadof)" in transport
        and "core\\.(fsmonitor|hookspath|attributesfile|worktree|excludesfile" in transport
        and "filter\\..*" in transport
        and "http\\..*" in transport
        and "http.sslVerify=true" in transport,
        "safe Git transport must isolate executable config, proxies, TLS, and credentials")
for publication_script in (
    "scripts/github/configure-public-repository.sh",
    "scripts/github/build-public-root.sh",
    "scripts/github/prepare-public-root.sh",
    "scripts/github/publish-public-root.sh",
):
    publication_source = Path(publication_script).read_text(encoding="utf-8")
    require("safe-git-transport.sh" in publication_source,
            f"publication path bypasses safe Git transport: {publication_script}")
    require(re.search(r"^\s*git\s+.*\b(push|clone|ls-remote)\b",
                      publication_source, re.MULTILINE) is None,
            f"publication path contains raw Git transport: {publication_script}")

cargo_verify = Path("scripts/verify/cargo-verify.sh").read_text(encoding="utf-8")
xtask = Path("tools/xtask/src/main.rs").read_text(encoding="utf-8")
require("pack-objects --stdout" in cargo_verify
        and "index-pack --stdin" in cargo_verify
        and "gitdir: /perfectory-git" in cargo_verify
        and "core.worktree /work" in cargo_verify
        and "target=/work/.git,readonly" in cargo_verify
        and "PERFECTORY_GIT_DIR" not in cargo_verify
        and "PERFECTORY_GIT_INDEX_FILE" not in cargo_verify
        and "PERFECTORY_GIT_DIR" not in xtask
        and '.env("GIT_DIR"' not in xtask
        and '.env("GIT_WORK_TREE"' not in xtask
        and "PERFECTORY_GIT_INDEX_FILE" not in xtask,
        "Docker verification must use a worktree-local isolated Git pointer without private history or ambient Git state")

prepare = Path("scripts/github/prepare-public-root.sh").read_text(encoding="utf-8")
for needle in (
    "status --porcelain=v1 --untracked-files=all",
    "bash scripts/guard/monorepo-guard.sh",
    "scripts/ci/reuse-lint.sh",
    "scripts/ci/lychee-docs.sh",
    "scripts/ci/gitleaks-scan.sh tree",
    "PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh products/gongzzang",
    "PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh platforms/foundation-platform",
    "PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh platforms/identity-platform",
    "PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh platforms/intelligence-platform",
    "scripts/verify/frontend-test.sh",
    'control_legal_validator="$root/scripts/github/validate-legal-publication.sh"',
    'candidate_legal_validator="$source_root/scripts/github/validate-legal-publication.sh"',
    'bash "$control_legal_validator"',
    'bash "$candidate_legal_validator"',
    'control_repository_identity_validator="$root/scripts/github/validate-public-repository-identity.sh"',
    'candidate_repository_identity_validator="$source_root/scripts/github/validate-public-repository-identity.sh"',
    'bash "$control_repository_identity_validator"',
    'bash "$candidate_repository_identity_validator"',
    "ls-files -v",
    "source index contains skip-worktree or assume-unchanged entries",
    "bash scripts/github/validate-legal-publication.sh",
    "bash scripts/github/validate-public-repository-identity.sh",
):
    require(needle in prepare, f"exact-tree preparation gate is missing: {needle}")
require(prepare.index('bash "$control_repository_identity_validator"')
        < prepare.index('bash "$control_legal_validator"')
        < prepare.index("ls-files -v")
        < prepare.index("status --porcelain=v1 --untracked-files=all")
        < prepare.index('bash "$candidate_repository_identity_validator"')
        < prepare.index('bash "$candidate_legal_validator"')
        < prepare.index('"$root/scripts/github/build-public-root.sh"')
        and "--allow-unconfirmed" not in prepare,
        "control and exact clean candidate legal gates must precede root construction")
require(prepare.count("bash scripts/github/validate-legal-publication.sh") == 1
        and prepare.count("bash scripts/github/validate-public-repository-identity.sh") == 1
        and prepare.index("clone --quiet --no-local")
        < prepare.index("bash scripts/github/validate-public-repository-identity.sh")
        < prepare.index("bash scripts/github/validate-legal-publication.sh")
        < prepare.index("bash scripts/guard/monorepo-guard.sh")
        < prepare.index("CI=true bash scripts/ci/reuse-lint.sh")
        < prepare.index("bash scripts/ci/gitleaks-scan.sh all ."),
        "strict legal and all publication gates must run in order on the cloned root")
require("worktree add" not in prepare,
        "publication audit must not run against a filterable source worktree")
require("audit_env=(" in prepare
        and "env -i" in prepare
        and "GIT_CONFIG_NOSYSTEM=1" in prepare
        and '"${audit_env[@]}" bash -ceu' in prepare,
        "publication audit must not inherit caller Git/config execution state")

materializer = Path("scripts/ci/materialize-public-candidate-tree.py").read_text(
    encoding="utf-8")
require("PERFECTORY_SAFE_GIT_TRANSPORT" in materializer
        and "PERFECTORY_TRUSTED_GIT_INDEX_FILE" in materializer
        and '"git",' not in materializer,
        "candidate materializer must use the isolated Git/index transport")

importer = Path("scripts/github/import-private-feature-diff.sh").read_text(encoding="utf-8")
require(" diff --binary --full-index" in importer
        and "--no-ext-diff --no-textconv" in importer,
        "private feature importer must transfer only a binary tree diff")
for forbidden in (" fetch ", " push ", " bundle ", "format-patch"):
    require(forbidden not in importer,
            f"private feature importer contains forbidden history operation: {forbidden.strip()}")
require("perfectory-inc/perfectory-public" in importer
        and 'canonical_remote_url="https://github.com/perfectory-inc/perfectory-public.git"' in importer
        and '"$git_transport" --no-repository' in importer
        and 'ls-remote "$canonical_remote_url" refs/heads/main' in importer
        and '"$public_main" != "$live_main"' in importer
        and "safe-git-transport.sh" in importer
        and "PERFECTORY_TRUSTED_GIT_INDEX_FILE" in importer
        and "env -i" in importer
        and "scripts/guard/monorepo-guard.sh" in importer
        and "scripts/ci/gitleaks-scan.sh tree" in importer,
        "private feature importer lacks live canonical-main or safety gates")
require(re.search(r"^\s*git\s+-C", importer, re.MULTILINE) is None,
        "private feature importer bypasses isolated Git execution")

legal_wrapper = Path("scripts/github/validate-legal-publication.sh").read_text(
    encoding="utf-8")
json_helper = Path("scripts/github/github-policy-json.py").read_text(
    encoding="utf-8")
repository_safety = Path("scripts/guard/public-repository-safety.sh").read_text(
    encoding="utf-8")
require("third-party-artifact-policy.json" in legal_wrapper
        and "validate_third_party_artifact_policy" in json_helper
        and "THIRD_PARTY_ARTIFACT_PATHS" in json_helper
        and "hashlib.sha256" in json_helper
        and "REUSE.toml legal annotation allowlist drift" in json_helper,
        "legal wrapper must pin the exact REUSE and third-party artifact contracts")
require('attribute_test_root="$(mktemp -d)"' in repository_safety
        and 'cp -- .gitattributes "$attribute_test_root/.gitattributes"'
        in repository_safety
        and 'git -C "$attribute_test_root" check-attr text eol'
        in repository_safety
        and "exact-hashed legal artifact lacks LF checkout attributes"
        in repository_safety
        and "WOFF2 checkout attributes are not binary" in repository_safety,
        "public safety must preserve exact-hash checkout byte attributes")

ci_gate = Path("scripts/guard/legal-publication-ci.sh").read_text(encoding="utf-8")
require('[ "${GITHUB_REPOSITORY:-}" = "perfectory-inc/perfectory-public" ]'
        in ci_gate
        and ci_gate.index('exec bash "$validator"')
        < ci_gate.index('exec bash "$validator" --allow-unconfirmed')
        and ci_gate.count("--allow-unconfirmed") == 1,
        "canonical public CI must be strict and only noncanonical CI may lint provisionally")

repository_identity_ci = Path("scripts/guard/repository-identity-ci.sh").read_text(
    encoding="utf-8")
require('[ "${GITHUB_REPOSITORY:-}" = "perfectory-inc/perfectory-public" ]'
        in repository_identity_ci
        and "GITHUB_REPOSITORY_ID" in repository_identity_ci
        and "GITHUB_REPOSITORY_OWNER_ID" in repository_identity_ci
        and "validate-repository-runtime-identity" in repository_identity_ci
        and repository_identity_ci.index('bash "$validator"')
        < repository_identity_ci.index('exec bash "$validator" --allow-unset')
        and repository_identity_ci.count("--allow-unset") == 1,
        "canonical CI must match immutable runtime IDs; only noncanonical CI may allow unset")

monorepo_guard = Path("scripts/guard/monorepo-guard.sh").read_text(encoding="utf-8")
require('legal_gate="$root/scripts/guard/legal-publication-ci.sh"'
        in monorepo_guard
        and 'repository_identity_gate="$root/scripts/guard/repository-identity-ci.sh"'
        in monorepo_guard
        and 'bash "$repository_identity_gate"' in monorepo_guard
        and 'bash "$legal_gate"' in monorepo_guard
        and "legal-publication-self-test" in monorepo_guard
        and "third-party-artifact-policy-self-test" in monorepo_guard
        and "repository-identity-policy-self-test" in monorepo_guard
        and "--allow-unconfirmed" not in monorepo_guard
        and "--allow-unset" not in monorepo_guard
        and monorepo_guard.index('bash "$repository_identity_gate"')
        < monorepo_guard.index('bash "$legal_gate"')
        < monorepo_guard.index("for g in"),
        "monorepo verification must run immutable identity and legal CI gates first")

publisher = Path("scripts/github/publish-public-root.sh").read_text(encoding="utf-8")
publisher_prepare = '"$prepare" "$source_root" "$2" "$snapshot"'
publisher_remote_read = '"$git_transport" --no-repository ls-remote --symref "$remote_url"'
require(publisher.count(publisher_prepare) == 1
        and publisher.index(publisher_prepare) < publisher.index(publisher_remote_read),
        "publisher must cross mandatory preparation before reading publication state")

public_policy = Path("scripts/guard/public-github-policy.sh").read_text(encoding="utf-8")
runtime_policy = public_policy.split("\nPY\n", 1)[1]
require("bash scripts/github/validate-legal-publication.sh --allow-unconfirmed"
        in runtime_policy
        and "bash scripts/github/validate-public-repository-identity.sh --allow-unset"
        in runtime_policy
        and "bash scripts/guard/legal-publication-self-test.sh" in runtime_policy
        and "bash scripts/guard/third-party-artifact-policy-self-test.sh"
        in runtime_policy
        and "bash scripts/guard/repository-identity-policy-self-test.sh"
        in runtime_policy
        and "python3 scripts/guard/check-repository-identity-prepare.py"
        in runtime_policy
        and "python3 scripts/guard/check-legal-publication-prepare.py"
        in runtime_policy
        and "github-policy-json.py validate-legal-identity" not in runtime_policy
        and "github-policy-json.py validate-repository-identity" not in runtime_policy,
        "local policy lint must use and test the canonical identity wrappers")
PY

bash scripts/github/validate-public-repository-identity.sh --allow-unset
# This guard is also used for private/local development, so it performs the
# explicit provisional structural lint. The monorepo entrypoint selects strict
# validation when GitHub identifies the canonical public repository.
bash scripts/github/validate-legal-publication.sh --allow-unconfirmed
python3 scripts/guard/check-repository-identity-prepare.py \
  scripts/github/prepare-public-root.sh
python3 scripts/guard/check-legal-publication-prepare.py \
  scripts/github/prepare-public-root.sh
bash scripts/guard/repository-identity-policy-self-test.sh
bash scripts/guard/legal-publication-self-test.sh
bash scripts/guard/third-party-artifact-policy-self-test.sh

bash scripts/guard/cargo-verify-isolation-self-test.sh
bash scripts/guard/actions-cache-controls-self-test.sh
bash scripts/guard/billing-budgets-self-test.sh
bash scripts/guard/publication-authority-self-test.sh
bash scripts/guard/public-tree-snapshot-self-test.sh
bash scripts/guard/frontend-test-isolation-self-test.sh
bash scripts/guard/safe-git-transport-self-test.sh
bash scripts/guard/repository-identity-capture-self-test.sh

python3 scripts/guard/check-public-root-publisher.py \
  scripts/github/publish-public-root.sh
bash scripts/guard/public-root-publisher-self-test.sh

if ! grep -Eq '^GITLEAKS_VERSION=[0-9]+\.[0-9]+\.[0-9]+$' tools/gitleaks.env \
  || [ "$(grep -Ec '^GITLEAKS_VERSION=' tools/gitleaks.env)" -ne 1 ] \
  || [ "$(grep -Ec '^GITLEAKS_(LINUX|DARWIN|WINDOWS)_(X64|ARM64)_SHA256=[0-9a-f]{64}$' tools/gitleaks.env)" -ne 6 ] \
  || [ "$(grep -Ec '^GITLEAKS_(LINUX|DARWIN|WINDOWS)_(X64|ARM64)_SHA256=' tools/gitleaks.env)" -ne 6 ]; then
  echo "FAIL public-github-policy: gitleaks release/checksum SSOT is malformed" >&2
  exit 1
fi

echo "OK public-github-policy"
