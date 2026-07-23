#!/usr/bin/env bash
# Applies and reads back the public repository's GitHub-side security contract.
# Usage: configure-public-repository.sh <bootstrap|prepublish|lock|activate|protect|verify>
set -euo pipefail

# GitHub policy helpers emit Unicode diagnostics; force a stable encoding on
# Windows hosts whose Python locale otherwise defaults to cp949.
export PYTHONIOENCODING=utf-8

mode="${1:?usage: $0 <bootstrap|prepublish|lock|activate|protect|verify>}"
target="${PERFECTORY_PUBLIC_REPOSITORY:-perfectory-inc/perfectory-public}"
expected_target="perfectory-inc/perfectory-public"
remote_url="https://github.com/${expected_target}.git"
root="$(cd "$(dirname "$0")/../.." && pwd)"
policy_dir="$root/tools/github"
json_helper="$root/scripts/github/github-policy-json.py"
legal_validator="$root/scripts/github/validate-legal-publication.sh"
repository_identity_validator="$root/scripts/github/validate-public-repository-identity.sh"
git_transport="$root/scripts/github/safe-git-transport.sh"
cache_checker="$root/scripts/github/check-actions-cache-controls.sh"
budget_checker="$root/scripts/github/check-billing-budgets.sh"
authority_checker="$root/scripts/github/check-publication-authority.sh"
repository_identity="$policy_dir/repository-identity.json"
cache_policy="$policy_dir/actions-cache-policy.json"
budget_policy="$policy_dir/billing-budget-policy.json"
main_policy="$policy_dir/main-ruleset.json"
bootstrap_main_policy="$policy_dir/bootstrap-main-ruleset.json"
bootstrap_branch_firewall="$policy_dir/bootstrap-non-main-branch-firewall.json"
branch_firewall="$policy_dir/non-main-branch-firewall.json"
tag_firewall="$policy_dir/tag-firewall.json"
api_version="2026-03-10"
actions_app_id=15368

if [ "$target" != "$expected_target" ]; then
  echo "FAIL public-repository-config: refusing unexpected target '$target'" >&2
  exit 1
fi
if [ -n "${GH_HOST:-}" ] && [ "$GH_HOST" != github.com ]; then
  echo "FAIL public-repository-config: GH_HOST must be unset or github.com" >&2
  exit 1
fi
for command_name in gh git python3 diff mktemp sed; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-repository-config: missing command '$command_name'" >&2
    exit 1
  }
done
for required_file in \
  "$json_helper" "$legal_validator" "$repository_identity_validator" \
  "$git_transport" "$cache_checker" "$budget_checker" \
  "$authority_checker" \
  "$repository_identity" "$cache_policy" "$budget_policy" \
  "$main_policy" "$bootstrap_main_policy" \
  "$bootstrap_branch_firewall" "$branch_firewall" "$tag_firewall"; do
  [ -f "$required_file" ] || {
    echo "FAIL public-repository-config: missing $required_file" >&2
    exit 1
  }
done

api() {
  env -u GH_HOST gh api --hostname github.com \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: $api_version" \
    "$@"
}

compare_json() {
  local label="$1"
  local expected_file="$2"
  local actual_file="$3"
  local expected_sorted actual_sorted
  expected_sorted="$(mktemp)"
  actual_sorted="$(mktemp)"
  python3 "$json_helper" canonical "$expected_file" >"$expected_sorted"
  python3 "$json_helper" canonical "$actual_file" >"$actual_sorted"
  if ! diff -u "$expected_sorted" "$actual_sorted"; then
    echo "FAIL public-repository-config: $label drift" >&2
    rm -f -- "$expected_sorted" "$actual_sorted"
    return 1
  fi
  rm -f -- "$expected_sorted" "$actual_sorted"
}

verify_repository_identity() {
  local actual
  bash "$repository_identity_validator"
  actual="$(mktemp)"
  api "repos/$target" --jq '{
    hostname: "github.com",
    full_name,
    repository_id: .id,
    repository_node_id: .node_id,
    owner: {login: .owner.login, id: .owner.id, node_id: .owner.node_id}
  }' >"$actual"
  compare_json repository-identity "$repository_identity" "$actual"
  rm -f -- "$actual"
}

verify_automated_security_fixes() {
  local expected="$1"
  local response status exit_code
  if [ "$expected" = enabled ]; then
    api "repos/$target/automated-security-fixes" >/dev/null
    return
  fi
  if [ "$expected" != disabled ] && [ "$expected" != either ]; then
    echo "FAIL public-repository-config: invalid security-fix expectation" >&2
    return 1
  fi
  [ "$expected" = either ] && return
  set +e
  response="$(api --include "repos/$target/automated-security-fixes" 2>&1)"
  exit_code=$?
  set -e
  status="$(printf '%s\n' "$response" | sed -n '1s#^HTTP/[0-9.]* \([0-9][0-9][0-9]\).*#\1#p')"
  # GitHub.com returns either a 404 (legacy behavior) or a 200 JSON body with
  # {"enabled":false}; both are valid disabled states.
  if [ "$exit_code" -eq 0 ] && printf '%s\n' "$response" \
    | tail -n 1 | grep -Eq '"enabled"[[:space:]]*:[[:space:]]*false'; then
    return
  fi
  if [ "$exit_code" -eq 0 ] || [ "$status" != 404 ]; then
    echo "FAIL public-repository-config: automated security fixes must remain disabled before main is locked" >&2
    return 1
  fi
}

verify_base_settings() {
  local security_fixes="${1:?security-fix state required}"
  local actual private_reporting_enabled
  actual="$(mktemp)"

  api "repos/$target" --jq '{
    description,
    has_discussions,
    has_issues,
    has_projects,
    has_wiki,
    allow_auto_merge,
    allow_merge_commit,
    allow_rebase_merge,
    allow_squash_merge,
    allow_update_branch,
    delete_branch_on_merge,
    squash_merge_commit_message,
    squash_merge_commit_title,
    security_and_analysis: {
      secret_scanning: .security_and_analysis.secret_scanning,
      secret_scanning_push_protection: .security_and_analysis.secret_scanning_push_protection
    }
  }' >"$actual"
  compare_json repository-settings "$policy_dir/repository-settings.json" "$actual"

  api "repos/$target/actions/permissions" \
    --jq '{enabled, allowed_actions, sha_pinning_required}' >"$actual"
  compare_json actions-policy "$policy_dir/actions-policy.json" "$actual"

  api "repos/$target/actions/permissions/selected-actions" \
    --jq '{github_owned_allowed, verified_allowed, patterns_allowed: (.patterns_allowed | sort)}' \
    >"$actual"
  compare_json selected-actions "$policy_dir/selected-actions.json" "$actual"

  api "repos/$target/actions/permissions/workflow" \
    --jq '{default_workflow_permissions, can_approve_pull_request_reviews}' >"$actual"
  compare_json workflow-permissions "$policy_dir/workflow-permissions.json" "$actual"

  api "repos/$target/actions/permissions/artifact-and-log-retention" \
    --jq '{days}' >"$actual"
  compare_json artifact-retention "$policy_dir/artifact-retention.json" "$actual"

  api "repos/$target/actions/permissions/fork-pr-contributor-approval" \
    --jq '{approval_policy}' >"$actual"
  compare_json fork-pr-approval "$policy_dir/fork-pr-approval.json" "$actual"

  api "repos/$target/vulnerability-alerts" >/dev/null
  verify_automated_security_fixes "$security_fixes"
  "$cache_checker" "$target" "$repository_identity" "$cache_policy"
  private_reporting_enabled="$(
    api "repos/$target/private-vulnerability-reporting" --jq '.enabled'
  )"
  if [ "$private_reporting_enabled" != true ]; then
    echo "FAIL public-repository-config: private vulnerability reporting is not enabled" >&2
    rm -f -- "$actual"
    return 1
  fi
  rm -f -- "$actual"
}

verify_ruleset_one_of() {
  local first="$1"
  local second="$2"
  local name actual_raw actual expected
  local -a ids
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["name"])' "$first")"
  ids=()
  mapfile -t ids < <(ruleset_ids "$name")
  if [ "${#ids[@]}" -ne 1 ]; then
    echo "FAIL public-repository-config: expected one transitional ruleset '$name'" >&2
    return 1
  fi
  actual_raw="$(mktemp)"
  actual="$(mktemp)"
  expected="$(mktemp)"
  api "repos/$target/rulesets/${ids[0]}" >"$actual_raw"
  python3 "$json_helper" normalize-ruleset "$actual_raw" >"$actual"
  python3 "$json_helper" normalize-ruleset "$first" >"$expected"
  if diff -q "$expected" "$actual" >/dev/null; then
    rm -f -- "$actual_raw" "$actual" "$expected"
    return
  fi
  python3 "$json_helper" normalize-ruleset "$second" >"$expected"
  if ! diff -q "$expected" "$actual" >/dev/null; then
    echo "FAIL public-repository-config: transitional ruleset is neither admitted state" >&2
    rm -f -- "$actual_raw" "$actual" "$expected"
    return 1
  fi
  rm -f -- "$actual_raw" "$actual" "$expected"
}

ruleset_ids() {
  local name="$1"
  api "repos/$target/rulesets?per_page=100" \
    --jq ".[] | select(.name == \"$name\" and .source_type == \"Repository\") | .id"
}

apply_ruleset() {
  local policy="$1"
  local name
  local -a ids
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["name"])' "$policy")"
  mapfile -t ids < <(ruleset_ids "$name")
  if [ "${#ids[@]}" -gt 1 ]; then
    echo "FAIL public-repository-config: duplicate repository ruleset '$name'" >&2
    return 1
  elif [ "${#ids[@]}" -eq 1 ]; then
    api --method PUT "repos/$target/rulesets/${ids[0]}" --input "$policy" >/dev/null
  else
    api --method POST "repos/$target/rulesets" --input "$policy" >/dev/null
  fi
}

verify_ruleset() {
  local policy="$1"
  local name actual expected
  local -a ids
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["name"])' "$policy")"
  mapfile -t ids < <(ruleset_ids "$name")
  if [ "${#ids[@]}" -ne 1 ]; then
    echo "FAIL public-repository-config: expected one repository ruleset '$name', found ${#ids[@]}" >&2
    return 1
  fi
  actual="$(mktemp)"
  expected="$(mktemp)"
  api "repos/$target/rulesets/${ids[0]}" >"$actual.raw"
  python3 "$json_helper" normalize-ruleset "$actual.raw" >"$actual"
  python3 "$json_helper" normalize-ruleset "$policy" >"$expected"
  if ! diff -u "$expected" "$actual"; then
    echo "FAIL public-repository-config: ruleset '$name' drift" >&2
    rm -f -- "$actual.raw" "$actual" "$expected"
    return 1
  fi
  rm -f -- "$actual.raw" "$actual" "$expected"
}

verify_effective_ruleset_set() {
  local actual_raw actual expected
  actual_raw="$(mktemp)"
  actual="$(mktemp)"
  expected="$(mktemp)"
  api --method GET "repos/$target/rulesets" \
    -f includes_parents=true -F per_page=100 >"$actual_raw"
  python3 "$json_helper" ruleset-summaries "$actual_raw" >"$actual"
  python3 "$json_helper" ruleset-summaries --expected "$@" >"$expected"
  if ! diff -u "$expected" "$actual"; then
    echo "FAIL public-repository-config: effective repository/parent ruleset set drift" >&2
    rm -f -- "$actual_raw" "$actual" "$expected"
    return 1
  fi
  rm -f -- "$actual_raw" "$actual" "$expected"
}

require_bootstrap_ruleset_subset() {
  local actual
  actual="$(mktemp)"
  api --method GET "repos/$target/rulesets" \
    -f includes_parents=true -F per_page=100 >"$actual"
  if ! python3 - "$actual" \
    "$bootstrap_main_policy" "$bootstrap_branch_firewall" "$tag_firewall" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    actual = json.load(handle)
allowed = {}
for path in sys.argv[2:]:
    with open(path, encoding="utf-8") as handle:
        policy = json.load(handle)
    allowed[policy["name"]] = (policy["target"], policy["enforcement"])
for ruleset in actual:
    expected = allowed.get(ruleset.get("name"))
    observed = (ruleset.get("target"), ruleset.get("enforcement"))
    if ruleset.get("source_type") != "Repository" or expected != observed:
        raise SystemExit(1)
PY
  then
    echo "FAIL public-repository-config: bootstrap found an unexpected or inherited ruleset" >&2
    rm -f -- "$actual"
    return 1
  fi
  rm -f -- "$actual"
}

require_empty_remote() {
  local refs
  refs="$("$git_transport" --no-repository ls-remote "$remote_url")"
  if [ -n "$refs" ]; then
    echo "FAIL public-repository-config: bootstrap requires an empty remote with no refs" >&2
    return 1
  fi
}

require_published_root() {
  local refs sha head_sha parent_count matching_refs
  refs="$("$git_transport" --no-repository ls-remote "$remote_url")"
  if [ "$(printf '%s\n' "$refs" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 2 ] \
    || ! printf '%s\n' "$refs" | grep -q $'refs/heads/main$' \
    || ! printf '%s\n' "$refs" | grep -q $'HEAD$'; then
    echo "FAIL public-repository-config: lock requires only HEAD and refs/heads/main" >&2
    return 1
  fi
  sha="$(printf '%s\n' "$refs" | awk '$2 == "refs/heads/main" { print $1 }')"
  head_sha="$(printf '%s\n' "$refs" | awk '$2 == "HEAD" { print $1 }')"
  if [ -z "$sha" ] || [ "$sha" != "$head_sha" ] \
    || [ "$sha" != "${PERFECTORY_EXPECTED_PUBLIC_ROOT:-}" ]; then
    echo "FAIL public-repository-config: remote HEAD/main is not the expected audited root" >&2
    return 1
  fi
  parent_count="$(api "repos/$target/commits/$sha" --jq '.parents | length')"
  if [ "$parent_count" -ne 0 ]; then
    echo "FAIL public-repository-config: published main commit is not parentless" >&2
    return 1
  fi
  matching_refs="$(api "repos/$target/git/matching-refs/" --jq 'map(.ref) | sort | .[]')"
  if [ "$matching_refs" != "refs/heads/main" ]; then
    echo "FAIL public-repository-config: unexpected Git ref exists before main lock" >&2
    printf '%s\n' "$matching_refs" >&2
    return 1
  fi
}

require_expected_main() {
  local refs sha parent_count
  refs="$("$git_transport" --no-repository \
    ls-remote "$remote_url" refs/heads/main)"
  if [ "$(printf '%s\n' "$refs" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 1 ]; then
    echo "FAIL public-repository-config: expected exactly one main ref" >&2
    return 1
  fi
  sha="$(printf '%s\n' "$refs" | awk '$2 == "refs/heads/main" { print $1 }')"
  if [ -z "$sha" ] || [ "$sha" != "${PERFECTORY_EXPECTED_PUBLIC_ROOT:-}" ]; then
    echo "FAIL public-repository-config: main is not the expected audited root" >&2
    return 1
  fi
  parent_count="$(api "repos/$target/commits/$sha" --jq '.parents | length')"
  if [ "$parent_count" -ne 0 ]; then
    echo "FAIL public-repository-config: expected main is not parentless" >&2
    return 1
  fi
}

verify_no_legacy_branch_protection() {
  local response status exit_code
  set +e
  response="$(api --include "repos/$target/branches/main/protection" 2>&1)"
  exit_code=$?
  set -e
  status="$(printf '%s\n' "$response" | sed -n '1s#^HTTP/[0-9.]* \([0-9][0-9][0-9]\).*#\1#p')"
  if [ "$exit_code" -eq 0 ] || [ "$status" != 404 ]; then
    echo "FAIL public-repository-config: unexpected legacy main branch protection or unreadable status ($status)" >&2
    return 1
  fi
}

require_green_contexts() {
  local head_sha context success_count jq_filter
  head_sha="$(api "repos/$target/commits/main" --jq '.sha')"
  if [ "$head_sha" != "${PERFECTORY_EXPECTED_PUBLIC_ROOT:-}" ]; then
    echo "FAIL public-repository-config: refusing checks from a non-audited main commit" >&2
    return 1
  fi
  while IFS= read -r context; do
    printf '%s\n' "$context" | grep -Eq '^required/[A-Za-z0-9._/-]+$' || {
      echo "FAIL public-repository-config: unsafe required context '$context'" >&2
      return 1
    }
    jq_filter="[.check_runs[] | select(.name == \"$context\" and .conclusion == \"success\" and .app.id == $actions_app_id)] | length"
    success_count="$(api --method GET "repos/$target/commits/$head_sha/check-runs" \
      -f check_name="$context" -F app_id="$actions_app_id" -f filter=latest -F per_page=100 \
      --jq "$jq_filter")"
    if [ "$success_count" -lt 1 ]; then
      echo "FAIL public-repository-config: '$context' from GitHub Actions app $actions_app_id is not green on main@$head_sha" >&2
      return 1
    fi
  done < <(python3 "$json_helper" required-contexts "$main_policy")
}

if [ "$mode" = bootstrap ] || [ "$mode" = prepublish ]; then
  bash "$legal_validator"
fi
verify_repository_identity
"$budget_checker" "$budget_policy"
case "$mode" in
  lock|activate|protect)
    if ! printf '%s\n' "${PERFECTORY_EXPECTED_PUBLIC_ROOT:-}" \
      | grep -Eq '^[0-9a-f]{40}$'; then
      echo "FAIL public-repository-config: lock/activate/protect require the audited root SHA" >&2
      exit 1
    fi
    ;;
esac
visibility="$(api "repos/$target" --jq '.visibility')"
if [ "$visibility" != public ]; then
  echo "FAIL public-repository-config: $target must already be public; this script never changes visibility" >&2
  exit 1
fi
default_branch="$(api "repos/$target" --jq '.default_branch')"
if [ "$default_branch" != main ]; then
  echo "FAIL public-repository-config: expected default branch 'main', found '$default_branch'" >&2
  exit 1
fi

case "$mode" in
  bootstrap)
    "$authority_checker"
    require_empty_remote
    # Retrying an interrupted bootstrap is safe: only an existing subset of the
    # exact prepublication rulesets is admitted, then all converge below.
    require_bootstrap_ruleset_subset
    # GitHub rejects the selected-actions/workflow sub-endpoints while Actions
    # is disabled. If a prior attempt was interrupted after disabling Actions,
    # temporarily converge the allowlist while the repository is still empty.
    if [ "$(api "repos/$target/actions/permissions" --jq '.enabled')" != true ]; then
      api --method PUT "repos/$target/actions/permissions" \
        --input "$policy_dir/actions-policy.json" >/dev/null
    fi
    api --method PATCH "repos/$target" --input "$policy_dir/repository-settings.json" >/dev/null
    api --method PUT "repos/$target/vulnerability-alerts" >/dev/null
    api --method DELETE "repos/$target/automated-security-fixes" >/dev/null
    api --method PUT "repos/$target/private-vulnerability-reporting" >/dev/null
    api --method PUT "repos/$target/actions/permissions/selected-actions" \
      --input "$policy_dir/selected-actions.json" >/dev/null
    api --method PUT "repos/$target/actions/permissions/workflow" \
      --input "$policy_dir/workflow-permissions.json" >/dev/null
    api --method PUT "repos/$target/actions/permissions/artifact-and-log-retention" \
      --input "$policy_dir/artifact-retention.json" >/dev/null
    api --method PUT "repos/$target/actions/permissions/fork-pr-contributor-approval" \
      --input "$policy_dir/fork-pr-approval.json" >/dev/null
    # Keep Actions off until the repository rulesets and all other trust
    # boundaries are in place; the final policy enables it after convergence.
    api --method PUT "repos/$target/actions/permissions" \
      -F enabled=false -F sha_pinning_required=true >/dev/null
    apply_ruleset "$bootstrap_branch_firewall"
    apply_ruleset "$tag_firewall"
    apply_ruleset "$bootstrap_main_policy"
    verify_ruleset "$bootstrap_branch_firewall"
    verify_ruleset "$tag_firewall"
    verify_ruleset "$bootstrap_main_policy"
    verify_effective_ruleset_set \
      "$bootstrap_main_policy" "$bootstrap_branch_firewall" "$tag_firewall"
    "$cache_checker" "$target" "$repository_identity" "$cache_policy"
    api --method PUT "repos/$target/actions/permissions" \
      --input "$policy_dir/actions-policy.json" >/dev/null
    verify_base_settings disabled
    ;;
  prepublish)
    "$authority_checker"
    require_empty_remote
    verify_base_settings disabled
    verify_ruleset "$bootstrap_main_policy"
    verify_ruleset "$bootstrap_branch_firewall"
    verify_ruleset "$tag_firewall"
    verify_effective_ruleset_set \
      "$bootstrap_main_policy" "$bootstrap_branch_firewall" "$tag_firewall"
    ;;
  lock)
    verify_base_settings disabled
    require_published_root
    # The update-deny bootstrap rule remains intact until every required root
    # check is green. "lock" verifies that invariant; it does not weaken it.
    verify_ruleset "$bootstrap_main_policy"
    verify_ruleset "$bootstrap_branch_firewall"
    verify_ruleset "$tag_firewall"
    verify_effective_ruleset_set \
      "$bootstrap_main_policy" "$bootstrap_branch_firewall" "$tag_firewall"
    verify_no_legacy_branch_protection
    ;;
  activate)
    # The publisher calls this only after the parentless root is locked and an
    # independent clone is verified. Either firewall/fix state is accepted on
    # entry so an interrupted activation converges safely on retry.
    verify_base_settings either
    require_expected_main
    verify_ruleset "$bootstrap_main_policy"
    verify_ruleset_one_of "$bootstrap_branch_firewall" "$branch_firewall"
    verify_ruleset "$tag_firewall"
    verify_effective_ruleset_set \
      "$bootstrap_main_policy" "$branch_firewall" "$tag_firewall"
    apply_ruleset "$branch_firewall"
    verify_ruleset "$branch_firewall"
    api --method PUT "repos/$target/automated-security-fixes" >/dev/null
    verify_base_settings enabled
    verify_ruleset "$bootstrap_main_policy"
    verify_effective_ruleset_set \
      "$bootstrap_main_policy" "$branch_firewall" "$tag_firewall"
    verify_no_legacy_branch_protection
    ;;
  protect)
    verify_base_settings enabled
    require_expected_main
    verify_ruleset_one_of "$bootstrap_main_policy" "$main_policy"
    verify_ruleset "$branch_firewall"
    verify_ruleset "$tag_firewall"
    verify_effective_ruleset_set \
      "$bootstrap_main_policy" "$branch_firewall" "$tag_firewall"
    require_green_contexts
    apply_ruleset "$main_policy"
    verify_ruleset "$main_policy"
    require_expected_main
    verify_effective_ruleset_set "$main_policy" "$branch_firewall" "$tag_firewall"
    verify_no_legacy_branch_protection
    ;;
  verify)
    verify_base_settings enabled
    verify_ruleset "$main_policy"
    verify_ruleset "$branch_firewall"
    verify_ruleset "$tag_firewall"
    verify_effective_ruleset_set "$main_policy" "$branch_firewall" "$tag_firewall"
    verify_no_legacy_branch_protection
    ;;
  *)
    echo "FAIL public-repository-config: unknown mode '$mode'" >&2
    exit 1
    ;;
esac

echo "OK public-repository-config $mode $target"
