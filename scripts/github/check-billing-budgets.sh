#!/usr/bin/env bash
# Read-only proof that both Actions compute and cache storage hard-stop at USD 0.
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <billing-budget-policy.json>" >&2
  exit 2
fi
policy="$1"
api_version="2026-03-10"
[ -f "$policy" ] || {
  echo "FAIL billing-budgets: policy is missing" >&2
  exit 1
}
if [ -n "${GH_HOST:-}" ] && [ "$GH_HOST" != github.com ]; then
  echo "FAIL billing-budgets: GH_HOST must be unset or github.com" >&2
  exit 1
fi
for command_name in gh mktemp python3; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL billing-budgets: missing command '$command_name'" >&2
    exit 1
  }
done

owner="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["owner"])' "$policy")"
if [ "$owner" != perfectory-inc ]; then
  echo "FAIL billing-budgets: refusing unexpected owner '$owner'" >&2
  exit 1
fi

actual="$(mktemp)"
cleanup() {
  rm -f -- "$actual"
}
trap cleanup EXIT
env -u GH_HOST gh api --hostname github.com \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: $api_version" \
  "organizations/$owner/settings/billing/budgets?per_page=100" >"$actual"

python3 - "$policy" "$actual" <<'PY'
import json
import sys

policy_path, actual_path = sys.argv[1:]
with open(policy_path, encoding="utf-8") as handle:
    policy = json.load(handle)
with open(actual_path, encoding="utf-8") as handle:
    response = json.load(handle)

if set(response) != {"budgets", "has_next_page", "total_count"}:
    raise SystemExit("FAIL billing-budgets: response shape drifted")
budgets = response["budgets"]
if not isinstance(budgets, list) \
        or response["has_next_page"] is not False \
        or type(response["total_count"]) is not int \
        or response["total_count"] != len(budgets):
    raise SystemExit("FAIL billing-budgets: incomplete or paginated budget inventory")

required = policy.get("required_budgets")
if not isinstance(required, list) or len(required) != 2:
    raise SystemExit("FAIL billing-budgets: policy must define exactly two budgets")
required_budget_keys = {
    "budget_type",
    "budget_product_sku",
    "budget_scope",
    "budget_entity_name",
    "budget_amount",
    "prevent_further_usage",
}
if any(set(item) != required_budget_keys for item in required):
    raise SystemExit("FAIL billing-budgets: required budget schema drifted")
required_skus = {"actions", "actions_cache_storage"}
if {item.get("budget_product_sku") for item in required} != required_skus:
    raise SystemExit("FAIL billing-budgets: required SKU set drifted")
required_fields = {
    "budget_type",
    "budget_product_sku",
    "budget_scope",
    "budget_entity_name",
    "budget_amount",
    "prevent_further_usage",
}

for expected in required:
    sku = expected["budget_product_sku"]
    if set(expected) != required_fields:
        raise SystemExit(
            f"FAIL billing-budgets: {sku} policy fields must be {sorted(required_fields)!r}"
        )
    matches = [item for item in budgets if item.get("budget_product_sku") == sku]
    if len(matches) != 1:
        raise SystemExit(
            f"FAIL billing-budgets: expected exactly one unambiguous {sku} budget, found {len(matches)}"
        )
    actual = matches[0]
    for key, value in expected.items():
        if actual.get(key) != value or type(actual.get(key)) is not type(value):
            raise SystemExit(
                f"FAIL billing-budgets: {sku}.{key} must be {value!r}, found {actual.get(key)!r}"
            )

print("OK billing-budgets actions=USD0/hard-stop actions_cache_storage=USD0/hard-stop")
PY
