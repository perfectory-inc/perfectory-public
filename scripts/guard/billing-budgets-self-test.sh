#!/usr/bin/env bash
# Proves missing, duplicate, paginated, and non-zero budgets fail closed.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/github/check-billing-budgets.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$test_root" ] || rm -rf -- "$test_root"
      ;;
    *) echo "billing-budgets-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/gh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "${FAKE_BUDGET_STATE:-valid}" in
  valid)
    cat <<'JSON'
{"budgets":[{"budget_type":"ProductPricing","budget_product_sku":"actions","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true},{"budget_type":"SkuPricing","budget_product_sku":"actions_cache_storage","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true}],"has_next_page":false,"total_count":2}
JSON
    ;;
  missing-cache)
    cat <<'JSON'
{"budgets":[{"budget_type":"ProductPricing","budget_product_sku":"actions","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true}],"has_next_page":false,"total_count":1}
JSON
    ;;
  duplicate)
    cat <<'JSON'
{"budgets":[{"budget_type":"ProductPricing","budget_product_sku":"actions","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true},{"budget_type":"ProductPricing","budget_product_sku":"actions","budget_scope":"repository","budget_entity_name":"perfectory-public","budget_amount":0,"prevent_further_usage":true},{"budget_type":"SkuPricing","budget_product_sku":"actions_cache_storage","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true}],"has_next_page":false,"total_count":3}
JSON
    ;;
  paginated)
    printf '{"budgets":[],"has_next_page":true,"total_count":101}\n'
    ;;
  nonzero)
    cat <<'JSON'
{"budgets":[{"budget_type":"ProductPricing","budget_product_sku":"actions","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":1,"prevent_further_usage":true},{"budget_type":"SkuPricing","budget_product_sku":"actions_cache_storage","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true}],"has_next_page":false,"total_count":2}
JSON
    ;;
  no-stop)
    cat <<'JSON'
{"budgets":[{"budget_type":"ProductPricing","budget_product_sku":"actions","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":true},{"budget_type":"SkuPricing","budget_product_sku":"actions_cache_storage","budget_scope":"organization","budget_entity_name":"perfectory-inc","budget_amount":0,"prevent_further_usage":false}],"has_next_page":false,"total_count":2}
JSON
    ;;
  *) exit 90 ;;
esac
SH
chmod +x "$fake_bin/gh"

run_checker() {
  PATH="$fake_bin:$PATH" "$checker" tools/github/billing-budget-policy.json
}
run_checker >/dev/null
for invalid in missing-cache duplicate paginated nonzero no-stop; do
  if FAKE_BUDGET_STATE="$invalid" run_checker >/dev/null 2>&1; then
    echo "FAIL billing-budgets-self-test: accepted $invalid budget state" >&2
    exit 1
  fi
done

echo "OK billing-budgets-self-test"
