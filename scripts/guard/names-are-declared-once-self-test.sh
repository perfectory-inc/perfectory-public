#!/usr/bin/env bash
# Proves names-are-declared-once.sh (root ADR-0626 §7 G4) passes declared, renamed, companion, prefix,
# marked, test and migration literals, and refuses an undeclared table name in production code.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"

guard="$(cd "$(dirname "$0")" && pwd)/names-are-declared-once.sh"
fixture_root
test_root="$FIXTURE_ROOT"
trap 'rm -rf -- "$test_root"' EXIT

fail() {
  echo "FAIL names-are-declared-once-self-test: $*" >&2
  exit 1
}

# A fixture tree in $1 with one production file holding the literal $2.
build() {
  local repo="$1" literal="$2"
  mkdir -p "$repo/platforms/foundation-platform/config" \
    "$repo/platforms/foundation-platform/infra/lakehouse/contracts" \
    "$repo/platforms/foundation-platform/src" "$repo/platforms/foundation-platform/tests" \
    "$repo/platforms/foundation-platform/migrations"
  fixture_git "$repo" init -q --initial-branch=main
  cat >"$repo/platforms/foundation-platform/config/naming.contract.json" <<'JSON'
{"role_suffixes": ["changes"],
 "renames": {"tables": {"silver.synthetic_rows": "silver.synthetic_row"}, "not_recreated": ["gold.synthetic_panel"]}}
JSON
  cat >"$repo/platforms/foundation-platform/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json" <<'JSON'
{"contracts": {"gold.synthetic_lane": {}, "silver.land_synthetic": {}}}
JSON
  printf 'const TABLE: &str = "%s";\n' "$literal" >"$repo/platforms/foundation-platform/src/lane.rs"
  # Never read: a test's own names, an append-only migration, a comment.
  printf 'NAME = "silver.only_in_a_test"\n' >"$repo/platforms/foundation-platform/tests/test_lane.py"
  printf "SELECT 1 FROM silver.only_in_history; -- 'silver.only_in_history'\n" \
    >"$repo/platforms/foundation-platform/migrations/20260101000000_x.sql"
  printf '// "silver.only_in_a_comment"\n' >>"$repo/platforms/foundation-platform/src/lane.rs"
  fixture_git "$repo" add -A
}

expect() {
  local verdict="$1" literal="$2" repo
  repo="$test_root/${literal//[^a-z0-9]/_}"
  build "$repo" "$literal"
  if bash "$guard" "$repo" >/dev/null 2>"$repo.err"; then
    [ "$verdict" = pass ] || fail "accepted $literal"
  else
    local refusal
    refusal="$(cat "$repo.err")"
    [ "$verdict" = fail ] || fail "refused $literal: $refusal"
    grep -q "names-are-declared-once: .*$literal is no table" "$repo.err" || fail "refused $literal without naming it"
  fi
}

expect pass "gold.synthetic_lane"
expect pass "silver.synthetic_rows"
expect pass "silver.synthetic_row"
expect pass "gold.synthetic_panel"
expect pass "gold.synthetic_lane__changes"
expect pass "silver.land_"
expect pass "silver.land"
expect pass "silver.data.json"
expect fail "silver.undeclared_table"
expect fail "reference.undeclared_code"
expect fail "gold.synthetic_lane__history"

# A marked literal passes; the marker covers its block, not the next one.
repo="$test_root/marked"
build "$repo" "gold.undeclared_resource"
{
  printf '\n// names-are-declared-once: an authorization resource, not a table.\n'
  printf 'const RESOURCE: (&str, &str) = (\n    "gold.undeclared_resource",\n    "read",\n);\n'
} >"$repo/platforms/foundation-platform/src/other.rs"
printf 'const TABLE: &str = "gold.synthetic_lane";\n' >"$repo/platforms/foundation-platform/src/lane.rs"
fixture_git "$repo" add -A
bash "$guard" "$repo" >/dev/null 2>&1 || fail "refused a marked literal"
printf '\nconst NEXT: &str = "gold.undeclared_resource";\n' >>"$repo/platforms/foundation-platform/src/other.rs"
if bash "$guard" "$repo" >/dev/null 2>&1; then fail "a marker covered the next block"; fi

echo "names-are-declared-once-self-test: ok"
