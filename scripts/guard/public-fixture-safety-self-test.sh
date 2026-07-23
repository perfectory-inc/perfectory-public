#!/usr/bin/env bash
# Negative fixtures prove that public-fixture-safety rejects the whole class,
# rather than only today's known sample values.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
checker="$root/scripts/guard/public-fixture-safety.sh"
[ -f "$checker" ] || {
  echo "FAIL public-fixture-safety-self-test: missing $checker" >&2
  exit 1
}

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

git -C "$test_root" init -q
git -C "$test_root" config user.name fixture-policy-test
git -C "$test_root" config user.email fixture-policy-test@example.invalid
mkdir -p "$test_root/platforms/foundation-platform/infra/lakehouse/spark/fixtures/bronze"
fixture="$test_root/platforms/foundation-platform/infra/lakehouse/spark/fixtures/bronze/sample.jsonl"
pretty_fixture="$test_root/platforms/foundation-platform/infra/lakehouse/spark/fixtures/bronze/sample.json"
unknown_fixture="$test_root/products/example/tests/fixtures/real.json"
load_registry="$test_root/products/gongzzang/tests/load/scenarios.v1.json"

cat >"$fixture" <<'JSON'
{"pnu":"9999900101100010000","sido_code":"99","sigungu_code":"99999","primary_bjdong_code":"9999900101","anchor_lng":127.123450,"anchor_lat":36.123450,"source_record_id":"synthetic-source-record-1","source_snapshot_id":"synthetic-source-snapshot-1"}
JSON
cat >"$test_root/loopback.rs" <<'RS'
const LOOPBACK: &str = "http://127.0.0.1:8080";
RS
mkdir -p "$test_root/platforms/identity-platform/tools/example/tests/fixtures"
cat >"$test_root/platforms/identity-platform/tools/example/tests/fixtures/config.json" <<'JSON'
{"fixture_provenance":"synthetic","schema_version":"example.v1","enabled":true}
JSON
mkdir -p "$(dirname "$load_registry")"
cat >"$load_registry" <<'JSON'
{
  "schemaVersion": "gongzzang.load.scenarios.v1",
  "defaultTargetBaseUrl": "https://load-target.example.invalid",
  "capacityBinding": "synthetic-public-safety-ceiling",
  "scenarios": [{"id":"smoke","file":"tests/load/scenarios/smoke.js","maxSafeRps":1}]
}
JSON
git -C "$test_root" add .
bash "$checker" "$test_root" >/dev/null

assert_rejected() {
  local label="$1"
  local expected="$2"
  local output status
  set +e
  output="$(bash "$checker" "$test_root" 2>&1)"
  status=$?
  set -e
  if [ "$status" -eq 0 ] || ! printf '%s\n' "$output" | grep -Fq "$expected"; then
    echo "FAIL public-fixture-safety-self-test: $label was not rejected as expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
}

cat >"$test_root/assignable.rs" <<'RS'
const PNU: &str = "1168010301100010000";
RS
git -C "$test_root" add assignable.rs
assert_rejected assignable-pnu "repository-reserved 99999 PNU range"
git -C "$test_root" rm -q -f assignable.rs

cat >"$test_root/coordinate.rs" <<'RS'
const POINT: (f64, f64) = (126.978, 37.5665);
RS
git -C "$test_root" add coordinate.rs
assert_rejected live-coordinate "reserved synthetic coordinate namespace"
git -C "$test_root" rm -q -f coordinate.rs

cat >"$pretty_fixture" <<'JSON'
{
  "complex_name": "Jeju Industrial Complex",
  "address_text": "Jeju City",
  "source_record_id": "synthetic-source-record-2",
  "source_snapshot_id": "synthetic-source-snapshot-2"
}
JSON
git -C "$test_root" add "$pretty_fixture"
assert_rejected unseen-place-pretty-json "synthetic namespace"
git -C "$test_root" rm -q -f "$pretty_fixture"

mkdir -p "$(dirname "$unknown_fixture")"
cat >"$unknown_fixture" <<'JSON'
{"complex_name":"Pangyo Industrial Complex","address_text":"Pangyo"}
JSON
git -C "$test_root" add "$unknown_fixture"
assert_rejected unknown-fixture-path "fixture has no declared synthetic namespace marker"
git -C "$test_root" rm -q -f "$unknown_fixture"

cat >"$fixture" <<'JSON'
{ "pnu": "9999900101100010000", "sido_code": "11", "sigungu_code": "11680", "primary_bjdong_code": "1168010300", "source_record_id": "provider-record-20260101", "source_snapshot_id": "provider-snapshot-20260101" }
JSON
git -C "$test_root" add "$fixture"
assert_rejected provider-fixture "reserved synthetic namespace"

cat >"$fixture" <<'JSON'
{"pnu":"9999900101100010000","sido_code":"99","sigungu_code":"99999","primary_bjdong_code":"9999900101","anchor_lng":127.123450,"anchor_lat":36.123450,"source_record_id":"synthetic-source-record-1","source_snapshot_id":"synthetic-source-snapshot-1"}
JSON
mkdir -p "$test_root/platforms/foundation-platform/infra/lakehouse/dbt/smoke"
cat >"$test_root/platforms/foundation-platform/infra/lakehouse/dbt/smoke/source-fixtures.sql" <<'SQL'
INSERT INTO synthetic_fixture VALUES ('SYNTHETIC-ROW-1');
SQL
git -C "$test_root" add .
assert_rejected unmarked-sql-fixture "synthetic-fixture provenance marker"

git -C "$test_root" rm -q -f \
  platforms/foundation-platform/infra/lakehouse/dbt/smoke/source-fixtures.sql
cat >"$load_registry" <<'JSON'
{
  "schemaVersion": "gongzzang.load.scenarios.v1",
  "defaultTargetBaseUrl": "https://capacity.private.internal",
  "capacityBinding": "measured",
  "scenarios": [{"id":"stress","file":"tests/load/scenarios/stress.js","maxSafeRps":731}]
}
JSON
git -C "$test_root" add "$load_registry"
assert_rejected private-load-binding "private load-test binding"

echo "OK public-fixture-safety-self-test"
