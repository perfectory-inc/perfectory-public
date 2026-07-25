#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/foundation-parcel-current-selector.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL foundation-parcel-current-selector-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

fixture_named() {
  local root="$1"
  local name="$2"
  local body="$3"
  mkdir -p "$root/platforms/foundation-platform/infra/lakehouse/spark/jobs"
  printf '%s\n' "$body" \
    >"$root/platforms/foundation-platform/infra/lakehouse/spark/jobs/$name"
}
fixture() {
  fixture_named "$1" "parcel_boundaries_job.py" "$2"
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL foundation-parcel-current-selector-self-test: rejected allowed fixture" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL foundation-parcel-current-selector-self-test: accepted forbidden fixture" >&2
    exit 1
  fi
}

allowed="$test_root/allowed"
fixture "$allowed" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
predicate = current_row_predicate(TABLE_CONTRACT)
current = frame.where(F.expr(predicate))
schema_only = frame.select(F.col("valid_to_utc"))
unrelated = frame.where(F.col("geometry_wkb").isNull())
'
expect_allowed "$allowed"

unrelated_sql_null="$test_root/unrelated-sql-null"
fixture "$unrelated_sql_null" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
schema_only = F.col("valid_to_utc"); unrelated = "geometry_wkb IS NULL"
'
expect_allowed "$unrelated_sql_null"

comment_only="$test_root/comment-only"
fixture "$comment_only" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
# current = frame.where(F.col("valid_to_utc").isNull())
'
expect_allowed "$comment_only"

literal="$test_root/literal"
fixture "$literal" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
current = spark.sql("SELECT * FROM parcels WHERE valid_to_utc IS NULL")
'
expect_rejected "$literal"

method="$test_root/method"
fixture "$method" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
current = frame.where(
    F.col("valid_to_utc").isNull()
)
'
expect_rejected "$method"

bracket_method="$test_root/bracket-method"
fixture "$bracket_method" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
current = frame["valid_to_utc"].isNull()
'
expect_rejected "$bracket_method"

attribute_method="$test_root/attribute-method"
fixture "$attribute_method" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
current = frame.valid_to_utc.isNull()
'
expect_rejected "$attribute_method"

backtick_sql="$test_root/backtick-sql"
fixture_named "$backtick_sql" "parcel_boundaries_job.sql" '
SELECT * FROM parcels WHERE `valid_to_utc` IS NULL
'
expect_rejected "$backtick_sql"

known_consumer="$test_root/known-consumer"
fixture_named "$known_consumer" "spatial_tile_publication_wap.py" '
current = frame.where(F.col("valid_to_utc").isNull())
'
expect_rejected "$known_consumer"

artifact="$test_root/artifact"
fixture "$artifact" '
LOGICAL_CONTRACT = "silver.parcel_boundaries"
current = TABLE_CONTRACT["current_row_predicate"]
'
expect_rejected "$artifact"

echo "OK foundation-parcel-current-selector-self-test"
