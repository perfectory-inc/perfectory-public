#!/usr/bin/env bash
# Proves parcel-reads-the-served-edition.sh refuses what it claims to (root ADR-0291 §5): the
# current rows of silver.parcel_boundaries taken whole, named literally or through a module name, as
# address_pnu and the 공매 producer did before 2026-10-21; and that it keeps the served reader, a
# reader of another table and a comment.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/parcel-reads-the-served-edition.sh"
jobs="platforms/foundation-platform/infra/lakehouse/spark/jobs"
test_root="$(mktemp -d)"
if command -v cygpath >/dev/null 2>&1; then test_root="$(cygpath -m "$test_root")"; fi
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL parcel-reads-the-served-edition-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

fixture() {
  local name="$1" body="$2"
  mkdir -p "$test_root/$name/$jobs"
  printf '%s\n' "$body" >"$test_root/$name/$jobs/job.py"
}
expect() {
  local verdict="$1" name="$2"
  if bash "$checker" "$test_root/$name" >/dev/null 2>&1; then got=allowed; else got=refused; fi
  [ "$got" = "$verdict" ] || {
    echo "FAIL parcel-reads-the-served-edition-self-test: $name was $got, expected $verdict" >&2
    exit 1
  }
}

fixture literal '
parcels = current_rows(spark.table(f"`{catalog}`.`silver`.`parcel_boundaries`"),
                       load_lakehouse_contract("silver.parcel_boundaries")).select("pnu")'
expect refused literal

fixture named '
PARCEL_SOURCE = "silver.parcel_boundaries"
def read(frames):
    return pc.current_rows_of_versioned(frames[PARCEL_SOURCE], PARCEL_SOURCE)'
expect refused named

fixture served '
PARCEL_SOURCE = "silver.parcel_boundaries"
parcels = served_rows(spark.table("silver.parcel_boundaries")).select("pnu")
titles = current_rows(spark.table("silver.building_register_titles"), contract)
# current_rows(spark.table("silver.parcel_boundaries")) was the 공매 bug
'
expect allowed served

# The real tree passes: the canonical reader is the only one.
bash "$checker" >/dev/null

echo "OK parcel-reads-the-served-edition-self-test"
