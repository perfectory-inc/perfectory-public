#!/usr/bin/env bash
# Proves the guard rejects what it claims to reject: each shape of
# platforms/foundation-platform/config/data-knows-no-screen.contract.json is planted once.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd -P)/scripts/guard/data-knows-no-screen.sh"
real="$(pwd -P)"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL data-knows-no-screen-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

area=platforms/foundation-platform
fixture() {
  local name="$1" plant="$2"
  local root="$test_root/$name"
  mkdir -p "$root/$area/infra/lakehouse/spark" "$root/$area/contracts" "$root/$area/infra/lakehouse/contracts" "$root/docs"
  cp -R "$real/$area/config" "$root/$area/config"
  cp -R "$real/$area/contracts/data" "$root/$area/contracts/data"
  cp "$real/$area/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json" "$root/$area/infra/lakehouse/contracts/"
  mkdir -p "$root/$area/infra/lakehouse/spark/jobs"
  cp "$real/$area/infra/lakehouse/spark/jobs/"*.py "$root/$area/infra/lakehouse/spark/jobs/"
  cp -R "$real/docs/adr" "$root/docs/adr"
  python3 - "$root/$area" "$root" "$plant" <<'PY'
import json, sys
area, root, plant = sys.argv[1], sys.argv[2], sys.argv[3]
def edit_json(path, change):
    doc = json.load(open(path, encoding="utf-8"))
    change(doc)
    open(path, "w", encoding="utf-8").write(json.dumps(doc, ensure_ascii=False))
def append(path, text):
    open(path, "a", encoding="utf-8").write(text)
artifact = f"{area}/infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
lanes = f"{area}/config/r2-connections.contract.json"
rules = f"{area}/config/data-knows-no-screen.contract.json"
kind_job = f"{area}/infra/lakehouse/spark/jobs/by_pnu_kind_silver_to_gold.py"
if plant == "gold-label-column":
    edit_json(artifact, lambda d: d["contracts"]["gold.building_titles"]["columns"].append(
        {"name": "floor_label", "logical_type": "string", "required": False}))
elif plant == "gold-total-column":
    edit_json(artifact, lambda d: d["contracts"]["gold.parcel_land_rights"]["columns"].append(
        {"name": "land_right_total", "logical_type": "long", "required": False}))
elif plant == "kind-line-total":
    edit_json(lanes, lambda d: d["by_pnu_lanes"]["parcel-land-rights"]["columns"].append("land_right_total:long"))
elif plant == "kind-line-screen":
    edit_json(lanes, lambda d: d["by_pnu_lanes"]["parcel-zoning"].update({"read_first_by": "summary"}))
elif plant == "producer-label":
    append(kind_job, '\nLABEL = F.lit("").alias("floor_label")\n')
elif plant == "producer-display":
    append(kind_job, '\nFIELDS = ["floor_display_ko"]\n')
elif plant == "producer-join":
    append(kind_job, '\nJOINED = F.concat_ws(" · ", F.col("usages"))\n')
elif plant == "producer-page":
    append(kind_job, '\nUNIT_PAGE_BOUND = 200\n')
elif plant == "contract-first-paint":
    edit_json(lanes, lambda d: d.update({"first_paint_p95_ms": 1400}))
elif plant == "adr-first-screen-row":
    append(f"{root}/docs/adr/0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md",
           "\n| 첫 화면 크기 | 세 요청 합 gzip p95 ≤ 2 KB | 측정 |\n")
elif plant == "stale-exception":
    edit_json(rules, lambda d: d["exceptions"].append(
        {"where": "gold.parcel_panel", "match": "a_column_that_is_not_there", "until": "x", "reason": "x"}))
elif plant == "exception-without-until":
    edit_json(rules, lambda d: d["exceptions"][0].pop("until"))
else:
    raise SystemExit(f"unknown plant {plant}")
PY
  printf '%s' "$root"
}

expect_rejected() {
  local output
  if output="$(bash "$checker" "$1" 2>&1)"; then
    echo "FAIL data-knows-no-screen-self-test: accepted a planted violation ($2)" >&2
    exit 1
  fi
  # Refused for the planted shape, not for something the fixture broke.
  case "$output" in
    *"$3"*) ;;
    *) echo "FAIL data-knows-no-screen-self-test: $2 was refused without naming $3: $output" >&2; exit 1 ;;
  esac
}

bash "$checker" "$real" >/dev/null || {
  echo "FAIL data-knows-no-screen-self-test: rejected the real tree" >&2
  exit 1
}

while read -r plant shape; do
  expect_rejected "$(fixture "$plant" "$plant")" "$plant" "$shape"
done <<'PLANTS'
gold-label-column display-text-name
gold-total-column page-count-name
kind-line-total page-count-name
kind-line-screen kind-line-names-a-screen
producer-label display-text-name
producer-display display-text-name
producer-join joined-display-text
producer-page page-bound
contract-first-paint screen-concept
adr-first-screen-row screen-concept
stale-exception matches nothing any more
exception-without-until needs a reason and an until
PLANTS

echo "OK data-knows-no-screen-self-test"
