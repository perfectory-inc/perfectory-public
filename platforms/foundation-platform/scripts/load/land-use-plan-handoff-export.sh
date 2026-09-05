#!/usr/bin/env bash
# Convert the collected D155 land-use-plan CSV ZIPs into Silver handoff JSONL (root ADR-0083).
#
# **Which objects.** `bronze/source=vworldkr__land_use_plan/` mixes two datasets (34 boundary
# shapefiles, 207 attribute CSVs) and piles up monthly vintages of the same 17 provinces. The
# set is not decided here; it is read from `vworld-land-use-plan-source-objects.json`, which
# records what was measured from each ZIP's directory. This script selects exactly the
# contract's `selected_vintage` and refuses to run unless that selection is one object per
# each of the 17 provinces — 16 of 17 looks complete and is how a province goes silently
# missing (ADR-0082's subset-refusal, applied to vintages).
#
# **What it keeps.** Nothing. Whether an object is already converted is answered by asking R2
# whether its handoff exists (root ADR-0062). The tally is counted from per-run summary files,
# never from log lines (a level chosen for noise cannot silence a field).
#
# Verbs: plan (show) | run (convert) | zone-code (convert the LMIS code table, one object).
# Second argument picks the 17-province lane: plan (default, D155) | price (D151 official
# land price, root ADR-0085). Both are the same loop over the same contract shape — only the
# contract file, the exporter command, and its env prefix differ.
set -uo pipefail

MODE="${1:-plan}"
LANE="${2:-plan}"
RELEASE="${FOUNDATION_PLATFORM_RELEASE_DIR:-/opt/foundation-platform/current}"
STATE="${LAND_USE_PLAN_EXPORT_STATE_DIR:-$HOME/land-use-export-state}"
JOBS="${LAND_USE_PLAN_EXPORT_JOBS:-4}"
case "$LANE" in
  plan)
    ENV_PREFIX="FOUNDATION_PLATFORM_LAND_USE_PLAN"
    EXPORT_CMD="export-land-use-plan-silver-handoff"
    SNAPSHOT_LABEL="vworldkr-land-use-plan"
    DEFAULT_CONTRACT="vworld-land-use-plan-source-objects.json"
    ;;
  price)
    ENV_PREFIX="FOUNDATION_PLATFORM_LAND_INDIVIDUAL_PRICE"
    EXPORT_CMD="export-land-individual-price-silver-handoff"
    SNAPSHOT_LABEL="vworldkr-land-individual-price"
    DEFAULT_CONTRACT="vworld-land-individual-price-source-objects.json"
    ;;
  *)
    echo "모르는 레인이다: $LANE (plan | price)" >&2
    exit 1
    ;;
esac
CONTRACT="${LAND_USE_PLAN_SOURCE_CONTRACT:-$RELEASE/infra/lakehouse/contracts/$DEFAULT_CONTRACT}"
PUBLISHER="${FOUNDATION_PLATFORM_PUBLISHER_BIN:-$RELEASE/bin/foundation-outbox-publisher}"
ZONE_CODE_CONTRACT="${LAND_USE_ZONE_CODE_SOURCE_CONTRACT:-$RELEASE/infra/lakehouse/contracts/vworld-land-use-zone-code-source-objects.json}"

# This command WRITES to R2; reader-only credentials pass a presence check and then fail on
# every object (measured on the parcel lane). What is required is what the exporter's
# R2ObjectStorageConfig::from_env demands.
for v in FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY; do
  if [ -z "${!v:-}" ]; then
    echo "필수 환경변수가 비어 있다: $v" >&2
    exit 1
  fi
done
if [ -z "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT:-}" ] \
   && [ -z "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID:-}" ]; then
  echo "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT 이나 ..._ACCOUNT_ID 중 하나는 있어야 한다" >&2
  exit 1
fi

[ -f "$CONTRACT" ] || { echo "원천 목록이 없다: $CONTRACT" >&2; exit 1; }
[ -x "$PUBLISHER" ] || { echo "실행 파일이 없다: $PUBLISHER" >&2; exit 1; }
mkdir -p "$STATE"

# The vintage, the prefix, and the suffix are the contract's. Spelled here they would drift
# from the loader that derives the same names from the same file.
read -r VINTAGE HANDOFF_PREFIX SUFFIX < <(python3 -c "
import json, sys
c = json.load(open(sys.argv[1], encoding='utf-8'))
if c['schema_version'] != 1:
    sys.exit('source object contract schema_version %r is not the 1 this script reads' % c['schema_version'])
print(c['selected_vintage'], c['handoff_prefix'], c['handoff_suffix'])
" "$CONTRACT") || { echo "계약을 못 읽었다" >&2; exit 1; }
SOURCE_SNAPSHOT_ID="$SNAPSHOT_LABEL:$VINTAGE"

if [ "$MODE" = "zone-code" ]; then
  [ -f "$ZONE_CODE_CONTRACT" ] || { echo "코드표 원천 목록이 없다: $ZONE_CODE_CONTRACT" >&2; exit 1; }
  read -r ZC_KEY ZC_PREFIX ZC_SUFFIX < <(python3 -c "
import json, sys
c = json.load(open(sys.argv[1], encoding='utf-8'))
if c['schema_version'] != 1:
    sys.exit('source object contract schema_version %r is not the 1 this script reads' % c['schema_version'])
[o] = c['objects']
print(o['object_key'], c['handoff_prefix'], c['handoff_suffix'])
" "$ZONE_CODE_CONTRACT") || { echo "코드표 계약을 못 읽었다" >&2; exit 1; }
  RUN_DIR="$STATE/zone-code-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$RUN_DIR"
  base="$(basename "$ZC_KEY" .zip)"
  FOUNDATION_PLATFORM_LAND_USE_ZONE_CODE_INPUT_OBJECT_KEY="$ZC_KEY" \
  FOUNDATION_PLATFORM_LAND_USE_ZONE_CODE_OUTPUT_OBJECT_KEY="$ZC_PREFIX/$base$ZC_SUFFIX" \
  FOUNDATION_PLATFORM_LAND_USE_ZONE_CODE_SOURCE_SNAPSHOT_ID="vworldkr-land-use-zone-code:$base" \
  FOUNDATION_PLATFORM_LAND_USE_ZONE_CODE_SUMMARY_PATH="$RUN_DIR/$base.summary.json" \
    "$PUBLISHER" export-land-use-zone-code-silver-handoff
  rc=$?
  [ "$rc" -eq 0 ] && echo "완료 zone-code $base → $ZC_PREFIX/$base$ZC_SUFFIX"
  exit "$rc"
fi

# Exactly the selected vintage, exactly 17 provinces, each exactly once.
mapfile -t objects < <(python3 -c "
import json, sys
c = json.load(open('$CONTRACT'))
picked = [o for o in c['objects'] if o['vintage'] == c['selected_vintage']]
regions = sorted(o['region_code'] for o in picked)
if len(regions) != 17 or len(set(regions)) != 17:
    sys.exit('selected vintage %s covers %d provinces, not the 17 a national load needs: %s'
             % (c['selected_vintage'], len(set(regions)), regions))
for o in picked:
    print(o['object_key'])
") || { echo "선택된 빈티지가 전국을 덮지 못한다" >&2; exit 1; }

total=${#objects[@]}
echo "원천 $total 개 · 빈티지 $VINTAGE · 방식 $MODE · 핸드오프 접두사 $HANDOFF_PREFIX"

if [ "$MODE" != "run" ]; then
  for key in "${objects[@]}"; do
    base="$(basename "$key" .zip)"
    echo "변환예정 $base  →  $HANDOFF_PREFIX/$base$SUFFIX"
  done
  echo "끝: 계획 $total 개 · 동시 $JOBS 개로 돌 예정"
  exit 0
fi

RUN_DIR="$STATE/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR"

convert_one() {
  local key="$1"
  local base; base="$(basename "$key" .zip)"
  local log="$RUN_DIR/$base.log"
  local t0; t0=$(date +%s)

  # Lineage is derived, not passed: the command records the key it opened (root ADR-0068).
  # Whether this object is already converted is answered by R2 inside the command.
  env "${ENV_PREFIX}_INPUT_OBJECT_KEY=$key" \
      "${ENV_PREFIX}_OUTPUT_OBJECT_KEY=$HANDOFF_PREFIX/$base$SUFFIX" \
      "${ENV_PREFIX}_SOURCE_SNAPSHOT_ID=$SOURCE_SNAPSHOT_ID" \
      "${ENV_PREFIX}_SUMMARY_PATH=$RUN_DIR/$base.summary.json" \
    "$PUBLISHER" "$EXPORT_CMD" > "$log" 2>&1
  local rc=$?
  local el=$(( $(date +%s) - t0 ))

  if [ "$rc" -ne 0 ]; then
    echo "실패    $base  rc=$rc  ${el}초" >&2
    tail -6 "$log" | cut -c1-240 >&2
    return 1
  fi
  echo "완료    $base  ${el}초"
}
export -f convert_one
export RUN_DIR PUBLISHER HANDOFF_PREFIX SUFFIX SOURCE_SNAPSHOT_ID ENV_PREFIX EXPORT_CMD

echo "원천 $total 개 · 동시 $JOBS 개 · 증거 $RUN_DIR"
started=$(date +%s)
printf '%s\n' "${objects[@]}" | xargs -P "$JOBS" -I{} bash -c 'convert_one "$@"' _ {}
el=$(( $(date +%s) - started ))

# The tally reads summaries, never log lines (the parcel run that skipped 255 objects once
# reported converting 255 because RUST_LOG=warn silenced both outcomes).
python3 - "$RUN_DIR" "$total" "$el" <<'PY'
import json, pathlib, sys

run_dir, total, elapsed = pathlib.Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
counts = {}
rejected = 0
for path in sorted(run_dir.glob("*.summary.json")):
    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
        outcome = summary["outcome"]
        rejected += int(summary.get("rejected_row_count") or 0)
    except (OSError, ValueError, KeyError) as error:
        print(f"요약을 못 읽었다 {path.name}: {error}", file=sys.stderr)
        outcome = "읽지못함"
    counts[outcome] = counts.get(outcome, 0) + 1

converted = counts.get("converted", 0)
skipped = counts.get("already_present", 0)
accounted = converted + skipped
unexplained = {name: n for name, n in counts.items()
               if name not in ("converted", "already_present")}
print(f"끝: 변환 {converted} · 건너뜀 {skipped} · 설명없음 {total - accounted} · 거부행 {rejected} · {elapsed // 60}분")
for name, n in sorted(unexplained.items()):
    print(f"  요약이 있으나 결과를 알 수 없음: {name} {n}개", file=sys.stderr)
sys.exit(1 if accounted != total else 0)
PY
