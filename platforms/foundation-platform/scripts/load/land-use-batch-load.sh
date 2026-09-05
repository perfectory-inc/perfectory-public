#!/usr/bin/env bash
# Load land-use Silver handoffs into their Iceberg tables, then leave the table healthy
# (root ADR-0083; the shape is lakehouse-batch-load.sh, the scalar job instead of the
# geometry one).
#
# **This script keeps no memory of what it loaded.** The table does (root ADR-0062): the job
# reads the snapshot summaries and skips batches already appended. Every batch is offered on
# every run.
#
# **Which handoffs.** Derived from `vworld-land-use-plan-source-objects.json` — the same file
# the exporter read — never by scanning the prefix, so a half-converted state cannot read as
# "this is all of it". The zone-code verb loads the single LMIS code-table handoff.
#
# Verbs: validate [table] | load [table] | zone-code-load. Default table: land_use_plan.
set -uo pipefail

MODE="${1:-validate}"
TABLE="${2:-land_use_plan}"

STATE="${LAND_USE_LOAD_STATE_DIR:-$HOME/land-use-load-state}"
WORK_ROOT="${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-$HOME/lakehouse-state}"
IVY_CACHE="${FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE:-$HOME/lakehouse-ivy}"
RELEASE="${FOUNDATION_PLATFORM_RELEASE_DIR:-/opt/foundation-platform/current}"
CONTRACT_FILE="${LAND_USE_PLAN_SOURCE_CONTRACT:-$RELEASE/infra/lakehouse/contracts/vworld-land-use-plan-source-objects.json}"
ZONE_CODE_CONTRACT="${LAND_USE_ZONE_CODE_SOURCE_CONTRACT:-$RELEASE/infra/lakehouse/contracts/vworld-land-use-zone-code-source-objects.json}"
FILES_PER_BATCH="${FILES_PER_BATCH:-4}"
DRIVER_MEM="${DRIVER_MEM:-24g}"
TASKS="${TASKS:-8}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-foundation-platform-compute}"

for v in FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
         FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
         FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY; do
  if [ -z "${!v:-}" ]; then
    echo "필수 환경변수가 비어 있다: $v" >&2
    exit 1
  fi
done
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER="${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER:-r2_data_catalog}"
export FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="$WORK_ROOT"
export FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="$IVY_CACHE"

mkdir -p "$STATE" "$WORK_ROOT" "$IVY_CACHE"
chmod 777 "$WORK_ROOT" "$IVY_CACHE" 2>/dev/null || true
cd "$RELEASE" || { echo "릴리스 디렉터리 없음: $RELEASE" >&2; exit 1; }

# 판은 계약이 정한다 (root ADR-0065). 'iceberg' 블록만 읽으면 s3a 없이 떠서 제 입력을 못 연다.
PACKAGES=$(python3 -c "
import json, sys
c = json.load(open('$RELEASE/infra/lakehouse/contracts/lakehouse-engine.contract.json'))
if c['schema_version'] != 2:
    sys.exit('engine contract schema_version %r is not the 2 this script reads' % c['schema_version'])
print(','.join(
    a + ':' + b['version']
    for b in (c['iceberg'], c['hadoop'])
    for a in b['artifacts']))") || { echo "패키지 목록을 못 만들었다" >&2; exit 1; }

submit() {
  docker compose -p "$COMPOSE_PROJECT" -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
    -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID \
    -e FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY \
    spark \
    spark-submit --master "local[$TASKS]" --driver-memory "$DRIVER_MEM" \
    --packages "$PACKAGES" --conf spark.jars.ivy=/home/spark/.ivy2 "$@"
}

if [ "$MODE" = "zone-code-load" ]; then
  read -r base ZC_PREFIX ZC_SUFFIX < <(python3 -c "
import json, os, sys
c = json.load(open(sys.argv[1], encoding='utf-8'))
if c['schema_version'] != 1:
    sys.exit('source object contract schema_version %r is not the 1 this script reads' % c['schema_version'])
[o] = c['objects']
print(os.path.basename(o['object_key'])[:-4], c['handoff_prefix'], c['handoff_suffix'])
" "$ZONE_CODE_CONTRACT") || { echo "코드표 계약을 못 읽었다" >&2; exit 1; }
  input="s3a://$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET/$ZC_PREFIX/$base$ZC_SUFFIX"
  log="$STATE/zone-code-load.log"
  submit /workspace/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py \
    --contract silver.land_use_zone_code --input "$input" \
    --write-mode iceberg --iceberg-write-mode append > "$log" 2>&1
  rc=$?
  grep -aoE "silver-scalar-handoff-[a-z-]+( rows=[0-9]+)?[^\"]*" "$log" | tail -1
  [ "$rc" -ne 0 ] && tail -6 "$log" | cut -c1-240 >&2
  exit "$rc"
fi

# 키는 R2 를 훑어서 얻지 않는다. 변환기와 같은 목록 파일에서 같은 규칙으로 만든다.
mapfile -t all < <(python3 -c "
import json, sys
c = json.load(open('$CONTRACT_FILE'))
if c['schema_version'] != 1:
    sys.exit('source object contract schema_version %r is not the 1 this script reads' % c['schema_version'])
import os
picked = [o for o in c['objects'] if o['vintage'] == c['selected_vintage']]
regions = sorted(o['region_code'] for o in picked)
if len(set(regions)) != 17:
    sys.exit('selected vintage %s covers %d provinces, not 17' % (c['selected_vintage'], len(set(regions))))
for o in picked:
    base = os.path.basename(o['object_key'])[:-4]
    print('s3a://' + '$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET' + '/' + c['handoff_prefix'] + '/' + base + c['handoff_suffix'])
") || { echo "핸드오프 키 목록을 못 만들었다" >&2; exit 1; }

total_files=${#all[@]}
batches=$(( (total_files + FILES_PER_BATCH - 1) / FILES_PER_BATCH ))
echo "입력 $total_files 개 · 묶음 $batches 개 · 표 $TABLE · 방식 $MODE"

wrote=0 skipped=0 fail=0 rows_total=0
started=$(date +%s)

for (( i=0; i<batches; i++ )); do
  start=$(( i * FILES_PER_BATCH ))
  input=""
  count=0
  for (( j=start; j<start+FILES_PER_BATCH && j<total_files; j++ )); do
    input="${input:+$input,}${all[$j]}"
    count=$((count+1))
  done

  extra="--validate-only"
  [ "$MODE" = "load" ] && extra="--iceberg-write-mode append"

  log="$STATE/batch-$i.$MODE.log"
  t0=$(date +%s)
  submit /workspace/infra/lakehouse/spark/jobs/silver_scalar_handoff_to_lakehouse.py \
    --contract "silver.$TABLE" --input "$input" \
    --write-mode iceberg \
    $extra > "$log" 2>&1
  rc=$?
  el=$(( $(date +%s) - t0 ))
  outcome=$(grep -aoE "silver-scalar-handoff-(validate-ok|iceberg-write-ok|iceberg-already-ingested)( rows=[0-9]+)?" "$log" | tail -1)
  rows=$(echo "$outcome" | grep -oE "[0-9]+$")

  if [ "$rc" -ne 0 ]; then
    fail=$((fail+1))
    echo "묶음 $i 실패 rc=$rc ${el}초" >&2
    grep -aviE 'WARN|INFO |log4j|^$|Ivy|found |downloading|:: |confs:|artifacts|^[[:space:]]*at ' "$log" | tail -6 | cut -c1-240 >&2
    break
  fi

  rows_total=$(( rows_total + ${rows:-0} ))
  case "$outcome" in
    *already-ingested*) skipped=$((skipped+1)); verb="건너뜀" ;;
    *)                  wrote=$((wrote+1));    verb="적재" ;;
  esac
  echo "묶음 $i/$((batches-1))  파일 ${count}개  행 ${rows:-?}  ${el}초  $verb"
done

el=$(( $(date +%s) - started ))
echo "적재 끝: 새로 $wrote · 건너뜀 $skipped · 실패 $fail · $((el/60))분 · 행 $rows_total"

# 적재의 나머지 절반 (합치기 → 스냅숏 정리). 실패했으면 돌리지 않는다.
if [ "$MODE" = "load" ] && [ "$fail" -eq 0 ] && [ "$wrote" -gt 0 ]; then
  echo "유지보수 시작 (합치기 → 스냅숏 정리)"
  submit /workspace/infra/lakehouse/spark/jobs/lakehouse_maintenance.py \
    --table "silver.$TABLE" --skip orphan_cleanup 2>&1 | grep -aE "^maintenance-"
elif [ "$MODE" = "load" ] && [ "$wrote" -eq 0 ]; then
  echo "유지보수 건너뜀: 새로 붙인 것이 없다"
elif [ "$fail" -ne 0 ]; then
  echo "유지보수 건너뜀: 적재가 실패했다 — 표를 먼저 확인할 것" >&2
fi

exit $(( fail > 0 ? 1 : 0 ))
