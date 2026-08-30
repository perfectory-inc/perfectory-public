#!/usr/bin/env bash
# Load a Silver handoff directory into its Iceberg table, then leave the table healthy.
#
# The load and the maintenance are one script because separating them is how the maintenance
# never ran. Every append writes new files; after 16 batches the parcel table held 328 files
# where 14 would do. Compaction is not an occasional chore, it is the second half of loading.
#
# **This script keeps no memory of what it loaded.** The table does (root ADR-0062): the job
# reads the snapshot summaries and skips objects already appended. A marker file beside the
# loader is a second commit to a different medium, and a run that dies between the two leaves
# them disagreeing — which on 2026-08-27 put 1,865,891 parcels into the table three times over.
# Every batch is therefore offered to the job on every run; an already-loaded batch costs the
# time to read and validate its input, and appends nothing.
#
# Measured on ai-server (20 cores, 62 GB) on 2026-08-28: 255 objects, 39,861,511 rows, 16
# batches, 19 minutes.
set -uo pipefail

MODE="${1:-validate}"          # validate (안 씀) | load (실제 적재)
TABLE="${2:-parcel_boundaries}"
SRC="${LAKEHOUSE_HANDOFF_DIR:-$HOME/parcel-handoff}"
STATE="${LAKEHOUSE_LOAD_STATE_DIR:-$HOME/parcel-load-state}"
WORK_ROOT="${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-$HOME/lakehouse-state}"
RELEASE="${FOUNDATION_PLATFORM_RELEASE_DIR:-/opt/foundation-platform/current}"
FILES_PER_BATCH="${FILES_PER_BATCH:-16}"
DRIVER_MEM="${DRIVER_MEM:-24g}"
TASKS="${TASKS:-8}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-foundation-platform-compute}"

for v in FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
         FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
         FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN; do
  if [ -z "${!v:-}" ]; then
    echo "필수 환경변수가 비어 있다: $v" >&2
    exit 1
  fi
done
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER="${FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER:-r2_data_catalog}"
export FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="$WORK_ROOT"

mkdir -p "$STATE" "$WORK_ROOT"
chmod 777 "$WORK_ROOT" 2>/dev/null || true
cd "$RELEASE" || { echo "릴리스 디렉터리 없음: $RELEASE" >&2; exit 1; }

# 판은 계약이 정한다. 여기 적으면 잡과 다른 Iceberg 가 실린다 (root ADR-0065).
PACKAGES=$(python3 -c "
import json
c = json.load(open('$RELEASE/infra/lakehouse/contracts/lakehouse-engine.contract.json'))
i = c['iceberg']
print(','.join(a + ':' + i['version'] for a in i['artifacts']))")

submit() {
  docker compose -p "$COMPOSE_PROJECT" -f compose.lakehouse.yml --profile lakehouse-batch run --rm \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark \
    spark-submit --master "local[$TASKS]" --driver-memory "$DRIVER_MEM" \
    --packages "$PACKAGES" --conf spark.jars.ivy=/tmp/.ivy2 "$@"
}

mapfile -t all < <(ls "$SRC"/*.jsonl 2>/dev/null | xargs -n1 basename | sort)
total_files=${#all[@]}
if [ "$total_files" -eq 0 ]; then echo "핸드오프 파일이 없다: $SRC" >&2; exit 1; fi

batches=$(( (total_files + FILES_PER_BATCH - 1) / FILES_PER_BATCH ))
echo "파일 $total_files 개 · 묶음 $batches 개 · 표 $TABLE · 방식 $MODE"

wrote=0 skipped=0 fail=0 rows_total=0
started=$(date +%s)

for (( i=0; i<batches; i++ )); do
  work="$WORK_ROOT/batch-$i"
  rm -rf "$work"
  mkdir -p "$work" || { echo "묶음 디렉터리를 못 만든다: $work" >&2; exit 1; }

  start=$(( i * FILES_PER_BATCH ))
  count=0
  for (( j=start; j<start+FILES_PER_BATCH && j<total_files; j++ )); do
    ln "$SRC/${all[$j]}" "$work/${all[$j]}" 2>/dev/null || cp "$SRC/${all[$j]}" "$work/${all[$j]}"
    count=$((count+1))
  done
  linked=$(ls "$work"/*.jsonl 2>/dev/null | wc -l)
  if [ "$linked" -ne "$count" ]; then
    echo "묶음 $i: $count 개 중 $linked 개만 준비됨 — 조용히 빠지는 것을 막기 위해 중단" >&2
    exit 1
  fi
  chmod -R a+rX "$work" 2>/dev/null || true

  extra="--validate-only"
  [ "$MODE" = "load" ] && extra="--iceberg-write-mode append"

  log="$STATE/batch-$i.$MODE.log"
  t0=$(date +%s)
  submit /workspace/infra/lakehouse/spark/jobs/vworld_parcel_boundaries_handoff_to_silver.py \
    --input "/workspace/target/lakehouse/batch-$i/*.jsonl" \
    --write-mode iceberg --iceberg-table "$TABLE" \
    $extra > "$log" 2>&1
  rc=$?
  el=$(( $(date +%s) - t0 ))
  outcome=$(grep -aoE "silver-parcel-boundaries-(validate-ok|iceberg-write-ok|iceberg-already-ingested) rows=[0-9]+" "$log" | tail -1)
  rows=$(echo "$outcome" | grep -oE "[0-9]+$")
  rm -rf "$work"

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

# 적재의 나머지 절반. 붙인 파일은 목표 크기보다 작고, 밀려난 파일은 스냅숏이 붙들고 있다.
# 실패했으면 돌리지 않는다 — 반쯤 들어간 표를 다시 쓰는 것은 사람이 볼 일이다.
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
