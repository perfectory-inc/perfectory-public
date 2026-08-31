#!/usr/bin/env bash
# Convert the collected cadastral shapefile ZIPs into the Silver handoff JSONL a load reads.
#
# This is the step between collection and loading, and until now it had no runner: the command
# existed, nothing invoked it, and the 39,861,511 parcels already in `silver.parcel_boundaries`
# were produced by hand. A step nobody can re-run is a step nobody can check.
#
# **Which objects.** `bronze/source=vworldkr__parcel/` holds the whole country twice — 17 objects
# at province granularity and 255 at district granularity — and the object names say nothing
# about which is which. Converting both would double every parcel. The set is not decided here;
# it is read from `vworld-parcel-source-objects.json`, which records what was measured by
# reading each ZIP's directory (root ADR-0067).
#
# **What it keeps.** Nothing. Whether an object has already been converted is answered by asking
# R2 whether its handoff object exists, the same way the loader asks the table rather than a
# marker file beside itself. A record kept in a third place is a record that can disagree with
# both (root ADR-0062).
set -uo pipefail

MODE="${1:-plan}"              # plan (무엇을 할지 보여주기만) | run (실제 변환)
RELEASE="${FOUNDATION_PLATFORM_RELEASE_DIR:-/opt/foundation-platform/current}"
STATE="${VWORLD_PARCEL_EXPORT_STATE_DIR:-$HOME/parcel-export-state}"
CONTRACT="${VWORLD_PARCEL_SOURCE_CONTRACT:-$RELEASE/infra/lakehouse/contracts/vworld-parcel-source-objects.json}"
HANDOFF_PREFIX="${VWORLD_PARCEL_HANDOFF_PREFIX:-silver-handoff/vworldkr__parcel}"
SOURCE_SNAPSHOT_ID="${VWORLD_PARCEL_SOURCE_SNAPSHOT_ID:-}"
VALID_FROM_UTC="${VWORLD_PARCEL_VALID_FROM_UTC:-}"
PUBLISHER="${FOUNDATION_PLATFORM_PUBLISHER_BIN:-$RELEASE/bin/foundation-outbox-publisher}"

for v in FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID \
         FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY; do
  if [ -z "${!v:-}" ]; then
    echo "필수 환경변수가 비어 있다: $v" >&2
    exit 1
  fi
done

# The lineage values are not invented here. A handoff whose source ids were made up is refused
# three layers down, at publication, long after the run that wrote it is gone.
if [ -z "$SOURCE_SNAPSHOT_ID" ]; then
  echo "VWORLD_PARCEL_SOURCE_SNAPSHOT_ID 가 비어 있다 — 계보 값은 지어내지 않는다" >&2
  exit 1
fi
if [ -z "$VALID_FROM_UTC" ]; then
  echo "VWORLD_PARCEL_VALID_FROM_UTC 가 비어 있다 — 원천 추출 시점을 넘겨야 한다" >&2
  exit 1
fi

[ -f "$CONTRACT" ] || { echo "원천 목록이 없다: $CONTRACT" >&2; exit 1; }
[ -x "$PUBLISHER" ] || { echo "실행 파일이 없다: $PUBLISHER" >&2; exit 1; }
mkdir -p "$STATE"

# 어떤 알갱이를 싣는지는 목록 파일이 정한다. 여기 적으면 목록과 갈라진다.
# 핸드오프 이름의 접미사도 목록 파일이 정한다. 적재기가 같은 파일에서 같은 이름을 만들기
# 때문에, 여기 적으면 둘이 갈라져 적재기가 없는 객체를 찾게 된다.
SUFFIX=$(python3 -c "
import json, sys
c = json.load(open('$CONTRACT'))
if c['schema_version'] != 1:
    sys.exit('source object contract schema_version %r is not the 1 this script reads' % c['schema_version'])
print(c['handoff_suffix'])") || { echo "핸드오프 접미사를 못 읽었다" >&2; exit 1; }

mapfile -t objects < <(python3 -c "
import json
c = json.load(open('$CONTRACT'))
want = c['load_granularity']
for o in c['objects']:
    if o['granularity'] == want:
        print(o['object_key'])
") || { echo "원천 목록을 못 읽었다" >&2; exit 1; }

total=${#objects[@]}
[ "$total" -gt 0 ] || { echo "변환할 객체가 없다" >&2; exit 1; }
echo "원천 $total 개 · 방식 $MODE · 핸드오프 접두사 $HANDOFF_PREFIX"

converted=0 skipped=0 fail=0
started=$(date +%s)

for key in "${objects[@]}"; do
  base="$(basename "$key" .zip)"
  out_key="$HANDOFF_PREFIX/$base$SUFFIX"

  if [ "$MODE" != "run" ]; then
    echo "변환예정 $base  →  $out_key"
    continue
  fi

  # 이미 변환됐는지는 여기서 묻지 않는다. 명령이 R2 에 직접 물어 이미 있으면 아무것도 하지
  # 않고 성공한다 — 적재기가 묶음마다 하는 것과 같다. 이 스크립트가 따로 기억하면 그것이
  # 실물과 어긋날 수 있는 세 번째 기록이 된다.
  log="$STATE/$base.log"
  t0=$(date +%s)
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_INPUT_OBJECT_KEY="$key" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_OUTPUT_OBJECT_KEY="$out_key" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_RECORD_ID="$key" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_SNAPSHOT_ID="$SOURCE_SNAPSHOT_ID" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_VALID_FROM_UTC="$VALID_FROM_UTC" \
    "$PUBLISHER" export-vworld-cadastral-shapefile-silver-handoff > "$log" 2>&1
  rc=$?
  el=$(( $(date +%s) - t0 ))

  if [ "$rc" -ne 0 ]; then
    fail=$((fail+1))
    echo "실패    $base  rc=$rc  ${el}초" >&2
    tail -6 "$log" | cut -c1-240 >&2
    break
  fi

  if grep -aq "already exists" "$log"; then
    skipped=$((skipped+1))
    echo "건너뜀  $base  ${el}초  (핸드오프가 이미 있다)"
  else
    converted=$((converted+1))
    echo "변환    $base  ${el}초"
  fi
done

el=$(( $(date +%s) - started ))
echo "끝: 변환 $converted · 건너뜀 $skipped · 실패 $fail · $((el/60))분"
exit $(( fail > 0 ? 1 : 0 ))
