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
#
# **Why it runs several at once.** Converted one at a time, a twenty-core machine sat at 0.27 load
# while the network stayed saturated: the time goes into moving bytes, not into computing. Runs
# that overlap overlap their waiting. The national run took 38 minutes at eight; sequentially the
# same work is hours, and a step that takes hours is a step nobody re-runs to check.
#
# **Why the tally is not read from the log.** It was, and it lied. Both outcomes are logged at
# `info`, the national run set `RUST_LOG=warn`, and so a run that skipped all 255 objects reported
# converting all 255. Each conversion now writes a summary naming what it did, and the tally is
# counted from those files — a level chosen for noise cannot silence a field.
set -uo pipefail

MODE="${1:-plan}"              # plan (무엇을 할지 보여주기만) | run (실제 변환)
RELEASE="${FOUNDATION_PLATFORM_RELEASE_DIR:-/opt/foundation-platform/current}"
STATE="${VWORLD_PARCEL_EXPORT_STATE_DIR:-$HOME/parcel-export-state}"
JOBS="${VWORLD_PARCEL_EXPORT_JOBS:-8}"
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

if [ "$MODE" != "run" ]; then
  for key in "${objects[@]}"; do
    base="$(basename "$key" .zip)"
    echo "변환예정 $base  →  $HANDOFF_PREFIX/$base$SUFFIX"
  done
  echo "끝: 계획 $total 개 · 동시 $JOBS 개로 돌 예정"
  exit 0
fi

# 실행마다 제 폴더를 갖는다. 앞 실행의 요약을 덮어쓰면 두 실행의 증거가 하나만 남고,
# 남는 쪽은 나중에 쓴 것 — 즉 대개 아무것도 안 한 실행의 것이다. 명령 자체도 이미 있는
# 요약 위에 쓰기를 거부하므로, 폴더를 나누지 않으면 두 번째 실행이 통째로 실패한다.
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="$STATE/$RUN_ID"
mkdir -p "$RUN_DIR"

convert_one() {
  local key="$1"
  local base; base="$(basename "$key" .zip)"
  local log="$RUN_DIR/$base.log"
  local t0; t0=$(date +%s)

  # 이미 변환됐는지는 여기서 묻지 않는다. 명령이 R2 에 직접 물어 이미 있으면 아무것도 하지
  # 않고 성공한다 — 적재기가 묶음마다 하는 것과 같다. 이 스크립트가 따로 기억하면 그것이
  # 실물과 어긋날 수 있는 세 번째 기록이 된다.
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_INPUT_OBJECT_KEY="$key" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_OUTPUT_OBJECT_KEY="$HANDOFF_PREFIX/$base$SUFFIX" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_RECORD_ID="$key" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SOURCE_SNAPSHOT_ID="$SOURCE_SNAPSHOT_ID" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_VALID_FROM_UTC="$VALID_FROM_UTC" \
  FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SHAPEFILE_SUMMARY_PATH="$RUN_DIR/$base.summary.json" \
    "$PUBLISHER" export-vworld-cadastral-shapefile-silver-handoff > "$log" 2>&1
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
export RUN_DIR PUBLISHER HANDOFF_PREFIX SUFFIX SOURCE_SNAPSHOT_ID VALID_FROM_UTC

echo "원천 $total 개 · 동시 $JOBS 개 · 증거 $RUN_DIR"
started=$(date +%s)
printf '%s\n' "${objects[@]}" | xargs -P "$JOBS" -I{} bash -c 'convert_one "$@"' _ {}
el=$(( $(date +%s) - started ))

# 무엇을 했는지는 각 변환이 남긴 요약이 말한다. 로그 문장을 세던 때는 `RUST_LOG` 하나로
# 답이 바뀌었다 — 두 결과 모두 info 수준이라, warn 으로 돌린 전국 실행은 255개를 전부
# 건너뛰고도 전부 변환했다고 셌다. 요약을 읽는 것은 결정이 아니라 집계다: 무엇을 건너뛸지는
# 여전히 R2 가 정한다.
python3 - "$RUN_DIR" "$total" "$el" <<'PY'
import json, pathlib, sys

run_dir, total, elapsed = pathlib.Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
counts = {}
for path in sorted(run_dir.glob("*.summary.json")):
    try:
        counts[json.loads(path.read_text(encoding="utf-8"))["outcome"]] = counts.get(
            json.loads(path.read_text(encoding="utf-8"))["outcome"], 0
        ) + 1
    except (OSError, ValueError, KeyError) as error:
        print(f"요약을 못 읽었다 {path.name}: {error}", file=sys.stderr)
        counts["읽지못함"] = counts.get("읽지못함", 0) + 1

converted = counts.get("converted", 0)
skipped = counts.get("already_present", 0)
accounted = sum(counts.values())
print(
    f"끝: 변환 {converted} · 건너뜀 {skipped} · 요약없음 {total - accounted} · {elapsed // 60}분"
)
# 요약이 없는 객체는 실패했거나 시작도 못 한 것이다. 0 으로 끝나면 아무도 다시 안 본다.
sys.exit(1 if accounted != total else 0)
PY
