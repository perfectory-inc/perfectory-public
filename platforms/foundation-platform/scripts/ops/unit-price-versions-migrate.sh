#!/usr/bin/env bash
# 공시가격 두 표의 판을 버전 표로 옮긴다 — ADR-0317 2단계, 작업 unit_price_versions_migration(jobs.v1.json,
# 매일 18:45 UTC 스케줄, 다 끝나면 걸음 없는 no-op). 루트 ADR-0317 §5.2 의 순서 그대로:
#
#   1. silver.building_register_apartment_price → silver.building_register_apartment_price_versions
#   2. silver.unit_official_price               → silver.unit_official_price_versions
#
# 한 번의 실행은 vintage-versions-migration.contract.json 의 run_time_budget_seconds 안에서 끝난다: 판 하나의
# 쓰기와 판 하나의 대조가 한 걸음이고, 남은 시간이 그 걸음의 측정 비용보다 짧으면 시작하지 않는다(2026-11-04
# 첫 실행이 판 하나 13,226초 뒤 8시간 한도에 죽고 어디까지 했는지 남기지 못했다). 쓴 판은 커밋으로, 대조를
# 통과한 판은 표 속성 foundation.parity.<판> 으로 남으니 다음 실행은 남은 걸음부터 한다. 대조가 어긋나면
# 그 표에서 멈추고 실패한다(종료 코드 1). 옛 표는 읽기만 한다(append-only).
#
# 걸음마다 `vintage-versions <표> <판>: {...}` 한 줄이 바로 journal 에 간다. 끝 줄은 두 표가 다 대조를
# 통과했으면 `unit-price-versions parity=ok` — #919(ADR-0317 3단계)의 병합 조건 — 아니면
# `unit-price-versions progress=...` 이고, 그 뒤에 `foundation-job-outcome changed|unchanged`. 끝나지 않았는데
# 한 걸음도 못 뗀 실행은 멈춘 이행이라 `failed` 로 실패한다(운영 지도 알림).
#
# Spark runs in the Compose project foundation-unit-price-versions-<INVOCATION_ID>. A timeout kills
# this script and the compose client but not the daemon-owned container, which would keep its 20g
# while the pool is free again; the unit's ExecStopPost (`cleanup`) has the release's publisher
# remove that project's one-off containers and networks (root ADR-0128), and nothing else.
set -euo pipefail

if [[ "${1:-}" == cleanup ]]; then
  [[ "$#" == 1 ]] || { echo "unit-price-versions: cleanup takes no arguments" >&2; exit 64; }
  [[ "${INVOCATION_ID:-}" =~ ^[0-9a-f]{32}$ ]] || { echo "unit-price-versions: cleanup needs the systemd INVOCATION_ID" >&2; exit 64; }
  # Any installed release: `current` may have moved since this invocation started.
  source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --installed
  exec "${PUBLISHER_BIN}" stop-unit-price-versions
fi
(($# == 0)) || { echo "unit-price-versions: expected no argument or cleanup, got '$1'" >&2; exit 64; }
if [[ -z "${INVOCATION_ID:-}" ]]; then
  INVOCATION_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "unit-price-versions: no systemd invocation; this run's Compose project is foundation-unit-price-versions-${INVOCATION_ID} (if interrupted: INVOCATION_ID=${INVOCATION_ID} $0 cleanup)" >&2
fi
[[ "${INVOCATION_ID}" =~ ^[0-9a-f]{32}$ ]] || { echo "unit-price-versions: INVOCATION_ID is not a systemd invocation id" >&2; exit 64; }
export INVOCATION_ID

source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

# The unit's StateDirectory (systemd makes it, owned by the unit's user); the override is the tests'.
STATE_ROOT="${FOUNDATION_UNIT_PRICE_VERSIONS_STATE_ROOT:-${STATE_DIRECTORY:-/var/lib/foundation-platform/unit-price-versions}}"
SCRATCH="${FOUNDATION_UNIT_PRICE_VERSIONS_SCRATCH:-/data/foundation-platform/lakehouse/spark-scratch}"

read -r master driver_memory < <(python3 -I - "${RELEASE_ROOT}/infra/lakehouse/spark/jobs" <<'PY'
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
import vintage_versions
spark = vintage_versions.load_contract()["spark"]
print(spark["master"], spark["driver_memory"])
PY
)
[[ -n "${driver_memory:-}" ]] || { printf 'unit-price-versions refused: the migration contract names no spark sizing\n'; exit 65; }

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # the Spark container writes as uid 185
# A backfill yields at a step boundary (root ADR-0435): while the run lasts, the host's capacity queue
# is asked every poll whether a higher priority class (a by-PNU publish) waits for the spark pool;
# then the run's yield file is written, which the job reads before each step, and no further step
# starts. Every step taken is committed, so the next start resumes after it. 2026-11-06 00:37Z: the
# parcel-ownership cut-over waited 45 minutes for this run and failed.
yield_file="${work}/yield"
(
  while sleep "${FOUNDATION_UNIT_PRICE_VERSIONS_YIELD_POLL_SECONDS:-30}"; do
    reason="$(python3 -I -B "${RELEASE_ROOT}/scripts/deploy/job_units.py" preempted \
      "${RELEASE_ROOT}/orchestration/jobs.v1.json" unit_price_versions_migration 2>/dev/null)" || continue
    [[ -z "${reason}" ]] || { printf '%s\n' "${reason}" >"${yield_file}"; exit 0; }
  done
) &
yield_watch=$!
trap 'kill "${yield_watch}" 2>/dev/null || true' EXIT
# Each step's line reaches the journal as it is printed; the Spark log stays in the run directory.
set +e
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${work}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
lakehouse_spark_run "foundation-unit-price-versions-${INVOCATION_ID}" -v "${SCRATCH}:/scratch" \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master "${master}" --driver-memory "${driver_memory}" \
  --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/vintage_versions.py \
  --summary-output /workspace/target/lakehouse/summary.json --yield-file /workspace/target/lakehouse/yield 2>&1 \
  | tee "${work}/run.log" | grep -a --line-buffered '^vintage-versions '
status="${PIPESTATUS[0]}"
set -e
if [[ "${status}" != 0 ]]; then
  grep -a -E 'Traceback|Error|Exception' "${work}/run.log" | grep -v '^\s*at ' | tail -5 || true
  printf 'unit-price-versions FAILED status=%s log=%s\n' "${status}" "${work}/run.log"
  exit "${status}"
fi
summary="$(grep -a '^vintage-versions {' "${work}/run.log" | tail -1)"
# The scheduled job's word for the run (vintage_versions.run_state): a complete run took no step and
# is the daily no-op; one not complete that took no step is stuck, and fails so the operations map
# alerts (FoundationScheduledJobProblem), not a quiet `unchanged` every day.
read -r state written < <(python3 -I - "${RELEASE_ROOT}/infra/lakehouse/spark/jobs" "${summary#vintage-versions }" <<'PY'
import json, sys
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
import vintage_versions
state, took = vintage_versions.run_state(json.loads(sys.argv[2]))
print(state, "yes" if took else "no")
PY
)
case "${state:-}" in
  complete) printf 'unit-price-versions parity=ok run=%s
' "${run_id}" ;;
  progress) printf 'unit-price-versions progress=incomplete run=%s (the next run continues)
' "${run_id}" ;;
  yielded)
    printf 'unit-price-versions yielded run=%s: %s (the next run continues)
' "${run_id}" "$(cat "${yield_file}" 2>/dev/null)"
    if [[ "${written}" != yes ]]; then
      printf 'foundation-job-outcome waiting %s
' "$(cat "${yield_file}" 2>/dev/null || echo 'yielded the spark pool to a higher priority class')"
      exit 0
    fi ;;
  stalled)
    printf 'unit-price-versions FAILED stalled: not complete and no step fit the run budget run=%s log=%s
' "${run_id}" "${work}/run.log"
    printf 'foundation-job-outcome failed no step fit the run budget; the migration made no progress
'
    exit 1 ;;
  *) printf 'unit-price-versions FAILED: the run summary is unreadable log=%s
' "${work}/run.log"; exit 1 ;;
esac
printf 'foundation-job-outcome %s
' "$([[ "${written}" == yes ]] && echo changed || echo unchanged)"
