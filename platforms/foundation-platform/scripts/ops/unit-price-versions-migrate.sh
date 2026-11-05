#!/usr/bin/env bash
# 공시가격 두 표의 판을 버전 표로 옮긴다 — ADR-0317 2단계, 작업 unit_price_versions_migration(jobs.v1.json,
# 운영자 `by-pnu-pack-operator.sh job start`). 루트 ADR-0317 §5.2 의 순서 그대로:
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
# `unit-price-versions progress=...` 이고, 그 뒤에 `foundation-job-outcome changed|unchanged`.
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
  --summary-output /workspace/target/lakehouse/summary.json 2>&1 \
  | tee "${work}/run.log" | grep -a --line-buffered '^vintage-versions '
status="${PIPESTATUS[0]}"
set -e
if [[ "${status}" != 0 ]]; then
  grep -a -E 'Traceback|Error|Exception' "${work}/run.log" | grep -v '^\s*at ' | tail -5 || true
  printf 'unit-price-versions FAILED status=%s log=%s\n' "${status}" "${work}/run.log"
  exit "${status}"
fi
summary="$(grep -a '^vintage-versions {' "${work}/run.log" | tail -1)"
read -r complete written < <(python3 -I -c '
import json, sys
s = json.loads(sys.argv[1].split(" ", 1)[1])
steps = [e for t in s["targets"].values() for e in t["vintages"].values()]
print("yes" if s["complete"] else "no", "yes" if any("appended_seconds" in e or "parity_seconds" in e for e in steps) else "no")
' "${summary}")
if [[ "${complete}" == yes ]]; then
  printf 'unit-price-versions parity=ok run=%s\n' "${run_id}"
else
  printf 'unit-price-versions progress=incomplete run=%s (the next run continues)\n' "${run_id}"
fi
printf 'foundation-job-outcome %s\n' "$([[ "${written}" == yes ]] && echo changed || echo unchanged)"
