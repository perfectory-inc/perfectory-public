#!/usr/bin/env bash
# 공시가격 두 표의 판을 버전 표로 옮긴다 — ADR-0317 2단계, 운영자 한 번(by-pnu-pack-operator.sh prices
# migrate-versions). 루트 ADR-0317 §5.2 의 순서 그대로:
#
#   1. silver.building_register_apartment_price → silver.building_register_apartment_price_versions
#   2. silver.unit_official_price               → silver.unit_official_price_versions
#
# vintage_versions.py 가 판을 오래된 것부터 쓰고, 판마다 rows_as_of 와 옛 사본을 exceptAll 로 양방향 대조한다.
# 어느 판이든 missing 이나 extra 가 0 이 아니면 그 표에서 멈추고 실패한다(종료 코드 1). 다시 돌리면 기록된 판은
# 건너뛰고 대조만 다시 한다. 옛 표는 읽기만 한다(append-only).
#
# 마지막 줄은 `unit-price-versions parity=ok` 다. 그 줄과 표마다의 요약이 #919(ADR-0317 3단계)의 병합 조건이다.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

# The unit's StateDirectory (systemd makes it, owned by the unit's user); the override is the tests'.
STATE_ROOT="${FOUNDATION_UNIT_PRICE_VERSIONS_STATE_ROOT:-${STATE_DIRECTORY:-/var/lib/foundation-platform/unit-price-versions}}"
SCRATCH="${FOUNDATION_UNIT_PRICE_VERSIONS_SCRATCH:-/data/foundation-platform/lakehouse/spark-scratch}"
TARGETS=(silver.building_register_apartment_price_versions silver.unit_official_price_versions)

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
for target in "${TARGETS[@]}"; do
  work="${STATE_ROOT}/runs/${run_id}/${target}"
  mkdir -p "${work}"
  chmod 0777 "${work}" # the Spark container writes as uid 185
  status=0
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${work}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute -v "${SCRATCH}:/scratch" \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
    -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
    spark spark-submit --master 'local[8,8]' --driver-memory 16g \
    --conf spark.local.dir=/scratch --jars "${SPARK_RELEASE_JARS}" \
    /workspace/infra/lakehouse/spark/jobs/vintage_versions.py --target "${target}" \
    --summary-output /workspace/target/lakehouse/summary.json > "${work}/run.log" 2>&1 || status=$?
  # One line per vintage (missing, extra, rows, seconds) reaches the journal; the Spark log stays here.
  grep -a '^vintage-versions ' "${work}/run.log" || true
  if [[ "${status}" != 0 ]]; then
    grep -a -E 'Traceback|Error|Exception' "${work}/run.log" | grep -v '^\s*at ' | tail -5 || true
    printf 'unit-price-versions FAILED target=%s status=%s log=%s\n' "${target}" "${status}" "${work}/run.log"
    exit "${status}"
  fi
done
printf 'unit-price-versions parity=ok run=%s\n' "${run_id}"
