#!/usr/bin/env bash
# ADR-0275 1단계 측정 한 번: 토지 이동 이력 한 판을 비교 후 바뀐 행만 쌓기로 scratch 네임스페이스에 써서
# 단계(복사·비교·추가)마다 시간과 행 수를 잰다. 통째로 쓰기는 첫 실행이 쟀다(2026-10-18, 약 3.5분).
# 단계 줄(silver-append-measure-phase)은 시작과 끝마다 바로 유닛 저널로 가서, 시간 상한에 끊겨도 남는다. 꺼진 작업 silver_append_measure 이고, 운영자가
# `by-pnu-pack-operator.sh job start silver_append_measure` 로 한 번 시작한다(자동 배포·Gold·Silver 가
# 돌고 있으면 그 스크립트가 거부한다). 운영 Silver 표는 읽기만 한다(silver_append_measure.py).
#
# 비교할 앞 판은 0630 판을 적재한 스냅숏이다(2026-10-11, 30일 보존 안). 다른 판을 재려면
# FOUNDATION_SILVER_APPEND_MEASURE_OLD_SNAPSHOT_ID 로 바꾼다.
# 마지막 줄은 `foundation-job-outcome changed` 다(root ADR-0171): scratch 표는 끝날 때 지운다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

source "$(dirname "${BASH_SOURCE[0]}")/job-state-root.sh"
# Both roots under the job's state_paths in jobs.v1.json (root ADR-0266).
STATE_ROOT="${FOUNDATION_SILVER_APPEND_MEASURE_STATE_ROOT:-$(job_state_root silver_append_measure)/silver-append-measure}"
LAKEHOUSE_STATE_ROOT="${FOUNDATION_SILVER_APPEND_MEASURE_LAKEHOUSE_ROOT:-$(job_state_root silver_append_measure)/lakehouse}"
OLD_SNAPSHOT_ID="${FOUNDATION_SILVER_APPEND_MEASURE_OLD_SNAPSHOT_ID:-8897560896258584627}"
DRIVER_MEMORY="${FOUNDATION_SILVER_APPEND_MEASURE_DRIVER_MEMORY:-16g}"

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/silver-append-measure/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.
run_log="${work}/run.log"
started="${SECONDS}"

on_error() {
  job_journal "${journal}" "silver append measure FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
spark_status=0
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[8,8]' --driver-memory "${DRIVER_MEMORY}" \
  --jars "${SPARK_RELEASE_JARS}" /workspace/infra/lakehouse/spark/jobs/silver_append_measure.py \
  --old-snapshot-id "${OLD_SNAPSHOT_ID}" 2>&1 | tee -a "${run_log}"   | { grep --line-buffered -a '^silver-append-measure-phase ' || true; } || spark_status=$?
# pipefail: a failed Spark run fails the pipe even when the phase lines got through.
((${spark_status:-0} == 0))

measured="$(grep -a '^silver-append-measure ' "${run_log}" | tail -1)"
[[ -n "${measured}" ]]
job_journal "${journal}" "silver append measure run=${run_id} seconds=$((SECONDS - started)) ${measured#silver-append-measure }"
printf '%s\n' "${measured}"
printf 'foundation-job-outcome changed\n'
