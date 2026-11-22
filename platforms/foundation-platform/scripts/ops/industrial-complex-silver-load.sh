#!/usr/bin/env bash
# 산업단지 기본 정보의 Silver 적재 (root ADR-0033, ADR-0559). 등록된 작업 silver_industrial_complexes(documents 풀)를
# 입력(산업단지 프로필 통합본·ILIS 목록·고시·상세·시군구 경계)의 새 Bronze 객체가 깨운다. Spark 는 쓰지 않는다.
#
# 1. plan-industrial-complex-silver-inputs 가 R2 에서 입력마다 가장 새 객체를 고르고, silver.industrial_complexes 의
#    커밋 기록이 그것을 모두 가지면 아무것도 받지 않고 끝난다(unchanged). 아니면 이 실행의 폴더로 제 키 그대로 받는다.
# 2. build-industrial-complex-address-resolution 이 주소를 해석하고, export-industrial-complex-bronze-raw-jsonl 이
#    단지마다 한 줄을 쓴다(둘 다 그대로).
# 3. load-industrial-complex-silver 가 발행기 안의 DuckDB 로 줄을 Silver 행으로 읽고 관문을 거친 뒤, 지금 행과 다른
#    단지만 붙이는 커밋 하나로 표가 아직 기록하지 않은 입력 객체를 기록한다. 바뀐 단지가 없어도 기록한다. 앞선 커밋이
#    기록한 입력(몇 달 그대로인 프로필 통합본)은 거기 남는다: 배치는 통째로만 붙고, 그것을 다시 적은 배치는 거부됐다
#    (2026-11-22 첫 실행).
#
# 실패하면 커밋이 없어 다음 실행이 같은 입력을 다시 받는다. 받은 사본은 성공하든 실패하든 지운다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_INDUSTRIAL_COMPLEX_SILVER_STATE_ROOT:-/var/lib/foundation-platform/industrial-complex-silver}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial-complex silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/bronze-root" "${work}/address-resolution.jsonl" "${work}/export.jsonl"' EXIT

SILVER="FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_SILVER"
env "${SILVER}_WORK_DIR=${work}" "${PUBLISHER_BIN}" plan-industrial-complex-silver-inputs >> "${run_log}" 2>&1
owed="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["owed"])' "${work}/plan.json")"
if [ "${owed}" != True ]; then
  job_journal "${journal}" "industrial-complex silver load: every input already recorded run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
authority="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["sigungu_authority"])' "${work}/plan.json")"

lakehouse_run_log "${run_log}"
RESOLUTION="FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_ADDRESS_RESOLUTION"
env "${RESOLUTION}_BRONZE_ROOT=${work}/bronze-root" \
  "${RESOLUTION}_SIGUNGU_AUTHORITY_PATH=${work}/${authority}" \
  "${RESOLUTION}_OUTPUT_PATH=${work}/address-resolution.jsonl" \
  "${RESOLUTION}_SUMMARY_PATH=${work}/address-resolution-summary.json" \
  "${PUBLISHER_BIN}" build-industrial-complex-address-resolution >> "${run_log}" 2>&1
EXPORT="FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BRONZE_RAW_JSONL"
env "${EXPORT}_BRONZE_ROOT=${work}/bronze-root" \
  "${EXPORT}_ADDRESS_SOURCE_PATH=${work}/address-resolution.jsonl" \
  "${EXPORT}_OUTPUT_PATH=${work}/export.jsonl" \
  "${EXPORT}_SUMMARY_PATH=${work}/export-summary.json" \
  "${PUBLISHER_BIN}" export-industrial-complex-bronze-raw-jsonl >> "${run_log}" 2>&1
env "${SILVER}_WORK_DIR=${work}" "${SILVER}_EXPORT_PATH=${work}/export.jsonl" "${SILVER}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" load-industrial-complex-silver >> "${run_log}" 2>&1

summary="$(grep '^industrial-complex-silver-summary-json ' "${run_log}" | tail -1)"
job_journal "${journal}" "industrial-complex silver load ok run=${run_id} ${summary#industrial-complex-silver-summary-json }"
# A commit that only records the inputs changes no row a reader sees: what reads the table is not woken.
changed_rows="$(python3 -I -c 'import json, sys; print(json.loads(sys.argv[1].split(" ", 1)[1])["changed_rows"])' "${summary}")"
if [ "${changed_rows}" = 0 ]; then
  printf 'foundation-job-outcome unchanged\n'
else
  printf 'foundation-job-outcome changed\n'
fi
