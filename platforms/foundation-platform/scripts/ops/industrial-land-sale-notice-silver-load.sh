#!/usr/bin/env bash
# 산업용지 분양공고의 Silver 적재 절반 (root ADR-0603). 등록된 작업 industrial_land_sale_notice_silver 가 수집 실행이
# Bronze 를 바꿨다고 알리면, 그리고 일정마다 돌린다. Spark 는 쓰지 않는다. 2026-11-28 부터(W109e, root ADR-0624 A1)
# 이 구성 요소의 표는 Lakekeeper 에 있고 적재는 AWS 레인이 돈다: 이 스크립트는 호스트로 되돌릴 때의 길이다.
#
# 발행기의 run-industrial-land-sale-notice-silver 하나가 AWS 레인과 같은 일을 한다: 쌓을 것은 원장(catalog.bronze_object)과
# 표의 커밋 기록만 보고 정하고(네 원천의 상세 객체 중 읽은 기록 표 silver.industrial_land_sale_notice_fetch_attempt 의
# 커밋 요약이 기록하지 않은 것, 오래된 것부터, 계약 silver_load 의 범위 안에서), 객체마다 발행기 안의 DuckDB 가 행을
# 만들고 발행기가 Iceberg 추가 커밋으로 붙인다(root ADR-0559·ADR-0560): 먼저 저장소에 담긴 첨부 가운데 새것·바뀐 것을
# silver.industrial_land_sale_notice_attachment(미디어 대조기가 읽는 색인)에, 그다음 읽은 기록을 쌓는다. 행이 없는
# 객체도 기록한다. 쌓을 것이 없으면 `unchanged`, 창보다 오래 기다린 객체가 있으면 쌓은 뒤 실패한다.
#
# 실패하면 커밋하지 못한 객체는 표에 기록이 없어 다음 실행이 다시 받는다. 받은 사본은 성공하든 실패하든 지운다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# The plan reads the sources' Bronze objects from the ledger (root ADR-0626 §4, amending root ADR-0280):
# recovery.env holds the password, not the address, so it is built as the collectors build it.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_INDUSTRIAL_LAND_SALE_NOTICE_STATE_ROOT:-/var/lib/foundation-platform/industrial-land-sale-notice}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "industrial land sale notice silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/details"' EXIT

lakehouse_run_log "${run_log}"
FOUNDATION_PLATFORM_INDUSTRIAL_LAND_SALE_NOTICE_SILVER_OUTPUT_DIR="${work}" \
  "${PUBLISHER_BIN}" run-industrial-land-sale-notice-silver >> "${run_log}" 2>&1

outcome="$(grep '^foundation-job-outcome ' "${run_log}" | tail -1)"
summary="$(grep '^industrial-land-sale-notice-summary-json ' "${run_log}" | tail -1 || true)"
job_journal "${journal}" "industrial land sale notice silver load ok run=${run_id} ${outcome#foundation-job-outcome } ${summary}"
printf '%s\n' "${outcome}"
