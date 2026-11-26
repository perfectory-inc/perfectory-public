#!/usr/bin/env bash
# 산업단지 정본 적재 (root ADR-0040, ADR-0426, ADR-0564 §6). 등록된 작업 industrial_complex_canonical_load 가
# gold.complex_catalog_versions 를 쓴 작업(gold_complex_catalog_versions_build)이 `changed` 를 알리면 돈다.
#
# 발행기가 그 표의 현재 행을 읽어 catalog.industrial_complex 에 공식 단지 코드로 넣거나 고친다
# (load-industrial-complex-canonical). 넘겨받기 전에는 계보 순환이 Spark 의 코드 따라가기 뒤에 이 적재를 불렀다:
# 이제 판을 쌓는 표가 짓기마다 코드 변경을 따르므로, 적재는 그 표의 변경을 따라가는 제 작업이다.
# 바뀐 행이 없으면 `unchanged` 다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — publish-outbox.sh 와 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

STATE_ROOT="${FOUNDATION_INDUSTRIAL_COMPLEX_CANONICAL_STATE_ROOT:-/var/lib/foundation-platform/industrial-complex-canonical}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
run_log="${work}/run.log"
mkdir -p "${work}"

on_error() {
  job_journal "${journal}" "industrial-complex canonical load FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_CONFIRM=true \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_CANONICAL_LOAD_SUMMARY_PATH="${work}/summary.json" \
  "${PUBLISHER_BIN}" load-industrial-complex-canonical >> "${run_log}" 2>&1

# "<inserted> <updated> <unchanged> <skipped>", read apart from the summary so a failed read fails the run.
counts="$(python3 -I -c '
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
print(s["inserted_row_count"], s["updated_row_count"], s["unchanged_row_count"], s["skipped_row_count"])
' "${work}/summary.json")"
read -r inserted updated unchanged skipped <<< "${counts}"
job_journal "${journal}" "industrial-complex canonical load inserted=${inserted} updated=${updated} unchanged=${unchanged} skipped=${skipped} run=${run_id}"
# The job's last line, the one every scheduled job ends with (root ADR-0171): `changed` only when a
# row of catalog.industrial_complex was inserted or updated.
if [ "$((inserted + updated))" -eq 0 ]; then
  printf 'foundation-job-outcome unchanged\n'
else
  printf 'foundation-job-outcome changed\n'
fi
