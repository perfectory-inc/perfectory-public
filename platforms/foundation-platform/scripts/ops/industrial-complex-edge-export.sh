#!/usr/bin/env bash
# 산업단지 엣지 내보내기 (root ADR-0456, ADR-0563 §3). 등록된 작업 industrial_complex_edge_export 가
# gold.complex_catalog 나 gold.complex_document_facts 를 쓴 작업이 `changed` 를 알리면, 그리고 대체 일정마다 돌린다.
#
# 발행기가 지금 Gold 와 지금 문서 사실을 읽어, 내용 해시가 엣지 목록(index)의 것과 다른 단지만 프로필을 쓰고
# 목록이 바뀌었으면 새 세대를 낸다(export-industrial-complex-gold-profiles). Spark 를 쓰지 않으므로 spark 풀을
# 기다리지 않는다: 2026-11-12 20:55Z 에 접힌 문서 사실이 11-13 내내 계보 순환의 spark 3칸 뒤에서 엣지에 닿지 못했다.
# 바뀐 단지가 없으면 아무것도 쓰지 않고 `unchanged` 다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_INDUSTRIAL_COMPLEX_EDGE_STATE_ROOT:-/var/lib/foundation-platform/industrial-complex-edge}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
run_log="${work}/run.log"
mkdir -p "${work}"

on_error() {
  job_journal "${journal}" "industrial-complex edge FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_CONFIRM_EXPORT=true \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_OUTPUT_STORAGE_DRIVER=r2 \
FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_PROFILE_SUMMARY_PATH="${work}/summary.json" \
  "${PUBLISHER_BIN}" export-industrial-complex-gold-profiles >> "${run_log}" 2>&1

# "<index generation or None> <profiles written> <unchanged> <complexes with document facts>"
read -r generation written unchanged with_facts < <(python3 -I -c '
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
print(s["index_generation"], s["artifact_count"], s["unchanged_profile_count"], s["document_facts_complex_count"])
' "${work}/summary.json")
job_journal "${journal}" "industrial-complex edge generation=${generation} written=${written} unchanged=${unchanged} document_facts_complexes=${with_facts} run=${run_id}"
# The job's last line, the one every scheduled job ends with (root ADR-0171): `changed` only when a
# new index generation was published.
if [ "${generation}" = None ]; then
  printf 'foundation-job-outcome unchanged\n'
else
  printf 'foundation-job-outcome changed\n'
fi
