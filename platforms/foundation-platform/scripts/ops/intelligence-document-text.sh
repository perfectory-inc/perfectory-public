#!/usr/bin/env bash
# 고시 첨부의 쪽·위치 텍스트 (root ADR-0477). 등록된 작업 intelligence_document_text(spark 1칸, 가중치 1)가 첨부 색인이
# 바뀌었다고 알리면, 그리고 일정마다 돌린다.
#
# 1. 읽을 것은 표만 보고 정한다: intelligence_documents.py plan-text 가 색인의 해시 중 이 이미지의 추출기 판의 결과가
#    없는 것을 새 고시부터 per_run_limit 까지 고른다(저장된 대기열 없음, ADR-0352). 판은 이미지가 담은 계약의 것이다.
# 2. 그 바이트를 공용 내용주소 저장소에서 읽기 키로 받는다(읽기만).
# 3. intelligence 이미지가 네트워크 없이, 계약의 메모리 상한 안에서 읽는다. 문서마다 결과를 바로 쓰므로 멈춘 run 도
#    끝낸 문서를 잃지 않는다.
# 4. intelligence_documents.py append-text 가 쪽을 먼저, 결과를 나중에 run 하나로 쌓는다. 결과가 없는 문서는 다음 run 이
#    다시 고른다.
#
# 받은 바이트는 성공하든 실패하든 지운다. 쌓을 것이 없으면 이미지를 띄우지 않는다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"
source "$(dirname "${BASH_SOURCE[0]}")/intelligence-document-runtime.sh"

STATE_ROOT="${FOUNDATION_INTELLIGENCE_DOCUMENTS_STATE_ROOT:-/data/foundation-platform/intelligence-documents}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
intelligence_document_run_dir text
trap 'job_journal "${journal}" "intelligence document text FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/media"' EXIT

intelligence_document_extraction_contract
intelligence_document_spark plan-text --out "${container_work}/plan"
owed="$(intelligence_document_summary owed)"
planned="$(intelligence_document_summary planned)"
if [ "${planned}" = 0 ]; then
  job_journal "${journal}" "intelligence document text: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

intelligence_document_fetch_media "${work}/plan/media.tsv"
# The image writes each document's rows as it finishes it: a run that stops (its memory cap, its
# time limit) still appends what it finished, then fails. 2026-11-12 three runs in a row were killed
# at the cap after about a hundred documents each and appended none, so each next run read them again.
text_rc=0
intelligence_document_text_run || text_rc=$?
intelligence_document_spark append-text --run-dir "${container_work}/out" --run-id "${run_id}"
if [ "${text_rc}" != 0 ]; then
  job_journal "${journal}" "intelligence document text FAILED: the image exited ${text_rc}; its finished documents were appended run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  exit "${text_rc}"
fi

read_summary="$(python3 -I -c '
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({k: s[k] for k in ("read", "run_seconds", "peak_memory_mib", "by_format")}, sort_keys=True))
' "${work}/out/summary.json")"
job_journal "${journal}" "intelligence document text ok run=${run_id} owed=${owed} planned=${planned} ${read_summary}"
printf 'foundation-job-outcome changed\n'
