#!/usr/bin/env bash
# 고시 첨부 텍스트에서 인용 달린 사실 (root ADR-0477 §5). 등록된 작업 intelligence_document_fact(spark 3칸 전부, 가중치 1)가
# 텍스트 작업이 쪽을 쌓았다고 알리면, 그리고 일정마다 돌린다.
#
# 이 유닛은 로컬 모델 런타임(intelligence-model-runtime-ollama.service)을 Requires= 하므로 모델은 이 run 동안만 뜨고,
# 이 작업이 spark 풀을 다 쥐므로 모델이 Spark 옆에서 메모리를 쥐지 않는다(root ADR-0478 §2).
#
# 1. intelligence_documents.py plan-facts 가 텍스트 결과가 있고 이 사실 판의 결과가 없는 해시를 고르고, 그 결과·쪽을
#    run 폴더에 낸다. OCR 쪽이 있는 PDF 의 바이트만 받는다(비전 재독이 쪽 그림을 그린다).
# 2. intelligence 이미지의 fact_run 이 규칙 먼저, 규칙이 못 닫은 표만 모델에 묻는다. 엔진은 모른다: 관문
#    MODEL_RUNTIME_* 만 읽는다(model-runtime.contract.json 의 serving).
# 3. intelligence_documents.py append-facts 가 사실을 먼저, 결과를 나중에 쌓는다.
# 4. intelligence_documents.py fold-complex-facts 가 이 run 에 사실이 쌓인 첨부의 단지만, 패널이 말해도 되는 사실을
#    gold.complex_document_facts 에 단지마다 한 줄 쌓는다(관문을 넘은 종류만, root ADR-0477 §1·§5). 빚이 없는 run 도
#    접기는 돈다: 관문이나 사실 판이 바뀌었으면 그것이 전부 다시 접는 선언된 예외이고, 아니면 할 일 없이 끝난다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"
source "$(dirname "${BASH_SOURCE[0]}")/intelligence-document-runtime.sh"

STATE_ROOT="${FOUNDATION_INTELLIGENCE_DOCUMENTS_STATE_ROOT:-/data/foundation-platform/intelligence-documents}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
intelligence_document_run_dir fact
trap 'job_journal "${journal}" "intelligence document fact FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/media"' EXIT

intelligence_document_extraction_contract
intelligence_document_spark plan-facts --out "${container_work}/plan"
owed="$(intelligence_document_summary owed)"
planned="$(intelligence_document_summary planned)"
# Foundation's read of the facts for the complex panel; prints "<complexes> <appended>".
fold_complex_facts() {
  intelligence_document_spark fold-complex-facts --run-id "${run_id}"
  folded="$(intelligence_document_summary complexes) $(intelligence_document_summary appended)"
}

if [ "${planned}" = 0 ]; then
  fold_complex_facts
  job_journal "${journal}" "intelligence document fact: nothing owed, folded=${folded} run=${run_id}"
  if [ "${folded#* }" = True ]; then printf 'foundation-job-outcome changed\n'; else printf 'foundation-job-outcome unchanged\n'; fi
  exit 0
fi

intelligence_document_fetch_media "${work}/plan/media.tsv"
# Like the text job: a stop or a failed image still appends the documents it finished (the image
# writes each as it ends), then the unit ends failed; the fold waits for the next run.
stopping=0
trap 'stopping=1' TERM
fact_rc=0
intelligence_document_fact_run || fact_rc=$?
intelligence_document_spark append-facts --run-dir "${container_work}/out" --run-id "${run_id}"
trap - TERM
if [ "${stopping}" = 1 ] || [ "${fact_rc}" != 0 ]; then
  job_journal "${journal}" "intelligence document fact $([ "${stopping}" = 1 ] && echo stopped || echo "FAILED: the image exited ${fact_rc}"); its finished documents were appended run=${run_id}" >&2
  job_run_log_tail "${run_log}"
  exit "$([ "${stopping}" = 1 ] && echo 143 || echo "${fact_rc}")"
fi
fold_complex_facts

read_summary="$(python3 -I -c '
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({k: s.get(k) for k in ("planned", "model", "run_seconds", "counts", "dropped")}, sort_keys=True))
' "${work}/out/summary.json")"
job_journal "${journal}" "intelligence document fact ok run=${run_id} owed=${owed} planned=${planned} folded=${folded} ${read_summary}"
printf 'foundation-job-outcome changed\n'
