#!/usr/bin/env bash
# 법원경매정보의 Silver 적재 절반 (root ADR-0280, ADR-0281). 등록된 작업 court_auction_silver(default_pool)가
# 매일 06:30 KST, 수집이 끝난 뒤에 돌린다.
#
# 엔진(root ADR-0559): 문서 전부(물건 검색·매각결과와 아래의 물건상세·공통코드·사건 문서·감정평가서·명세서·매각공고·
# 배당요구종기공고)는 발행기 load-courtauction-go-kr-silver 가 DuckDB 로 같은 계획(${work}/duckdb/*)을 읽어 쌓는다.
# 적재 끝에 두 검색 표의 법정동 변경 따라가기도 같은 발행기가 한다(root ADR-0329 §3). Spark 는 띄우지 않는다.
#
# 쌓을 것은 R2 만 보고 정한다. plan-silver-load-objects 가 검색마다 원천의 Bronze 접두어를 나열하고, 그 표의 커밋
# 요약이 이미 기록한 객체를 빼고, 남은 것을 오래된 것부터 계약의 범위(courtauction-go-kr.contract.json silver_load)
# 안에서 이 실행의 폴더로 받는다. 적재가 객체마다 커밋 하나로, 하나뿐인 해석기로 PNU 를 찾아
# (place_identity_stage) silver.court_auction_item 에 내용이 바뀐 물건과(물건 검색을 끝까지 읽은 밤에만) 빠진 물건의
# 삭제 표지를, silver.court_auction_sale_result 에 바뀐 행을 쌓는다. 물건상세 객체는 silver.court_auction_item_photo 에
# 새 사진(내용 해시)을, 문서 여섯 표(사건·명세서 요약·기일·목록·제시외건물·감정요항, root ADR-0540)에 바뀐 것을,
# 그다음 silver.court_auction_item_detail 에 읽은 기록을 쌓는다(root ADR-0384). 계획은 읽은 기록 표와 마지막 문서 표
# (silver.court_auction_appraisal_point) 둘 다 기준이다: 둘 중 하나라도 기록하지 않은 객체를 받고, 적재는 한 번만 읽는다.
# 그래서 문서 표가 새로 생기면 창 안의 물건상세 객체가 그 표에 처음 채워진다. 공통코드 객체는 silver.court_auction_code 에
# 이름이 바뀐 코드를 쌓는다(root ADR-0540 §5). 사건상세·현황조사·감정평가 객체는 문서 표(사건·관련사건·목록별 종국결과·
# 당사자 구분, 현황조사 회차와 목록, 감정평가 회차)의 바뀐 것을, 그다음 silver.court_auction_case_document_read 에 읽은 기록을
# 쌓는다(root ADR-0540 §1·§3): 계획은 문서마다 읽은 기록 표 기준이다. 문건·송달 객체는 사건마다 접수 문건·송달 판
# (silver.court_auction_case_filing·_case_delivery, 가린 이름 없이)을 같은 방식으로 쌓는다(root ADR-0575 §4·§5). 감정평가서 객체는 저장된 것을
# silver.court_auction_appraisal_report 에, 그다음 모든 요청(캡차 건너뜀 포함)을 silver.court_auction_appraisal_report_read 에
# 쌓는다: 계획은 읽은 기록 표 기준이다. 매각물건명세서 객체는 물은 판마다(캡차 건너뜀 포함) 한 줄을
# silver.court_auction_sale_specification_read 에 쌓는다(root ADR-0575): 계획은 그 표 기준이다. 쌓을 것이 없으면
# 아무것도 띄우지 않는다. 매각공고 객체는 공고마다 공고 행과 그 목록 판(매각물건·정정·취소변경)을 쌓는다(root ADR-0575 §6):
# 계획은 목록 표 기준이다.
#
# 대기열이 없으니 옮길 것도 없다. 실패하면 커밋하지 못한 객체는 표에 기록이 없어 다음 실행이 다시 받는다.
# 받은 사본은 성공하든 실패하든 지운다.
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

STATE_ROOT="${FOUNDATION_COURT_AUCTION_STATE_ROOT:-/var/lib/foundation-platform/court-auction}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/courtauction-go-kr.contract.json"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "court-auction silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/duckdb"' EXIT

read -r window_days max_objects item_slug result_slug detail_slug code_slug case_slug survey_slug appraisal_slug filing_slug tenant_slug report_slug spec_slug notice_slug dividend_slug < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"],
      "courtauctiongokr__" + c["item_search"]["dataset_slug"], "courtauctiongokr__" + c["sale_result_search"]["dataset_slug"],
      "courtauctiongokr__" + c["item_detail"]["dataset_slug"], "courtauctiongokr__" + c["common_codes"]["dataset_slug"],
      *("courtauctiongokr__" + d["dataset_slug"] for d in c["case_documents"]["documents"]),
      "courtauctiongokr__" + c["appraisal_reports"]["dataset_slug"],
      "courtauctiongokr__" + c["sale_specifications"]["dataset_slug"],
      "courtauctiongokr__" + c["sale_notices"]["dataset_slug"],
      "courtauctiongokr__" + c["dividend_notices"]["dataset_slug"])
' "${CONTRACT}")

plan() { # <table> <source slug> <dir>
  env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE="$1" FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="$2" \
    FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
    FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="$3" \
    "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
  python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "$3/plan.json"
}
owed_items="$(plan silver.court_auction_item "${item_slug}" "${work}/duckdb/items")"
owed_results="$(plan silver.court_auction_sale_result "${result_slug}" "${work}/duckdb/results")"
owed_details="$(plan silver.court_auction_item_detail "${detail_slug}" "${work}/duckdb/details")"
owed_documents="$(plan silver.court_auction_appraisal_point "${detail_slug}" "${work}/duckdb/documents")"
owed_codes="$(plan silver.court_auction_code "${code_slug}" "${work}/duckdb/codes")"
owed_cases="$(plan silver.court_auction_case_document_read "${case_slug}" "${work}/duckdb/cases")"
owed_surveys="$(plan silver.court_auction_case_document_read "${survey_slug}" "${work}/duckdb/surveys")"
owed_appraisals="$(plan silver.court_auction_case_document_read "${appraisal_slug}" "${work}/duckdb/appraisals")"
owed_filings="$(plan silver.court_auction_case_document_read "${filing_slug}" "${work}/duckdb/filings")"
owed_tenants="$(plan silver.court_auction_case_document_read "${tenant_slug}" "${work}/duckdb/tenants")"
owed_reports="$(plan silver.court_auction_appraisal_report_read "${report_slug}" "${work}/duckdb/reports")"
owed_specs="$(plan silver.court_auction_sale_specification_read "${spec_slug}" "${work}/duckdb/specs")"
owed_notices="$(plan silver.court_auction_sale_notice_item "${notice_slug}" "${work}/duckdb/notices")"
owed_dividends="$(plan silver.court_auction_dividend_object "${dividend_slug}" "${work}/duckdb/dividends")"
# Every document loads in DuckDB (root ADR-0559): 물건 검색, 매각결과, 공통코드, 감정평가서, 매각물건명세서, 배당요구종기공고,
# 매각공고, the five 사건 문서 and 물건상세.
owed_duckdb=$((owed_codes + owed_reports + owed_specs + owed_dividends + owed_notices + owed_cases + owed_surveys
  + owed_appraisals + owed_filings + owed_tenants + owed_details + owed_documents + owed_items + owed_results))
if [ "${owed_duckdb}" = 0 ]; then
  job_journal "${journal}" "court-auction silver load: nothing owed run=${run_id}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
# The load ends by following the 법정동 code changes on the two search tables (root ADR-0329 §3).
FOUNDATION_PLATFORM_COURT_AUCTION_PLAN_ROOT="${work}/duckdb" \
  "${PUBLISHER_BIN}" load-courtauction-go-kr-silver >> "${run_log}" 2>&1
summary="$(grep '^courtauction-go-kr-duckdb-summary-json ' "${run_log}" | tail -1)"
# The load's result checks, to the journal, where the op that ran this unit reads them as Dagster
# asset check evaluations (orchestration/dagster/load_checks.py).
grep '^{"__dagster_pipes_version"' "${run_log}" || true
# An object no load recorded for longer than the window means loads have been failing: it is loaded
# now, and the run fails so the gap raises an alert (the plan names it `late`).
late="$(python3 -I -c '
import json, sys
print(sum(len(json.load(open(p, encoding="utf-8")).get("selection", {}).get("late", [])) for p in sys.argv[1:]))
' "${work}"/duckdb/*/plan.json)"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "court-auction silver load LATE run=${run_id}: loaded ${late} object(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "court-auction silver load: ${late} Bronze object(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen" >&2
  exit 1
fi
job_journal "${journal}" "court-auction silver load ok run=${run_id} owed=${owed_duckdb} ${summary}"
printf 'foundation-job-outcome changed\n'
