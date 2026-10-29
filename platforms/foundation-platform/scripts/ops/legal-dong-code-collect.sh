#!/usr/bin/env bash
# 법정동 코드 변경을 code.go.kr 에서 받는다 — 수집 절반 (root ADR-0143, ADR-0144). 등록된
# 작업(ADR-0122, default_pool)이 하루 한 번 돌린다. Spark 는 쓰지 않는다.
#
#   legal-dong-code-collect.sh               매일 수집
#   legal-dong-code-collect.sh steward --steward <id> --reason <why> --approve OLD=NEW [--approve OLD=NEW]...
#                                            스튜어드 결정 한 건을 파일로 남긴다(다음 짝 맞추기가 반영)
#
# 매일 수집:
#   0. 물어보기 — 표 전체(58 MB) 앞에서, 사이트의 변경기간 검색으로 since 이후 생성·폐지된 코드만 받는다
#             (약 70 KB, root ADR-0322 §2). 직전 전체 실행이 고정한 창(accepted.json probe)과 같은 행이고
#             전체 대조가 밀리지 않았으면(full_reconcile_days) 확인 시각만 남기고 unchanged 로 끝난다.
#             아니면 아래 1–3 을 그대로 하고, 이 물어보기를 다음 실행들의 창으로 고정한다.
#   1. 받기 — 법정동 전체 표(코드·상위코드·생성일·폐지일)를 받기만 한다(collect-code-go-kr-legal-dong 의
#             dry run: Bronze 에 쓰지 않는다). 원천은 내려받은 데이터뿐이다. 코드변경안내 게시판은 읽지 않는다(ADR-0144).
#   2. 검사 — 표의 형식이 계약과 같은지, 직전에 넘긴 표보다 계약의 한계 넘게 줄지 않았는지, 행이 바뀌었는지
#             본다(code_go_kr_legal_dong.py check). 검사를 통과하면 바뀐 것이 없어도 확인 시각(accepted.json
#             checked_at_utc)을 남긴다: 허브 내보내기는 계약의 projection.max_age_days 보다 오래 확인되지
#             않은 대응표를 거부한다. 행이 같으면 아무것도 남기지 않고 unchanged 로 끝난다. 받은 사본마다
#             시각이 달라 sha256 이 매번 다르므로(같은 표 283 벌, 15 GB, 2026-10-04~28) 비교는 쓰기 전, 행으로 한다.
#   3. 남기기 — 행이 바뀐 표만 다시 받아 Bronze 에 남기고(live write), 적재 대기 넘김(pending/<표 객체>/)을
#             쓴다(stage-handoff).
#
# 적재와 짝 맞추기는 lineage_stewardship 단위가 자기 단계 앞에서 한다(legal-dong-code-load.sh). 어느
# 단계든 실패하면 0 이 아닌 값으로 끝나고, Airflow 실패 알림이 슬랙에 간다(ADR-0122).
# -E: 함수·명령 치환 안에서 실패해도 ERR trap 이 journal 에 실패를 남긴다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT:-/var/lib/foundation-platform/legal-dong-code}"
JOBS="${RELEASE_ROOT}/infra/lakehouse/spark/jobs"
COLLECT="FOUNDATION_PLATFORM_CODE_GO_KR_LEGAL_DONG"
# -E -s, not -I: the jobs import their sibling modules (legal_dong_code_change_pairs.py imports
# code_go_kr_legal_dong), and -I drops the script's own directory from sys.path. -E and -s still
# shut out PYTHONPATH and the user site, which is the isolation that matters here.
PY=(python3 -E -s)
journal="${STATE_ROOT}/journal.log"
mkdir -p "${STATE_ROOT}"

if [ "${1:-}" = steward ]; then
  shift
  "${PY[@]}" "${JOBS}/legal_dong_code_change_pairs.py" stage-steward-decision \
    --review "${STATE_ROOT}/steward-review.json" --output-dir "${STATE_ROOT}/steward/pending" "$@"
  job_journal "${journal}" "legal-dong-code steward decision staged"
  exit 0
fi

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 다른 등록 작업과 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/probe" "${work}/fetch" "${work}/collect"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "legal-dong-code collect FAILED at line ${LINENO} run=${run_id}" >&2; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true; job_run_log_tail "${run_log}"' ERR

# The status in a `legal-dong-code-stage-json` line (root ADR-0171, ADR-0260).
stage_status() {
  "${PY[@]}" -c '
import json, sys
prefix = "legal-dong-code-stage-json "
print(next(json.loads(line[len(prefix):])["status"] for line in sys.stdin if line.startswith(prefix)))
' <<<"$1"
}

# 0. 물어보기 (root ADR-0322 §2: the site's own changed-since filter, about 70 KB; never stored)
since="$("${PY[@]}" "${JOBS}/code_go_kr_legal_dong.py" probe-window --state-dir "${STATE_ROOT}")"
env "${COLLECT}_OUTPUT_DIR=${work}/probe" "${COLLECT}_CHANGED_SINCE=${since}" \
  "${PUBLISHER_BIN}" collect-code-go-kr-legal-dong >> "${run_log}" 2>&1
result="$("${PY[@]}" "${JOBS}/code_go_kr_legal_dong.py" probe --probe-dir "${work}/probe" \
  --state-dir "${STATE_ROOT}" | tee -a "${run_log}")"
if [ "$(stage_status "${result}")" = unchanged ]; then
  rm -rf "${work}/probe"
  job_journal "${journal}" "legal-dong-code collect ok run=${run_id} ${result}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

# 1. 받기 (dry run: nothing is stored)
env "${COLLECT}_OUTPUT_DIR=${work}/fetch" \
  "${PUBLISHER_BIN}" collect-code-go-kr-legal-dong >> "${run_log}" 2>&1

# 2. 검사 (a confirmed table anchors this run's probe as the next runs' window)
result="$("${PY[@]}" "${JOBS}/code_go_kr_legal_dong.py" check --collect-dir "${work}/fetch" \
  --state-dir "${STATE_ROOT}" --probe-dir "${work}/probe" | tee -a "${run_log}")"
# The dry-run copy is scratch once checked; a run that failed the check keeps it for the diagnosis.
rm -rf "${work}/fetch"
if [ "$(stage_status "${result}")" = unchanged ]; then
  rm -rf "${work}/probe"
  job_journal "${journal}" "legal-dong-code collect ok run=${run_id} ${result}"
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi

# 3. 남기기와 넘김. shortcut: the changed table is fetched a second time to commit it (two requests,
# only on the rare day the table changes); a collector that commits the dry-run copy would save it.
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-code-go-kr-legal-dong >> "${run_log}" 2>&1
result="$("${PY[@]}" "${JOBS}/code_go_kr_legal_dong.py" stage-handoff --collect-dir "${work}/collect" \
  --state-dir "${STATE_ROOT}" --probe-dir "${work}/probe" | tee -a "${run_log}")"
rm -rf "${work}/probe"
job_journal "${journal}" "legal-dong-code collect ok run=${run_id} ${result}"
# The run's last line says whether it handed anything new to the load (root ADR-0171, ADR-0260).
if [ "$(stage_status "${result}")" = staged ]; then
  printf 'foundation-job-outcome changed\n'
else
  printf 'foundation-job-outcome unchanged\n'
fi
