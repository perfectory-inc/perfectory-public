#!/usr/bin/env bash
# 법정동 코드 변경을 code.go.kr 에서 받는다 — 수집 절반 (root ADR-0143, ADR-0144). 등록된
# 작업(ADR-0122, default_pool)이 하루 한 번 돌린다. Spark 는 쓰지 않는다.
#
#   legal-dong-code-collect.sh               매일 수집
#   legal-dong-code-collect.sh steward --steward <id> --reason <why> --approve OLD=NEW [--approve OLD=NEW]...
#                                            스튜어드 결정 한 건을 파일로 남긴다(다음 짝 맞추기가 반영)
#
# 매일 수집:
#   1. 표 — 법정동 전체 표(코드·상위코드·생성일·폐지일)를 받아 Bronze 에 남긴다(collect-code-go-kr-legal-dong).
#           원천은 내려받은 데이터뿐이다. 코드변경안내 게시판은 읽지 않는다(ADR-0144).
#   2. 검사 — 표의 형식이 계약과 같은지, 직전에 넘긴 표보다 계약의 한계 넘게 줄지 않았는지 본다
#             (code_go_kr_legal_dong.py stage-handoff). 표의 행이 바뀌었을 때만 적재 대기 넘김
#             (pending/<표 객체>/)을 쓴다. 검사를 통과하면 바뀐 것이 없어도 확인 시각(accepted.json
#             checked_at_utc)을 남긴다: 허브 내보내기는 계약의 projection.max_age_days 보다 오래 확인되지
#             않은 대응표를 거부한다.
#
# 적재와 짝 맞추기는 lineage_stewardship 단위가 자기 단계 앞에서 한다(legal-dong-code-load.sh). 어느
# 단계든 실패하면 0 이 아닌 값으로 끝나고, Airflow 실패 알림이 슬랙에 간다(ADR-0122).
# -E: 함수·명령 치환 안에서 실패해도 ERR trap 이 journal 에 실패를 남긴다.
set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

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
  printf '%s legal-dong-code steward decision staged\n' "$(date -u +%FT%TZ)" >> "${journal}"
  exit 0
fi

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 다른 등록 작업과 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  : "${FOUNDATION_ADMIN_PASSWORD:?recovery.env must provide FOUNDATION_ADMIN_PASSWORD}"
  DATABASE_URL="$(python3 - <<PY
import os, urllib.parse
q = lambda s: urllib.parse.quote(s, safe=str())
port = os.environ.get("FOUNDATION_DB_PORT", "15434")
print("postgres://foundation_admin:" + q(os.environ["FOUNDATION_ADMIN_PASSWORD"])
      + "@127.0.0.1:" + port + "/foundation")
PY
)"
  export DATABASE_URL
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}/collect"
run_log="${work}/run.log"
trap 'printf "%s legal-dong-code collect FAILED at line %s run=%s\n" "$(date -u +%FT%TZ)" "${LINENO}" "${run_id}" >> "${journal}"; tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true' ERR

# 1. 표
env "${COLLECT}_OUTPUT_DIR=${work}/collect" "${COLLECT}_LIVE_WRITE=1" \
  "${PUBLISHER_BIN}" collect-code-go-kr-legal-dong >> "${run_log}" 2>&1

# 2. 검사와 넘김
result="$("${PY[@]}" "${JOBS}/code_go_kr_legal_dong.py" stage-handoff --collect-dir "${work}/collect" \
  --state-dir "${STATE_ROOT}" | tee -a "${run_log}")"
printf '%s legal-dong-code collect ok run=%s %s\n' "$(date -u +%FT%TZ)" "${run_id}" "${result}" >> "${journal}"
echo "${result}"
