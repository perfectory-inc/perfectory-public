#!/usr/bin/env bash
# 판을 쌓는 Gold 의 생산(root ADR-0564 §3). 등록된 작업이 입력이 움직이면, 그리고 대체 일정마다 돌린다.
#
#   gold-versions-build.sh <lane>       foundation-gold-versions@<lane>.service 가 돌린다
#
# 레인의 Gold 를 발행기 안의 DuckDB 가 계산하고, 발행기가 바뀐 키만 `_versions` 표에 한 번의 추가 커밋으로
# 붙인다. Spark 세션을 열지 않는다. 입력·판·코드가 기준 커밋의 것과 같으면 아무것도 쓰지 않고 `unchanged`.
# 레인과 그 명령: parcel-nearby → build-parcel-nearby-versions (root ADR-0465, ADR-0564 §9 차례 2).
set -Eeuo pipefail
lane="${1:?usage: gold-versions-build.sh <lane>}"
case "${lane}" in
  parcel-nearby) command=build-parcel-nearby-versions ;;
  *) echo "gold-versions-build: ${lane} has no table of appended row versions the publisher builds" >&2; exit 64 ;;
esac
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_GOLD_VERSIONS_STATE_ROOT:-/var/lib/foundation-gold-versions}/${lane}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
run_log="${work}/run.log"
mkdir -p "${work}"
# The runs a disk keeps: the last 30 (each holds a summary and a log; DuckDB's stage directory is
# removed by the publisher when it ends).
ls -1d "${STATE_ROOT}"/runs/*/ 2>/dev/null | head -n -30 | xargs -r rm -rf --

on_error() {
  job_journal "${journal}" "gold versions ${lane} FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY="${work}/summary.json" \
  "${PUBLISHER_BIN}" "${command}" >> "${run_log}" 2>&1

# "<outcome> <version_seq or -> <keys changed or 0> <seconds>"
read -r outcome sequence keys seconds < <(python3 -I -c '
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
diff = s.get("diff") or {}
print(s["outcome"], s.get("version_seq", "-"), sum(diff.get(k, 0) for k in ("added", "changed", "removed")), int(s["seconds"]))
' "${work}/summary.json")
# The publisher's own last line: what the cut-over's gold step quotes.
tail -n 1 "${run_log}"
job_journal "${journal}" "gold versions ${lane} ${outcome} version_seq=${sequence} keys_changed=${keys} seconds=${seconds} run=${run_id}"
# Every scheduled job's last line (root ADR-0171): a commit is `changed`, even one of no row (its
# new pins are what the next run plans from and what the bake reads).
if [ "${outcome}" = committed ]; then
  printf 'foundation-job-outcome changed\n'
else
  printf 'foundation-job-outcome unchanged\n'
fi
