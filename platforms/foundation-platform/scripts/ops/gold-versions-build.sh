#!/usr/bin/env bash
# 판을 쌓는 Gold 의 생산(root ADR-0564 §3). 등록된 작업이 입력이 움직이면, 그리고 대체 일정마다 돌린다.
#
#   gold-versions-build.sh <lane>       foundation-gold-versions@<lane>.service 가 돌린다
#
# 레인의 Gold 를 발행기 안의 DuckDB 가 계산하고, 발행기가 바뀐 키만 `_versions` 표에 한 번의 추가 커밋으로
# 붙인다. Spark 세션을 열지 않는다. 입력·판·코드가 기준 커밋의 것과 같으면 아무것도 쓰지 않고 `unchanged`.
# 레인과 그 명령: parcel-nearby → build-parcel-nearby-versions (root ADR-0465, ADR-0564 §9 차례 2).
# parcel-register-masters → build-parcel-register-masters-versions (root ADR-0450, ADR-0564 §6: 옛 표의 그림자).
# parcel-demolitions → build-parcel-demolitions-versions, parcel-energy → build-parcel-energy-versions
# (root ADR-0399·0398, 같은 그림자 길). parcel-sub-parcels → build-parcel-sub-parcels-versions (root ADR-0399),
# parcel-building-permits → build-parcel-building-permits-versions (root ADR-0451, 같은 길).
# parcel-boundary-served → build-parcel-boundary-served-versions (root ADR-0602: 필지 타일 굽기가 Spark 의
# gold.parcel_boundary_served 를 읽는 동안 그 표의 그림자 — 태그 없는 빌드라 가장 새 커밋과 그 시각의 입력을 따른다,
# 굽기에 넘기는 것도 원장에 더하는 것도 없다).
# parcel → build-parcel-panel-versions (root ADR-0096, ADR-0564 §9 차례 5: 필지 레인의 호스트 패널, 같은 그림자 길).
# parcel-ownership → build-parcel-ownership-versions (root ADR-0361, 같은 그림자 길).
# parcel-factories → build-parcel-factories-versions (root ADR-0449, 같은 그림자 길).
# parcel-public-auction → build-parcel-public-auction-versions (root ADR-0287·0340, 같은 그림자 길; 빌드 시각이 입력).
# parcel-court-auction → build-parcel-court-auction-versions (root ADR-0287·0353·0540, 같은 길; 빌드 시각이 입력).
# parcel-real-transaction → build-parcel-real-transaction-versions (root ADR-0350·0371, 같은 그림자 길: 거래는
# 주소로 놓이므로 종류 단계가 아닌 자기 단계 real_transaction_versions_stage 가 놓임이 넘지 않는 시군구 묶음마다 짓는다).
# unit-price → build-unit-price-versions (root ADR-0190·0204, ADR-0564 §9 차례 4: 호 공시가격 레인의 Gold, 같은 그림자 길).
# place-id → build-place-id-versions (root ADR-0113 §8·0481, 레인 밖 표 둘: 발행 ID 목록·ID↔코드 연결표,
# 옛 표의 마지막 발행이 읽은 입력으로; ADR-0564 §6-9).
# unit-official-price → build-unit-official-price-versions (root ADR-0317 §6: Spark 조합 unit_official_price.py 의
# 그림자 — 표는 이행이 쓰므로 아무것도 쓰지 않고, 판정이 가장 새 배치를 그 배치의 핀으로 다시 조합해 Spark 의 행과 맞춘다).
# complex-catalog → build-complex-catalog-versions (root ADR-0426: 산업단지 목록, 1,442행이라 시도 고리 없이 한 번에;
# 손으로 지은 옛 표라 태그가 없어 같은 길로 가장 새 커밋과 그 시각의 입력을 따른다).
# 그림자 커밋 뒤에는(그리고 그 머리의 판정이 끝까지 돌 때까지 다음 실행마다) 같은 유닛이 넘겨받기 판정
# (gold-takeover-verdict.sh)을 선언된 시도에서 돌려 Dagster 의 자산 검사 `takeover_equal` 로 알린다(journal 의
# Pipes 줄, orchestration/dagster/takeover_checks.py; ADR-0564 §6-3). 판정의 실패는 검사의 것이고 빌드의 커밋은 선다.
#
#   gold-versions-build.sh cleanup      the unit's ExecStopPost, through pinned-cleanup.sh: the release the
#                                       run started with sweeps the stages its stopped run left, however it
#                                       ended (duckdb-stage-orphans.sh --stopped; root ADR-0270 §3, ADR-0599).
#                                       The lane is the unit's instance, named by its RuntimeDirectory.
set -Eeuo pipefail
if [[ "${1:-}" == cleanup ]]; then
  [[ "$#" == 1 ]] || { echo "gold-versions-build: cleanup takes no arguments" >&2; exit 64; }
  runtime="${RUNTIME_DIRECTORY:-}"
  runtime="${runtime%%:*}"
  lane="${runtime##*/foundation-gold-versions@}"
  [[ -n "${runtime}" && "${lane}" != "${runtime}" ]] ||
    { echo "gold-versions-build: cleanup runs as foundation-gold-versions@<lane>'s ExecStopPost (its RuntimeDirectory names the lane)" >&2; exit 64; }
else
  lane="${1:?usage: gold-versions-build.sh <lane>|cleanup}"
fi
case "${lane}" in
  parcel-nearby) command=build-parcel-nearby-versions ;;
  parcel-register-masters) command=build-parcel-register-masters-versions ;;
  parcel-demolitions) command=build-parcel-demolitions-versions ;;
  parcel-energy) command=build-parcel-energy-versions ;;
  parcel-sub-parcels) command=build-parcel-sub-parcels-versions ;;
  parcel-building-permits) command=build-parcel-building-permits-versions ;;
  parcel-boundary-served) command=build-parcel-boundary-served-versions ;;
  parcel) command=build-parcel-panel-versions ;;
  parcel-ownership) command=build-parcel-ownership-versions ;;
  parcel-factories) command=build-parcel-factories-versions ;;
  parcel-public-auction) command=build-parcel-public-auction-versions ;;
  parcel-court-auction) command=build-parcel-court-auction-versions ;;
  parcel-real-transaction) command=build-parcel-real-transaction-versions ;;
  unit-price) command=build-unit-price-versions ;;
  place-id) command=build-place-id-versions ;;
  unit-official-price) command=build-unit-official-price-versions ;;
  complex-catalog) command=build-complex-catalog-versions ;;
  *) echo "gold-versions-build: ${lane} has no table of appended row versions the publisher builds" >&2; exit 64 ;;
esac
if [[ "${1}" == cleanup ]]; then
  exec bash "$(dirname "${BASH_SOURCE[0]}")/duckdb-stage-orphans.sh" \
    "${FOUNDATION_GOLD_VERSIONS_STATE_ROOT:-/var/lib/foundation-gold-versions}" "${lane}" --stopped
fi
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_GOLD_VERSIONS_STATE_ROOT:-/var/lib/foundation-gold-versions}/${lane}"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${STATE_ROOT}/runs/${run_id}"
run_log="${work}/run.log"
mkdir -p "${work}"
# The runs a disk keeps: the last 30 (each holds a summary and a log).
mapfile -t kept < <(ls -1d "${STATE_ROOT}"/runs/*/ 2>/dev/null)
((${#kept[@]} <= 30)) || rm -rf -- "${kept[@]:0:${#kept[@]}-30}"
# Before this run opens its DuckDB stage, the stages dead runs of any lane left go
# (duckdb-stage-orphans.sh says which are no one's): they hold the SSD's free space, which bounds
# every stage's spill (root ADR-0599).
# Each removal is a journal line; a cleanup that fails is one too, and the run goes on.
if orphans="$(bash "$(dirname "${BASH_SOURCE[0]}")/duckdb-stage-orphans.sh" "${STATE_ROOT%/*}" "${lane}" "${work}" 2>&1)"; then
  while IFS= read -r line; do
    job_journal "${journal}" "gold versions ${lane} ${line} run=${run_id}"
  done <<<"${orphans}"
else
  job_journal "${journal}" "gold versions ${lane} could not clear dead stages run=${run_id}: ${orphans##*$'\n'}"
fi

on_error() {
  job_journal "${journal}" "gold versions ${lane} FAILED at line $1 run=${run_id}" >&2
  job_run_log_tail "${run_log}"
}
trap 'on_error ${LINENO}' ERR

# The `takeover_equal` check of a shadow build (root ADR-0564 §6-3): after a commit, and on each later
# run until one verdict of that head has run to its end, the verdict runs here, with this unit's keys
# and pool, and its outcome is reported through the journal as the check its definition declares.
# It is called after the build's own journal line and before its outcome line, and its failure is
# only the check's: the build's commit stands, and its outcome line is the build's.
# Measured 2026-11-23 (run 20261123T171121Z, parcel-boundary-served, the widest table): the export
# of both sides took 26.5 of the 30 minutes this bound was, which left the comparison 3.5; a full
# build before it takes about 12 (20261122T230957Z, 16 시도), so 40 keep both inside the unit's hour.
TAKEOVER_SECONDS=2400
takeover_check() {
  local run_dir="$1" check state declared table sido status=0
  check="$(dirname "${BASH_SOURCE[0]}")/gold_takeover_check.py"
  state="${STATE_ROOT}/takeover-verdict.json"
  declared="$(python3 -I -B "${check}" declared "${run_dir}/summary.json" "${RELEASE_ROOT}/definitions" "${state}")" ||
    return 1
  [ -n "${declared}" ] || return 0
  # One verdict a run: the unit's hour holds the build and one verdict. A run that built two tables (the
  # place-id lane) judges the first line, which starts after the table judged last (a record a table).
  read -r table sido <<<"${declared}"
  timeout "${TAKEOVER_SECONDS}" bash "$(dirname "${BASH_SOURCE[0]}")/gold-takeover-verdict.sh" \
    "${table}" "${sido}" "${run_dir}/verdict" >> "${run_dir}/verdict.log" 2>&1 || status=$?
  python3 -I -B "${check}" report "${table}" "${sido}" "${status}" "${run_dir}/verdict" "${state}" || return 1
  job_journal "${journal}" "gold versions ${lane} takeover verdict ${table} sido=${sido} exit=${status} run=${run_id}"
  if [ "${status}" -ne 0 ]; then
    job_run_log_tail "${run_dir}/verdict.log"
  fi
  # The record stays (verdict.json, checks.json, sides.json); the 시도's two sides and the spill go.
  rm -rf -- "${run_dir}/verdict/reference.parquet" "${run_dir}/verdict/candidate.parquet" "${run_dir}/verdict/spill"
}

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
takeover_check "${work}" ||
  job_journal "${journal}" "gold versions ${lane} takeover verdict could not be reported run=${run_id}"
# Every scheduled job's last line (root ADR-0171): a commit is `changed`, even one of no row (its
# new pins are what the next run plans from and what the bake reads).
if [ "${outcome}" = committed ]; then
  printf 'foundation-job-outcome changed\n'
else
  printf 'foundation-job-outcome unchanged\n'
fi
