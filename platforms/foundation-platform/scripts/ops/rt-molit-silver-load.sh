#!/usr/bin/env bash
# 국토교통부 실거래가의 Silver 적재 절반 (root ADR-0348, ADR-0349). 등록된 작업 rt_molit_real_transaction_silver
# (duckdb 1칸)가 매시간 돌린다.
#
# 엔진(root ADR-0559): 발행기 load-rt-molit-real-transaction-silver 가 DuckDB 로 계획(${work}/duckdb/real_transaction)을
# 읽어 쌓는다. Spark 는 띄우지 않는다.
#
# 쌓을 것은 R2 만 보고 정한다(plan-silver-load-objects: 12 원천의 Bronze 중 silver.real_transaction 의 커밋이
# 아직 기록하지 않은 것, 계약의 window_days·max_objects 안, 내려앉은 순). 적재가 객체(한 데이터셋의 한 달 전국 CSV)마다
# 커밋 하나로, 싣지 않는 줄을 silver.real_transaction_rejected 에 먼저(root ADR-0364), 그 달의 새 이름·바뀐 행과 빠진
# 이름의 삭제 표지를 silver.real_transaction 에 쌓는다. 계약해제는 같은 이름의 새 판이다. 법정동·PNU 는 정본 해석기의
# 규칙으로 정한다(root ADR-0288).
#
# 쌓을 것이 없으면 아무것도 띄우지 않고 unchanged 로 끝난다. 받은 사본은 성공하든 실패하든 지운다. 늦은
# 객체(window_days 보다 오래 기록 없이 남은 것)나 연결률이 계약의 하한 아래인 객체는 싣고 실패로 끝난다.
set -Eeuo pipefail
shopt -s inherit_errexit
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

STATE_ROOT="${FOUNDATION_REAL_TRANSACTION_STATE_ROOT:-/var/lib/foundation-platform/real-transaction}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
CONTRACT="${RELEASE_ROOT}/infra/lakehouse/contracts/rt-molit-real-transaction.contract.json"
journal="${STATE_ROOT}/journal.log"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
mkdir -p "${work}"
run_log="${work}/run.log"
trap 'job_journal "${journal}" "rt.molit silver load FAILED at line ${LINENO} run=${run_id}" >&2; job_run_log_tail "${run_log}"' ERR
trap 'rm -rf "${work}/duckdb"' EXIT

# Root ADR-0364: the exports this run quarantined whole, as a node-exporter textfile; the alert
# FoundationRealTransactionExportQuarantined fires on a run that quarantined one and clears on the next.
METRICS_DIR="/var/lib/foundation-jobs/metrics"
write_metrics() { # <quarantined exports> <rejected lines>
  local tmp="${METRICS_DIR}/.rt-molit-silver.prom.$$"
  {
    printf '# HELP foundation_rt_molit_quarantined_exports Exports the last 실거래가 Silver load quarantined whole.\n'
    printf '# TYPE foundation_rt_molit_quarantined_exports gauge\n'
    printf 'foundation_rt_molit_quarantined_exports %s\n' "$1"
    printf '# HELP foundation_rt_molit_rejected_lines Lines the last 실거래가 Silver load rejected.\n'
    printf '# TYPE foundation_rt_molit_rejected_lines gauge\n'
    printf 'foundation_rt_molit_rejected_lines %s\n' "$2"
    printf '# HELP foundation_rt_molit_silver_written_timestamp_seconds When the 실거래가 Silver load last wrote these.\n'
    printf '# TYPE foundation_rt_molit_silver_written_timestamp_seconds gauge\n'
    printf 'foundation_rt_molit_silver_written_timestamp_seconds %s\n' "$(date -u +%s)"
  } > "${tmp}" && mv -f "${tmp}" "${METRICS_DIR}/rt-molit-silver.prom" || true # a metrics write never fails the load
}

read -r window_days max_objects slugs < <(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1], encoding="utf-8"))
print(c["silver_load"]["window_days"], c["silver_load"]["max_objects"],
      ",".join("rtmolitgokr__" + name for name in sorted(c["datasets"])))
' "${CONTRACT}")
table=silver.real_transaction

plan_dir="${work}/duckdb/real_transaction"
env FOUNDATION_PLATFORM_SILVER_LOAD_TABLE="${table}" FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS="${slugs}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS="${window_days}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_MAX_OBJECTS="${max_objects}" \
  FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR="${plan_dir}" \
  "${PUBLISHER_BIN}" plan-silver-load-objects >> "${run_log}" 2>&1
owed="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["objects"]))' "${plan_dir}/plan.json")"
if [ "${owed}" = 0 ]; then
  job_journal "${journal}" "rt.molit silver load: nothing owed run=${run_id}"
  write_metrics 0 0
  printf 'foundation-job-outcome unchanged\n'
  exit 0
fi
FOUNDATION_PLATFORM_RT_MOLIT_PLAN_ROOT="${work}/duckdb" \
  "${PUBLISHER_BIN}" load-rt-molit-real-transaction-silver >> "${run_log}" 2>&1

# The load's result checks, to the journal, where the op that ran this unit reads them as Dagster
# asset check evaluations (orchestration/dagster/load_checks.py).
grep '^{"__dagster_pipes_version"' "${run_log}" || true
summary="$(grep '^rt-molit-real-transaction-summary-json ' "${run_log}" | tail -1)"
read -r quarantined rejected < <(python3 -I -c '
import json, sys
s = json.loads(sys.argv[1].split(" ", 1)[1])
print(s.get("quarantined_exports", 0), s.get("rejected", 0))
' "${summary}")
write_metrics "${quarantined}" "${rejected}"
late="$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1], encoding="utf-8"))["selection"].get("late", [])))' "${plan_dir}/plan.json")"
if [ "${late}" != 0 ]; then
  job_journal "${journal}" "rt.molit silver load LATE run=${run_id}: loaded ${late} object(s) older than ${window_days} days that no load had recorded ${summary}" >&2
  echo "rt.molit silver load: ${late} Bronze object(s) waited more than ${window_days} days unloaded; they are loaded now, and this run fails so the gap is seen (run.log names them)" >&2
  exit 1
fi
job_journal "${journal}" "rt.molit silver load ok run=${run_id} owed=${owed} ${summary}"
printf 'foundation-job-outcome changed\n'
