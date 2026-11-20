#!/usr/bin/env bash
# 법정동 코드 짝 맞추기를 운영 데이터로 한 번 돌려 보고 아무것도 쓰지 않는다 (root ADR-0144, ADR-0148).
#
# 최신 code.go.kr 스냅숏(latest-legal-dong-snapshot.json), 변경표에 기록된 짝, 폐지마다 그 날짜를 감싸는 필지 판
# 둘(silver.parcel_boundaries)을 정기 실행과 똑같이 읽고, `--validate-only` 로 판단만 한다. 레이크하우스에는 쓰지
# 않는다. 결과는 실행 폴더의 세 파일이다: summary.json(쌓였을 짝 `would_append`, 증거별 개수), steward-review.json,
# projection.json. 스튜어드 결정 대기 파일은 읽기만 하고 옮기지 않는다.
#
# 규칙은 lineage_stewardship 과 같은 계산 컨테이너에서 돌므로 그 단위가 도는 동안에는 돌리지 않는다. 런북:
# docs/runbooks/legal-dong-code-changes.md 의 "운영 데이터로 미리 보기".
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current

STATE_ROOT="${FOUNDATION_LINEAGE_STEWARDSHIP_STATE_ROOT:-/var/lib/foundation-platform/lineage-stewardship}"
LAKEHOUSE_STATE_ROOT="${STATE_ROOT}/lakehouse"
LEGAL_DONG_STATE_ROOT="${FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT:-/var/lib/foundation-platform/legal-dong-code}"
PARCEL_NUMBER_CHANGE_STATE_ROOT="${FOUNDATION_PARCEL_NUMBER_CHANGE_STATE_ROOT:-/var/lib/foundation-platform/parcel-number-change}"

run_id="validate-$(date -u +%Y%m%dT%H%M%SZ)"
work="${LAKEHOUSE_STATE_ROOT}/runs/${run_id}"
container_work="/workspace/target/lakehouse/runs/${run_id}"
mkdir -p "${work}"
chmod 0777 "${work}" # 규칙은 계산 컨테이너(uid 185)에서 돌며 여기에 쓴다.

marker="${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json"
read -r snapshot_date table_key < <(python3 -I -c 'import json, sys
m = json.load(open(sys.argv[1], encoding="utf-8"))
print(m["snapshot_date"], m["source_record_id"])' "${marker}")

# The official history, as the scheduled run reads it: once one of its handoffs was loaded (root ADR-0150).
official=()
official_history=0
if compgen -G "${PARCEL_NUMBER_CHANGE_STATE_ROOT}/loaded/*" >/dev/null; then
  official=(--official-history-table silver.parcel_number_change_history)
  official_history=1
fi

decisions=()
if compgen -G "${LEGAL_DONG_STATE_ROOT}/steward/pending/*.json" >/dev/null; then
  mkdir -p "${work}/steward"
  cp "${LEGAL_DONG_STATE_ROOT}"/steward/pending/*.json "${work}/steward/"
  chmod -R a+rX "${work}/steward"
  decisions=(--steward-decisions "${container_work}/steward")
fi

# 정기 실행과 같은 읽기와 같은 규칙이다(legal-dong-code-load.sh 의 함수 그대로): 쌓기만 하지 않는다.
run_log="${work}/run.log"
publisher() {
  "${PUBLISHER_BIN}" "$@"
}
pairing_rules() {
  lakehouse_run_log "${run_log}"
  FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
  FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
  lakehouse_spark_run foundation-platform-compute \
    spark python3 -E -s /workspace/infra/lakehouse/spark/jobs/legal_dong_code_change_pairs.py "$@" >> "${run_log}" 2>&1
}
source "$(dirname "${BASH_SOURCE[0]}")/legal-dong-code-load.sh"
status=0
{ legal_dong_pairing_inputs "${snapshot_date}" "${table_key}" "${official_history}" \
    && pairing_rules --validate-only --snapshot-date "${snapshot_date}" --table-source-record-id "${table_key}" \
      --jibun-evidence editions ${official[@]+"${official[@]}"} ${decisions[@]+"${decisions[@]}"} \
      --inputs "${container_work}/legal-dong/pairing" \
      --projection-output "${container_work}/projection.json" --review-output "${container_work}/steward-review.json" \
      --summary-output "${container_work}/summary.json"; } || status=$?
rm -rf "${work}/legal-dong/pairing" # 필지 파일은 GB 단위다.
[ "${status}" = 0 ] || { tail -n 20 "${run_log}" >&2; exit "${status}"; }

printf 'legal-dong-code validate-only: %s\n' "${work}"
python3 -I - "${work}/summary.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({k: s[k] for k in ("status", "reads", "jibun_evidence", "official_parcel_links",
                                     "non_polygon_parcels_skipped", "pairs_by_evidence", "fresh_changes",
                                     "awaiting_by_sido", "awaiting_by_evidence", "official_partial_moves",
                                     "official_off_window_decisive",
                                     "review_by_status", "crosswalk_entries", "governed_sido")}, ensure_ascii=False))
for row in s["would_append"]:
    print(f"would append {row['level']:<12} {row['old_code']} -> {row['new_code']}  {row['source']}  {row['detail'] or ''}")
PY
