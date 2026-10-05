#!/usr/bin/env bash
# 법정동 코드 짝 맞추기를 운영 데이터로 한 번 돌려 보고 아무것도 쓰지 않는다 (root ADR-0144, ADR-0148).
#
# 최신 code.go.kr 스냅숏(latest-legal-dong-snapshot.json), 변경표에 기록된 짝, 폐지마다 그 날짜를 감싸는 필지 판
# 둘(silver.parcel_boundaries)을 정기 실행과 똑같이 읽고, `--validate-only` 로 판단만 한다. 레이크하우스에는 쓰지
# 않는다. 결과는 실행 폴더의 세 파일이다: summary.json(쌓였을 짝 `would_append`, 증거별 개수), steward-review.json,
# projection.json. 스튜어드 결정 대기 파일은 읽기만 하고 옮기지 않는다.
#
# lineage_stewardship 과 같은 spark 자리를 쓰므로 그 단위가 도는 동안에는 돌리지 않는다. 런북:
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
chmod 0777 "${work}" # Spark 컨테이너는 uid 185 로 쓴다.

marker="${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json"
read -r snapshot_date table_key < <(python3 -I -c 'import json, sys
m = json.load(open(sys.argv[1], encoding="utf-8"))
print(m["snapshot_date"], m["source_record_id"])' "${marker}")

# The official history, as the scheduled run reads it: once one of its handoffs was loaded (root ADR-0150).
official=()
if compgen -G "${PARCEL_NUMBER_CHANGE_STATE_ROOT}/loaded/*" >/dev/null; then
  official=(--official-history-table silver.parcel_number_change_history)
fi

decisions=()
if compgen -G "${LEGAL_DONG_STATE_ROOT}/steward/pending/*.json" >/dev/null; then
  mkdir -p "${work}/steward"
  cp "${LEGAL_DONG_STATE_ROOT}"/steward/pending/*.json "${work}/steward/"
  chmod -R a+rX "${work}/steward"
  decisions=(--steward-decisions "${container_work}/steward")
fi

docker rm foundation-platform-lakehouse-target-init >/dev/null 2>&1 || true
FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT="${LAKEHOUSE_STATE_ROOT}" \
FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro \
docker compose --project-directory "${RELEASE_ROOT}" -f "${RELEASE_ROOT}/compose.lakehouse.yml" \
  -p foundation-platform-compute --profile lakehouse-batch run --rm \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI -e FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN -e FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  spark spark-submit --master 'local[4]' --driver-memory 4g --jars "${SPARK_RELEASE_JARS}" \
  /workspace/infra/lakehouse/spark/jobs/legal_dong_code_change_pairs.py --validate-only \
  --snapshot-date "${snapshot_date}" --table-source-record-id "${table_key}" --jibun-evidence editions \
  ${official[@]+"${official[@]}"} ${decisions[@]+"${decisions[@]}"} \
  --projection-output "${container_work}/projection.json" --review-output "${container_work}/steward-review.json" \
  --summary-output "${container_work}/summary.json" > "${work}/run.log" 2>&1

printf 'legal-dong-code validate-only: %s\n' "${work}"
python3 -I - "${work}/summary.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1], encoding="utf-8"))
print(json.dumps({k: s[k] for k in ("status", "reads", "jibun_evidence", "official_parcel_links",
                                     "non_polygon_parcels_skipped", "pairs_by_evidence", "fresh_changes",
                                     "awaiting_by_sido", "awaiting_by_evidence",
                                     "review_by_status", "crosswalk_entries", "governed_sido")}, ensure_ascii=False))
for row in s["would_append"]:
    print(f"would append {row['level']:<12} {row['old_code']} -> {row['new_code']}  {row['source']}  {row['detail'] or ''}")
PY
