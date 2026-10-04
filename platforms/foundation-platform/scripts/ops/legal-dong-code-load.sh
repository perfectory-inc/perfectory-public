#!/usr/bin/env bash
# 법정동 코드 변경의 적재·짝 맞추기 절반 (root ADR-0143, ADR-0144). lineage-stewardship-cycle.sh 가 source 하고, 자기
# 단계 앞에서 load_legal_dong_code_handoffs 를 부른다. 그 단위의 spark 자리(3)와 spark 함수, 작업 폴더를
# 그대로 쓴다. 계보는 법정동 코드 변경의 소비자이므로 바로 앞에서 적재한다.
#
# 대기 넘김(수집 작업이 쓴 pending/<표 객체>/)마다, 오래된 것부터:
#   1. 스냅숏 — 전체 표를 reference.legal_dong_code_snapshot 에 쌓는다. 형식이 바뀌었거나 표가 그 표의 직전
#               code.go.kr 스냅숏보다 줄었으면 아무것도 쌓지 않고 실패한다. 성공하면 최신 스냅숏 표지를 바꾼다.
#   2. 짝     — 기록된 짝·스튜어드 짝 → 날짜·이름 규칙 → (켜졌으면) 지번 겹침 → (켜졌으면) 필지 번호 공식 이력
#               → 상위 단위 묶기로 짝을 맞춰 reference.legal_dong_code_change 와 reference.sigungu_canonical_crosswalk
#               에 쌓고, 허브 내보내기가 읽는 투영 파일과 스튜어드 목록을 바꾼다. 1 이 성공하고 2 가 실패하면
#               투영이 표지보다 낡아 내보내기가 거부한다.
#               지번 겹침은 개편 전후 필지 스냅숏 쌍(LEGAL_DONG_PARCELS_BEFORE/AFTER)이, 필지 번호 공식 이력은
#               LEGAL_DONG_PARCEL_LINEAGE_TABLE 이 주어질 때만 돈다. 지금은 둘 다 비어 있어 꺼진 채 출시한다
#               (ADR-0144); 그 동안 그런 동은 스튜어드 목록으로 간다.
#   3. 넘김을 loaded/ 로 옮긴다.
# 대기 넘김이 없어도 대기 중인 스튜어드 결정이 있으면 최신 스냅숏으로 짝 맞추기만 돈다. 둘 다 없으면 그렇게
# 남기고 지나간다. 어느 단계든 실패하면 함수가 0 이 아닌 값을 돌려주고, 호출한 단위가 계보로 넘어가지 않는다.

LEGAL_DONG_STATE_ROOT="${FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT:-/var/lib/foundation-platform/legal-dong-code}"

legal_dong_install() {
  cp "$1" "$2.tmp"
  mv -f "$2.tmp" "$2"
}

# Pairs one snapshot: $1 snapshot date, $2 its Bronze key.
legal_dong_pair() {
  local snapshot_date="$1" table_key="$2" out="${work}/legal-dong" cout="${container_work}/legal-dong"
  local decisions=() evidence=()
  if compgen -G "${LEGAL_DONG_STATE_ROOT}/steward/pending/*.json" >/dev/null; then
    mkdir -p "${out}/steward"
    cp "${LEGAL_DONG_STATE_ROOT}"/steward/pending/*.json "${out}/steward/"
    decisions=(--steward-decisions "${cout}/steward")
  fi
  if [ -n "${LEGAL_DONG_PARCELS_BEFORE:-}" ] && [ -n "${LEGAL_DONG_PARCELS_AFTER:-}" ]; then
    evidence+=(--parcels-before-snapshot-id "${LEGAL_DONG_PARCELS_BEFORE}" --parcels-after-snapshot-id "${LEGAL_DONG_PARCELS_AFTER}")
  fi
  if [ -n "${LEGAL_DONG_PARCEL_LINEAGE_TABLE:-}" ]; then
    evidence+=(--parcel-lineage-table "${LEGAL_DONG_PARCEL_LINEAGE_TABLE}")
  fi
  spark legal_dong_code_change_pairs.py --allow-non-smoke-write --snapshot-date "${snapshot_date}" \
    --table-source-record-id "${table_key}" ${evidence[@]+"${evidence[@]}"} ${decisions[@]+"${decisions[@]}"} \
    --projection-output "${cout}/projection.json" --review-output "${cout}/steward-review.json" \
    --summary-output "${cout}/pairs-summary.json"
  legal_dong_install "${out}/projection.json" "${LEGAL_DONG_STATE_ROOT}/sigungu-crosswalk.projection.json"
  legal_dong_install "${out}/steward-review.json" "${LEGAL_DONG_STATE_ROOT}/steward-review.json"
  # Each staged decision moves to recorded/ or rejected/, as the run judged it.
  python3 -I - "${out}/pairs-summary.json" "${LEGAL_DONG_STATE_ROOT}/steward" <<'PY'
import json, pathlib, sys
verdicts = json.load(open(sys.argv[1], encoding="utf-8")).get("steward_decisions", {})
root = pathlib.Path(sys.argv[2])
for name, verdict in verdicts.items():
    target = root / ("recorded" if verdict == "recorded" else "rejected")
    target.mkdir(parents=True, exist_ok=True)
    (root / "pending" / name).rename(target / name)
    print(f"steward decision {name}: {verdict}")
PY
}

load_legal_dong_code_handoffs() {
  local pending="${LEGAL_DONG_STATE_ROOT}/pending" loaded=0 name handoff snapshot_date table_key table_file
  mkdir -p "${work}/legal-dong" "${LEGAL_DONG_STATE_ROOT}/loaded"
  chmod 0777 "${work}/legal-dong"
  for handoff in $(find "${pending}" -mindepth 1 -maxdepth 1 -type d ! -name '.*' 2>/dev/null | sort); do
    name="$(basename "${handoff}")"
    cp -R "${handoff}" "${work}/legal-dong/${name}"
    chmod -R a+rwX "${work}/legal-dong/${name}" # Spark 컨테이너(uid 185)가 요약과 표지를 쓴다.
    read -r snapshot_date table_key table_file < <(python3 -I -c 'import json, sys
h = json.load(open(sys.argv[1] + "/handoff.json", encoding="utf-8"))
manifest = json.load(open(sys.argv[1] + "/manifest.json", encoding="utf-8"))
table = [o for o in manifest["objects"] if o["role"] == "full_table"][0]
print(h["snapshot_date"], h["table_object_key"], table["local_path"])' "${handoff}")
    spark legal_dong_code_snapshot_to_reference.py --allow-non-smoke-write --input-format code-go-kr-html \
      --input "${container_work}/legal-dong/${name}/${table_file}" --snapshot-date "${snapshot_date}" \
      --source-record-id "${table_key}" --summary-output "${container_work}/legal-dong/${name}/snapshot-summary.json" \
      --latest-marker-output "${container_work}/legal-dong/${name}/latest-legal-dong-snapshot.json"
    legal_dong_install "${work}/legal-dong/${name}/latest-legal-dong-snapshot.json" \
      "${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json"
    legal_dong_pair "${snapshot_date}" "${table_key}"
    mv "${handoff}" "${LEGAL_DONG_STATE_ROOT}/loaded/${name}"
    loaded=$((loaded + 1))
    printf '%s legal-dong-code loaded handoff=%s run=%s\n' "$(date -u +%FT%TZ)" "${name}" "${run_id}" >> "${journal}"
  done
  if [ "${loaded}" = 0 ]; then
    if compgen -G "${LEGAL_DONG_STATE_ROOT}/steward/pending/*.json" >/dev/null \
      && [ -f "${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json" ]; then
      read -r snapshot_date table_key < <(python3 -I -c 'import json, sys
m = json.load(open(sys.argv[1], encoding="utf-8")); print(m["snapshot_date"], m["source_record_id"])' \
        "${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json")
      legal_dong_pair "${snapshot_date}" "${table_key}"
      printf '%s legal-dong-code paired steward decisions on %s run=%s\n' "$(date -u +%FT%TZ)" "${snapshot_date}" "${run_id}" >> "${journal}"
    else
      printf '%s legal-dong-code no pending handoff run=%s\n' "$(date -u +%FT%TZ)" "${run_id}" >> "${journal}"
    fi
  fi
}
