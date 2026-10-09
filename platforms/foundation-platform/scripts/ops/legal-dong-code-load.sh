#!/usr/bin/env bash
# 법정동 코드 변경의 적재·짝 맞추기 절반 (root ADR-0143, ADR-0144). lineage-stewardship-cycle.sh 가 source 하고, 자기
# 단계 앞에서 load_legal_dong_code_handoffs 를 부른다. 그 단위의 spark 자리(3)와 spark 함수, 작업 폴더를
# 그대로 쓴다. 계보는 법정동 코드 변경의 소비자이므로 바로 앞에서 적재한다.
#
# 대기 넘김(수집 작업이 쓴 pending/<표 객체>/)마다, 오래된 것부터:
#   1. 스냅숏 — 전체 표를 reference.legal_dong_code_snapshot 에 쌓는다. 형식이 바뀌었거나 표가 그 표의 직전
#               code.go.kr 스냅숏보다 줄었으면 아무것도 쌓지 않고 실패한다. 성공하면 최신 스냅숏 표지를 바꾼다.
#   2. 짝     — 기록된 짝·스튜어드 짝 → 날짜·이름 규칙 → (켜졌으면) 지번 겹침 → 상위 단위 묶기로 짝을 맞춰
#               reference.legal_dong_code_change 하나에만 쌓고(ADR-0145), 그 표의 시군구 view 인 투영 파일과
#               스튜어드 목록을 바꾼다. 1 이 성공하고 2 가 실패하면 투영이 표지보다 낡아 내보내기가 거부한다.
#               지번 겹침은 폐지된 동마다 그 날짜를 감싸는 필지 판 둘(원천 계약 vworld-parcel-source-objects.json
#               의 판 중 폐지 전에 뽑은 마지막 것과 뒤에 뽑은 첫 것)로 돈다. 계약에 그 판이 없거나 표에 적재되지
#               않았으면 그 동은 판단 대기로 남고, 필요한 판을 이름으로 말한다(ADR-0148).
#               필지 번호 공식 이력(필지고유번호변동연혁, VWorld 30527)은 silver.parcel_number_change_history 가
#               행을 가졌을 때(그 넘김을 하나라도 적재했을 때) 읽는다: 짝 맞추기 기간의 공식 짝이 지번 단계보다
#               먼저 정하고, 둘이 다르게 정하면 아무것도 쓰지 않고 멈춘다(ADR-0150). 표가 비었으면 그 단계는 꺼져
#               있고, 그것만 정할 수 있는 동은 판단 대기(awaiting_data)로 남는다(ADR-0144 §4). 필지 계보는 증거가
#               아니다: 계보가 이 표의 동 짝을 읽으므로, 거꾸로 읽으면 서로가 서로를 읽는다(ADR-0145 §2).
#   3. 넘김을 loaded/ 로 옮긴다.
# 그 앞에 필지고유번호변동연혁 수집 작업(parcel-number-change-collect.sh)이 쓴 대기 넘김을 먼저
# silver.parcel_number_change_history 에 쌓는다(파일 하나가 한 번의 적재). 새 공식 이력이 들어왔는데 법정동 표
# 넘김이 없으면 최신 스냅숏으로 짝 맞추기만 다시 돈다: 판단 대기였던 동을 이력이 정할 수 있다.
# 대기 넘김이 없어도 대기 중인 스튜어드 결정이 있으면 최신 스냅숏으로 짝 맞추기만 돈다. 둘 다 없으면 그렇게
# 남기고 지나간다. 어느 단계든 실패하면 함수가 0 이 아닌 값을 돌려주고, 호출한 단위가 계보로 넘어가지 않는다.
#
# "짝 맞추기를 빚졌다"의 정본은 pairing-owed.json 하나다. 어느 넘김이든 적재하기 전에(loaded/ 로 옮기기 전에)
# 쓰고, 짝 맞추기의 쌓기와 설치가 끝난 뒤에만 지운다. 그 파일이 있으면 넘김도 결정도 없는 날에도 다시 짝을
# 맞춘다: 2026-10-06 에 30527 넘김을 loaded/ 로 옮긴 뒤 짝 맞추기가 실패했고, 다음 실행은 볼 대기 넘김이 없어
# 다시 맞추지 않았다. 짝 맞추기가 성공하면 last-pairing.json 이 남는다. 그 기록도 빚도 없는데 loaded/ 에 넘김이
# 있으면(이 파일들이 생기기 전에 적재한 호스트) 그 넘김들로 한 번 빚을 만든다.

# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

LEGAL_DONG_STATE_ROOT="${FOUNDATION_LEGAL_DONG_CODE_STATE_ROOT:-/var/lib/foundation-platform/legal-dong-code}"
PARCEL_NUMBER_CHANGE_STATE_ROOT="${FOUNDATION_PARCEL_NUMBER_CHANGE_STATE_ROOT:-/var/lib/foundation-platform/parcel-number-change}"

legal_dong_install() {
  cp "$1" "$2.tmp"
  mv -f "$2.tmp" "$2"
}

# Pairs one snapshot: $1 snapshot date, $2 its Bronze key.
legal_dong_pair() {
  local snapshot_date="$1" table_key="$2" out="${work}/legal-dong" cout="${container_work}/legal-dong"
  local decisions=()
  if compgen -G "${LEGAL_DONG_STATE_ROOT}/steward/pending/*.json" >/dev/null; then
    mkdir -p "${out}/steward"
    cp "${LEGAL_DONG_STATE_ROOT}"/steward/pending/*.json "${out}/steward/"
    decisions=(--steward-decisions "${cout}/steward")
  fi
  # The official history is read once the table holds rows: one of its handoffs was loaded here.
  local official=()
  if compgen -G "${PARCEL_NUMBER_CHANGE_STATE_ROOT}/loaded/*" >/dev/null; then
    official=(--official-history-table silver.parcel_number_change_history)
  fi
  spark legal_dong_code_change_pairs.py --allow-non-smoke-write --snapshot-date "${snapshot_date}" \
    --table-source-record-id "${table_key}" --jibun-evidence editions ${official[@]+"${official[@]}"} ${decisions[@]+"${decisions[@]}"} \
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

# Loads every pending 필지고유번호변동연혁 handoff into silver.parcel_number_change_history, oldest first,
# and sets parcel_number_change_loaded to how many it loaded.
load_parcel_number_change_handoffs() {
  local pending="${PARCEL_NUMBER_CHANGE_STATE_ROOT}/pending" name handoff
  parcel_number_change_loaded=0
  mkdir -p "${work}/parcel-number-change" "${PARCEL_NUMBER_CHANGE_STATE_ROOT}/loaded"
  chmod 0777 "${work}/parcel-number-change"
  for handoff in $(find "${pending}" -mindepth 1 -maxdepth 1 -type d ! -name '.*' 2>/dev/null | sort); do
    name="$(basename "${handoff}")"
    cp -R "${handoff}" "${work}/parcel-number-change/${name}"
    chmod -R a+rwX "${work}/parcel-number-change/${name}" # Spark 컨테이너(uid 185)가 요약을 쓴다.
    spark vworld_parcel_number_change_history.py load --allow-non-smoke-write \
      --handoff-dir "${container_work}/parcel-number-change/${name}" \
      --summary-output "${container_work}/parcel-number-change/${name}/load-summary.json"
    mv "${handoff}" "${PARCEL_NUMBER_CHANGE_STATE_ROOT}/loaded/${name}"
    parcel_number_change_loaded=$((parcel_number_change_loaded + 1))
    job_journal "${journal}" "parcel-number-change loaded handoff=${name} run=${run_id}"
  done
}

# Records that a pairing is owed, before any handoff is loaded: the pending handoffs of both roots, the
# snapshot the pairing will run on, and the run that first owed it (kept while the marker stays). Once, when
# no pairing has been recorded as done but handoffs sit in loaded/, owes one for those. Journals the debt.
legal_dong_owe_pairing() {
  local since
  since="$(python3 -I - "${LEGAL_DONG_STATE_ROOT}" "${PARCEL_NUMBER_CHANGE_STATE_ROOT}" "${run_id}" <<'PY'
import datetime, json, pathlib, sys
legal, parcel, run = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
marker, done = legal / "pairing-owed.json", legal / "last-pairing.json"
latest = legal / "latest-legal-dong-snapshot.json"

def handoffs(root, state):
    d = root / state
    return sorted(p.name for p in d.iterdir() if p.is_dir() and not p.name.startswith(".")) if d.is_dir() else []

owed = {"legal_dong_code": handoffs(legal, "pending"), "parcel_number_change": handoffs(parcel, "pending")}
reason = "pending handoffs"
if not any(owed.values()) and not marker.exists() and not done.exists() and latest.exists():
    owed = {"legal_dong_code": handoffs(legal, "loaded"), "parcel_number_change": handoffs(parcel, "loaded")}
    reason = "loaded handoffs with no pairing recorded after them"
if not any(owed.values()) and not marker.exists():
    sys.exit(0)
dates = [json.load(open(legal / "pending" / n / "handoff.json", encoding="utf-8"))["snapshot_date"]
         for n in owed["legal_dong_code"] if (legal / "pending" / n / "handoff.json").exists()]
if latest.exists():
    dates.append(json.load(open(latest, encoding="utf-8"))["snapshot_date"])
now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
state = json.load(open(marker, encoding="utf-8")) if marker.exists() else {
    "owed_since_run": run, "created_at": now, "reason": reason, "handoffs": {}}
for kind, names in owed.items():
    state["handoffs"][kind] = sorted(set(state["handoffs"].get(kind, [])) | set(names))
if state.get("snapshot_date"):
    dates.append(state["snapshot_date"])
if dates:
    state["snapshot_date"] = max(dates)
tmp = marker.with_name(marker.name + ".tmp")
tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
tmp.replace(marker)
print(state["owed_since_run"])
PY
)"
  if [ -n "${since}" ]; then
    job_journal "${journal}" "legal-dong-code pairing owed since ${since} run=${run_id}"
  fi
}

# The owed pairing is paid — its append and installs succeeded on snapshot $1: records that, then drops the marker.
legal_dong_pairing_paid() {
  python3 -I - "${LEGAL_DONG_STATE_ROOT}" "${run_id}" "$1" <<'PY'
import datetime, json, pathlib, sys
legal = pathlib.Path(sys.argv[1])
marker = legal / "pairing-owed.json"
done = {"run": sys.argv[2], "snapshot_date": sys.argv[3],
        "at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
if marker.exists():
    done["paid"] = json.load(open(marker, encoding="utf-8"))
tmp = legal / "last-pairing.json.tmp"
tmp.write_text(json.dumps(done, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
tmp.replace(legal / "last-pairing.json")
marker.unlink(missing_ok=True)
PY
}

load_legal_dong_code_handoffs() {
  local pending="${LEGAL_DONG_STATE_ROOT}/pending" loaded=0 name handoff snapshot_date table_key table_file reason
  mkdir -p "${LEGAL_DONG_STATE_ROOT}"
  legal_dong_owe_pairing
  load_parcel_number_change_handoffs
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
    job_journal "${journal}" "legal-dong-code loaded handoff=${name} run=${run_id}"
  done
  if [ "${loaded}" != 0 ]; then
    legal_dong_pairing_paid "${snapshot_date}"
    return 0
  fi
  if [ "${parcel_number_change_loaded}" != 0 ]; then
    reason="new official history"
  elif compgen -G "${LEGAL_DONG_STATE_ROOT}/steward/pending/*.json" >/dev/null; then
    reason="steward decisions"
  elif [ -f "${LEGAL_DONG_STATE_ROOT}/pairing-owed.json" ]; then
    reason="pairing owed"
  else
    job_journal "${journal}" "legal-dong-code no pending handoff run=${run_id}"
    return 0
  fi
  if [ ! -f "${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json" ]; then
    # No legal-dong table loaded yet, so nothing to pair on; the marker stays until one is.
    job_journal "${journal}" "legal-dong-code ${reason} but no legal-dong snapshot loaded yet run=${run_id}"
    return 0
  fi
  read -r snapshot_date table_key < <(python3 -I -c 'import json, sys
m = json.load(open(sys.argv[1], encoding="utf-8")); print(m["snapshot_date"], m["source_record_id"])' \
    "${LEGAL_DONG_STATE_ROOT}/latest-legal-dong-snapshot.json")
  legal_dong_pair "${snapshot_date}" "${table_key}"
  legal_dong_pairing_paid "${snapshot_date}"
  job_journal "${journal}" "legal-dong-code re-paired (${reason}) on ${snapshot_date} run=${run_id}"
}
