#!/usr/bin/env bash
# 관은 매일 원천을 살핀다 (root ADR-0077, vworld 레인은 ADR-0168·0172, raon 레인은 ADR-0170).
#
# systemd 타이머가 매일 새벽 이 스크립트를 돌린다. 레인마다 provider 목록을 다시 계획하고, 이미 가진
# 파일은 건너뛰며, 새 파일만 Bronze 로 받는다.
#
#   hub     hub.go.kr 건축물대장 벌크. provider_file_id 지문으로 건너뛴다.
#   vworld  VWorld 토지 데이터셋 파일. 어느 데이터셋인지는 엔드포인트 카탈로그가 정한다
#           (daily_collection 이 source_sweep 인 vworld_dataset 엔드포인트) — 이 스크립트는 목록을
#           갖지 않는다. 같은 파일 번호·같은 제공자 갱신일·원장에 체크섬이 있으면 가진 것이다. 새 파일은
#           내용 해시 키로 쓴다(ADR-0152). 가지지 않은 파일은 크기와 무관하게 전부 받는다(ADR-0172): 파일마다
#           따로 커밋되고, 멈춘 실행은 다음 실행이 이어 받는다(받은 것은 건너뛴다). 한 실행이 받은 바이트가
#           카탈로그의 landed_bytes_notice 를 넘으면 슬랙에 안내 한 줄만 보낸다 — 거부도 실패도 아니다.
#   raon    제공자가 RAON 에이전트로만 주는 대용량 파일(ADR-0170). 한 실행 상한(카탈로그의
#           selection_archive_new_bytes_budget)이 0 이면 레인은 꺼져 있다: 원장에 없는 선택 묶음의 수·목록
#           크기·파일 id 를 journal 에 적고 슬랙에 안내 한 줄만 보낸다(실패 아님). 상한이 양수이고 증거가
#           원장에 없는 선택 묶음을 적었을 때만 scripts/ops/raon-large-files.sh 를 부른다. 그 스크립트의
#           전제(패키지·도커)가 없으면 78 로 끝나고 이 레인은 실패다 — 조용히 건너뛰지 않는다.
#   members 세 레인 뒤, Bronze ZIP 안 파일 이름을 아직 잰 적 없는 객체만 잰다(root ADR-0169 §1):
#           scripts/ops/bronze-object-members.sh 를 인자 없이 부른다 — 잴 원천의 기본 목록과 한 실행 개수
#           (FOUNDATION_PLATFORM_BRONZE_MEMBER_LIMIT, 기본 2000)는 그 스크립트와 명령에만 있다. 읽기 전용
#           키 쌍(lakehouse-reader 묶음)으로만 읽는다. 재지 못한 객체가 있거나 명령이 실패하면 이 단계는
#           실패한 레인처럼 빨갛다: 받은 파일은 journal 줄의 new= 에 그대로 남고, 이 실행은 결과 줄을 내지 않는다.
#           그 객체는 다음 실행이 다시 잰다(재지 못한 객체로는 Silver 레인이 어차피 거부한다).
#
# 실패한 레인이 증거에 실패 파일을 적었으면 레인마다 20 줄까지 `failed <원천>:<파일> <이유>` 를 stderr 로
# 낸다(유닛 저널에 간다, 루트 ADR-0174). 이유는 job-journal.sh 가 가린 뒤 200 자로 자른다.
#
# 한 레인이 실패해도 다른 레인은 돈다. 신규 0 인 날도 journal 에 한 줄을 남긴다 — "아무 일도
# 없었음"과 "확인 안 함"은 구별되어야 한다. 신규가 있거나 실패하면 슬랙 #alerts 가 안다.
# Silver 반영은 이 스크립트가 하지 않는다(ADR-0077 §5). 마지막 줄 `foundation-job-outcome changed|unchanged`
# 가 새 파일이 왔는지 말하고, changed 면 Airflow 가 이 원천을 읽는 Silver 레인을 시작한다(루트 ADR-0171).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/vworld-login.sh"
# journal 의 줄과 실패한 실행 로그의 끝은 유닛 저널에도 간다 — 운영자는 상태 디렉터리를 못 읽는다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152 §5). 이 목록이 단위의 환경 파일에 다 있는지는 저장소 검사가
#    계약으로 본다(config/runtime-secrets.contract.json, scripts/deploy/runtime_secrets.py check; ADR-0153).
#    읽기 전용 키 쌍(READER)은 members 단계의 것이다(ADR-0152, ADR-0169 §1). bronze-object-members.sh 도 없으면
#    거부하지만 그때는 레인들이 이미 돈 뒤다.
required_env=(
  FOUNDATION_ADMIN_PASSWORD
  FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER
  FOUNDATION_PLATFORM_RUNTIME_ENV
  FOUNDATION_PLATFORM_EXECUTION_CONTEXT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
# The VWorld login has a canonical name and deprecated aliases; the naming contract is the one list
# of them, so the check reads it rather than repeating the names here. Values are never printed.
for name in $(vworld_login_missing "${RELEASE_ROOT}/config/environment-variable-naming.contract.json"); do
  missing+=("${name}")
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "daily-source-sweep: refused before any side effect: missing ${missing[*]}" >&2
  exit 78 # EX_CONFIG
fi

STATE_ROOT="${FOUNDATION_SOURCE_SWEEP_STATE_ROOT:-/var/lib/foundation-platform/source-sweep}"
SLACK_TOKEN_FILE="${FOUNDATION_SOURCE_SWEEP_SLACK_TOKEN_FILE:-/etc/foundation-platform/secrets/alertmanager-slack-bot-token}"
SLACK_CHANNEL="${FOUNDATION_SOURCE_SWEEP_SLACK_CHANNEL:-#alerts}"
CATALOG="${RELEASE_ROOT}/docs/catalog/public-source-endpoint-catalog.v1.json"
# 내용 해시 키는 본문을 다 받은 뒤에 정해지므로, 본문은 해시를 재며 이 디렉터리에 받아 두었다가 올린다(ADR-0168).
# 기본값은 데이터 디스크다 — 상태 디렉터리가 있는 루트 디스크는 작고 두 번 찼다(2026-10-02). 단위의
# ReadWritePaths 와 릴리스 설치가 이 기본값을 만든다.
SPOOL_DIR="${FOUNDATION_SOURCE_SWEEP_SPOOL_DIR:-/data/foundation-platform/source-sweep/spool}"

mkdir -p "${STATE_ROOT}"
journal="${STATE_ROOT}/journal.log"
run_log="${STATE_ROOT}/last-run.log"
plan_path="${STATE_ROOT}/building-hub-plan.json"
evidence_path="${STATE_ROOT}/building-hub-evidence.json"
vworld_plan_path="${STATE_ROOT}/vworld-plan.json"
vworld_inventory_path="${STATE_ROOT}/vworld-inventory.json"
vworld_evidence_path="${STATE_ROOT}/vworld-evidence.json"
raon_summary_path="${STATE_ROOT}/raon-summary.json"
members_output_path="${STATE_ROOT}/bronze-object-members.out"
# Yesterday's evidence must not read as today's: a lane that dies before writing leaves none.
rm -f "${evidence_path}" "${vworld_plan_path}" "${vworld_inventory_path}" "${vworld_evidence_path}" \
  "${raon_summary_path}" "${members_output_path}"
# A run killed mid-file (OOM, timeout) leaves its spool files; the next run starts empty.
mkdir -p "${SPOOL_DIR}"
find "${SPOOL_DIR}" -mindepth 1 -maxdepth 1 -name '.provider-*.part' -delete
: > "${run_log}"

# 슬랙으로 한 줄 보낸다. 토큰은 변수로만 다루고 어디에도 찍지 않는다. 배달 실패가
# sweep 자체를 죽여서는 안 되므로 (이미 journal 이 정본이다) 오류는 삼킨다.
notify_slack() {
  local token payload
  token="$(tr -d '\r\n' < "${SLACK_TOKEN_FILE}")" || return 0
  payload="$(python3 - "${SLACK_CHANNEL}" "$1" <<'PY'
import json, sys
print(json.dumps({"channel": sys.argv[1], "text": sys.argv[2]}, ensure_ascii=False))
PY
)"
  curl -sS --max-time 30 -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "${payload}" https://slack.com/api/chat.postMessage >/dev/null 2>&1 || true
}

on_error() {
  local line="$1"
  job_journal "${journal}" "sweep FAILED at line ${line} (tail of run log follows)" >&2
  tail -5 "${run_log}" >> "${journal}" 2>/dev/null || true
  job_run_log_tail "${run_log}"
  # Slack hears it from the unit (OnFailure=foundation-unit-failed@), once.
}
trap 'on_error ${LINENO}' ERR

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — compose 가 postgres 를 세우는 것과 같은
# 재료로 조립한다: 사용자는 docker-compose.yml 이 고정한 foundation_admin, 비밀번호는
# recovery.env 의 FOUNDATION_ADMIN_PASSWORD (2026-09-04 첫 발사가 이 이름 어긋남으로 죽었다).
if [ -z "${DATABASE_URL:-}" ]; then
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

# 1. hub 레인.
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_PLAN_PATH="${plan_path}"
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_EVIDENCE_PATH="${evidence_path}"
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_LIVE_WRITE=1
export FOUNDATION_PLATFORM_BUILDING_HUB_BULK_COLLECTION_CONFIRM_FULL_DOWNLOAD=1
hub_rc=0
{ "${PUBLISHER_BIN}" plan-building-hub-bulk-collection &&
  "${PUBLISHER_BIN}" ingest-building-hub-bulk-collection; } >> "${run_log}" 2>&1 || hub_rc=$?

# 2. vworld 레인. 계획은 카탈로그가 source_sweep 으로 표시한 엔드포인트만, 요약 파일 없이 만든다.
#    바이트 예산은 없다(ADR-0172) — 가지지 않은 파일은 전부 받는다.
export FOUNDATION_PLATFORM_VWORLD_DATASET_ENDPOINT_CATALOG_PATH="${CATALOG}"
export FOUNDATION_PLATFORM_VWORLD_DATASET_DAILY_COLLECTION=source_sweep
unset FOUNDATION_PLATFORM_VWORLD_DATASET_INVENTORY_SUMMARY_PATH FOUNDATION_PLATFORM_BRONZE_FORCE_REFETCH
export FOUNDATION_PLATFORM_VWORLD_DATASET_COLLECTION_PLAN_PATH="${vworld_plan_path}"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INVENTORY_PATH="${vworld_inventory_path}"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_INGEST_EVIDENCE_PATH="${vworld_evidence_path}"
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_CONFIRM_FULL_DOWNLOAD=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_EXCLUDE_SELECTION_ARCHIVES=1
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed
export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR="${SPOOL_DIR}"
vworld_rc=0
{ "${PUBLISHER_BIN}" plan-vworld-dataset-collection &&
  "${PUBLISHER_BIN}" inventory-vworld-dataset-files &&
  "${PUBLISHER_BIN}" ingest-vworld-dataset-files; } >> "${run_log}" 2>&1 || vworld_rc=$?

# 3. raon 레인(ADR-0170). 상한은 카탈로그의 selection_archive_new_bytes_budget, 운영자가 그 실행 하나만
#    FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET 로 바꾼다 — raon-large-files.sh 와 같은 규칙이다. 상한 0 은
#    "레인 꺼짐": 스크립트를 부르지 않고 4 에서 밀린 파일을 적고 안내만 한다. 상한이 양수이고 vworld 레인이
#    원장에 없는 RAON 선택 묶음을 증거에 적었을 때만 돈다. vworld 레인의 파일이 실패해도 증거는 선택
#    묶음을 적으므로 레인은 서로 독립이다.
raon_deferred="$(python3 -I -c 'import json, sys; print(int(json.load(open(sys.argv[1], encoding="utf-8")).get("deferred_selection_archive_file_count") or 0))' \
  "${vworld_evidence_path}" 2>/dev/null || echo 0)"
raon_budget="${FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET:-$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["daily_collections"]["source_sweep"]["selection_archive_new_bytes_budget"])' \
  "${CATALOG}" 2>/dev/null || true)}"
[[ "${raon_budget}" =~ ^[0-9]+$ ]] || raon_budget=invalid
raon_rc=0
if [ "${raon_deferred}" -gt 0 ] && [ "${raon_budget}" != invalid ] && [ "${raon_budget}" -gt 0 ]; then
  FOUNDATION_RAON_LARGE_FILES_SUMMARY_PATH="${raon_summary_path}" \
    "${RELEASE_ROOT}/scripts/ops/raon-large-files.sh" run "${vworld_evidence_path}" >> "${run_log}" 2>&1 || raon_rc=$?
fi

# 4. ZIP 안 이름(root ADR-0169 §1). 레인이 실패해도 돈다 — 그때까지 받은 것과 지난날 못 잰 것을 잰다. 잴 원천과
#    한 실행 개수는 그 스크립트·명령의 기본값이다. 요약 줄(`bronze-object-members-json`)은 stdout 에, 객체마다의
#    경고는 실행 로그에 간다.
members_rc=0
"${RELEASE_ROOT}/scripts/ops/bronze-object-members.sh" > "${members_output_path}" 2>> "${run_log}" || members_rc=$?
cat "${members_output_path}" >> "${run_log}" 2>/dev/null || true

# 5. 증거를 요약해 journal 한 줄 + 슬랙 알림으로 바꾼다. 증거가 없으면 그 레인은 실패다.
summary="$(python3 - "${evidence_path}" "${hub_rc}" "${vworld_evidence_path}" "${vworld_rc}" \
  "${vworld_plan_path}" "${vworld_inventory_path}" \
  "${raon_deferred}" "${raon_rc}" "${raon_summary_path}" "${raon_budget}" \
  "${members_output_path}" "${members_rc}" <<'PY'
import json, os, sys
(hub_path, hub_rc, vworld_path, vworld_rc, plan_path, inventory_path, raon_deferred, raon_rc, raon_path,
 raon_budget, members_path, members_rc) = sys.argv[1:13]

# Failed files and their reasons, per failed lane, for the unit's journal: the evidence naming them
# is in the state directory, which an operator cannot read. job_failed_files masks and cuts them.
FAILED_FILES_PER_LANE = 20
failed_files = []

def collect_failures(lane, entries):
    entries = list(entries)
    for ident, reason in entries[:FAILED_FILES_PER_LANE]:
        # One line per file: a reason's own newlines and tabs would split it.
        failed_files.append(f"{ident}\t{' '.join(str(reason or 'no reason recorded').split())}")
    if len(entries) > FAILED_FILES_PER_LANE:
        failed_files.append(f"{lane}:+{len(entries) - FAILED_FILES_PER_LANE}\tmore failed files, not listed here")

def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return None

def gib(n):
    return f"{n / 2**30:.1f}GiB"

failed, new_names, notes, info = [], [], [], []
hub = load(hub_path)
if hub is None:
    hub_line = f"hub status=no-evidence rc={hub_rc}"
    failed.append("hub")
else:
    jobs = hub.get("jobs", [])
    hub_line = (f"hub planned={hub.get('selected_job_count')} new={hub.get('succeeded_job_count')} "
                f"skipped={hub.get('skipped_job_count')} failed={hub.get('failed_job_count')} "
                f"status={hub.get('status')}")
    if hub_rc != "0" or hub.get("failed_job_count"):
        failed.append("hub")
        collect_failures("hub", ((f"{j.get('source_slug')}:{j.get('provider_file_id')}", j.get("error_message"))
                                 for j in jobs if j.get("status") == "failed"))
    new_names += [f"{j.get('source_slug')}:{j.get('provider_file_id')}" for j in jobs if j.get("status") == "succeeded"]

vworld = load(vworld_path)
if vworld is None:
    vworld_line = f"vworld status=no-evidence rc={vworld_rc}"
    failed.append("vworld")
else:
    inventory = load(inventory_path) or {}
    # What this run had to fetch (every listed file not already held) and what it landed, both by
    # count and bytes: pending is the provider's listed size, landed what Bronze committed (ADR-0172).
    listed = {(f.get("download_ds_id"), f.get("file_no")): (f.get("size_kib") or 0) * 1024
              for job in inventory.get("jobs", []) for f in job.get("files", [])}
    files = vworld.get("files", [])
    pending = [f for f in files if f.get("status") != "skipped_existing"]
    pending_bytes = sum(listed.get((f.get("download_ds_id"), f.get("file_no")), 0) for f in pending)
    landed = [f for f in files if f.get("status") == "succeeded"]
    landed_bytes = sum(f.get("size_bytes") or 0 for f in landed)
    vworld_line = (f"vworld planned={vworld.get('selected_file_count')} new={vworld.get('succeeded_file_count')} "
                   f"skipped={vworld.get('skipped_file_count')} failed={vworld.get('failed_file_count')} "
                   f"pending={len(pending)} pending_bytes={pending_bytes} landed_bytes={landed_bytes} "
                   f"status={vworld.get('status')}")
    # Files the provider lists only as RAON selection archives (over about 500 MB) are not taken by
    # this lane (ADR-0168); the count is journaled so their absence is visible, not silent.
    vworld_line += f" selection_archives_not_swept={inventory.get('selection_archive_file_count')}"
    # A signal, never a limit (ADR-0172): an unusually large day is said once.
    notice = (load(plan_path) or {}).get("landed_bytes_notice")
    if isinstance(notice, int) and landed_bytes > notice:
        info.append(f"ℹ️ daily-source-sweep: VWorld 레인이 한 실행에 {len(landed)}건 {gib(landed_bytes)}을 받았다 — "
                    f"평소보다 많다(안내 기준 {gib(notice)}). 막지 않았다; 제공자가 과거 판을 다시 올렸는지 볼 것 (ADR-0172)")
    if vworld_rc != "0" or vworld.get("failed_file_count"):
        failed.append("vworld")
        # A file the provider blocked fails the lane too unless the ingest deferred it (status says so).
        collect_failures("vworld", (
            (f"{f.get('source_slug')}:{f.get('download_ds_id')}-{f.get('file_no')}",
             f.get("error_message") or (f.get("status") if f.get("status") != "failed" else None))
            for f in files if f.get("status") in ("failed", "provider_acquisition_blocked")))
    new_names += [f"{f.get('source_slug')}:{f.get('download_ds_id')}-{f.get('file_no')}"
                  for f in vworld.get("files", []) if f.get("status") == "succeeded"]

# The RAON lane (ADR-0170): a cap of 0 is the lane switched off, which records what waits and says
# so once (not a failure); a positive cap runs it when the vworld evidence lists selection archives
# Bronze lacks.
if raon_budget == "invalid":
    raon_line = f"raon deferred={raon_deferred} status=invalid-budget"
    notes.append("RAON 대용량 레인의 상한(selection_archive_new_bytes_budget 또는 "
                 "FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET)이 0 이상의 정수 바이트가 아니다 (ADR-0170)")
    failed.append("raon")
elif raon_deferred == "0":
    raon_line = "raon deferred=0"
elif raon_budget == "0":
    waiting = [a for a in ((load(vworld_path) or {}).get("selection_archives") or [])
               if a.get("status") == "deferred_selection_archive"]
    listed = sum(a.get("size_bytes") or 0 for a in waiting)
    ids = ",".join(f"{a.get('source_slug')}:{a.get('download_ds_id')}-{a.get('file_no')}" for a in waiting)
    raon_line = f"raon deferred={raon_deferred} listed_bytes={listed} budget=0 status=lane-off files={ids}"
    info.append(f"ℹ️ daily-source-sweep: RAON 대용량 파일 {raon_deferred}건 {gib(listed)}이 Bronze 에 없다 — "
                f"레인이 꺼져 있어(상한 0) 받지 않았다. 받는 법은 런북 provider-acquisition-fargate.md "
                f"'첫 감독 실행' (ADR-0170)")
else:
    raon = load(raon_path)
    if raon is None:
        raon_line = f"raon deferred={raon_deferred} status=no-summary rc={raon_rc}"
        if raon_rc == "78":
            notes.append("RAON 대용량 레인의 전제(호스트의 고정 패키지, 도커, 설정)가 없어 아무것도 하지 않았다 — "
                         "런북 provider-acquisition-fargate.md '데이터 호스트의 RAON 대용량 레인' (ADR-0170)")
        failed.append("raon")
    else:
        raon_line = (f"raon deferred={raon_deferred} planned={raon.get('planned')} committed={raon.get('committed')} "
                     f"failed={raon.get('failed')} listed_bytes={raon.get('listed_bytes')} budget={raon.get('budget')} "
                     f"status={raon.get('status')} run={raon.get('run_id')}")
        if raon.get("budget_override"):
            raon_line += " budget_override=1"
        if raon.get("status") == "blocked_new_bytes_budget":
            notes.append(f"RAON 대용량 파일 {raon.get('planned')}건 {gib(raon.get('listed_bytes') or 0)}이 한 실행 상한 "
                         f"{gib(raon.get('budget') or 0)}을 넘어 하나도 받지 않았다 — 운영자가 상한을 올려 받는다 (ADR-0170)")
        if raon_rc != "0" or raon.get("status") not in ("ready", "nothing-to-fetch"):
            failed.append("raon")
            collect_failures("raon", ((f"{f.get('source_slug')}:{f.get('provider_file_id')}", f.get("error_kind"))
                                      for f in raon.get("files", [])
                                      if f.get("status") == "failed" or f.get("error_kind")))
        new_names += [f"{f.get('source_slug')}:{f.get('provider_file_id')}"
                      for f in raon.get("files", []) if f.get("status") == "committed"]

# ZIP members (root ADR-0169 §1): the command's last stdout line is its summary. A run that could not
# read some objects, or failed outright, is a failed step: what landed stays in new= above, and the
# objects are measured again next run.
members = None
try:
    for raw in open(members_path, encoding="utf-8"):
        if raw.startswith("bronze-object-members-json "):
            members = json.loads(raw.split(" ", 1)[1])
except (OSError, ValueError):
    members = None
if members is None:
    members_line = f"members status=no-summary rc={members_rc}"
    if members_rc == "78":
        notes.append("ZIP 안 이름 측정의 설정(lakehouse-reader 읽기 키 등)이 없어 재지 않았다 — "
                     "런북 silver-refresh.md 5 절 (ADR-0169 §1)")
    failed.append("members")
else:
    members_line = (f"members measured={members.get('measured')} failed={members.get('failed')} "
                    f"selected={members.get('selected')} members={members.get('members')}")
    if members_rc != "0" or members.get("failed"):
        notes.append(f"Bronze 객체 {members.get('failed')}건의 ZIP 안 이름을 읽지 못했다 — 다음 실행이 다시 잰다; "
                     "객체 이름은 유닛 저널의 실행 로그 끝 (ADR-0169 §1)")
        failed.append("members")

names = ", ".join(new_names[:10]) + (f" 외 {len(new_names) - 10}건" if len(new_names) > 10 else "")
print(json.dumps({"line": f"{hub_line} | {vworld_line} | {raon_line} | {members_line}", "failed": failed,
                  "new": len(new_names), "names": names, "notes": notes, "info": info,
                  "failed_files": failed_files}, ensure_ascii=False))
PY
)"
field() { python3 -c 'import json, sys; v = json.loads(sys.argv[1])[sys.argv[2]]; print(" / ".join(v) if isinstance(v, list) else v)' "${summary}" "$1"; }
line="$(field line)"
failed_lanes="$(field failed)"
new_count="$(field new)"
job_journal "${journal}" "sweep ${line}"

if [ -n "${failed_lanes}" ]; then
  notes="$(field notes)"
  echo "daily-source-sweep: failed lanes: ${failed_lanes}${notes:+ — ${notes}}" >&2
  # Which files failed and why, from the evidence (masked, cut; ADR-0174's helper).
  python3 -I -c 'import json, sys; print("\n".join(json.loads(sys.argv[1])["failed_files"]))' "${summary}" \
    | job_failed_files || true
  job_run_log_tail "${run_log}"
  notify_slack "🔴 daily-source-sweep: ${failed_lanes} 레인 실패 (${line})${notes:+ — ${notes}}"
  exit 1
fi
info="$(field info)"
if [ -n "${info}" ]; then
  notify_slack "${info}"
fi
if [ "${new_count}" != "0" ]; then
  notify_slack "📦 새 원천 파일 ${new_count}건 도착 — 오늘 반영할 것 (ADR-0077 §5): $(field names)"
  # 새 파일이 Bronze 에 왔다: 이 원천을 읽는 Silver 레인이 바로 시작된다(루트 ADR-0171).
  echo "foundation-job-outcome changed"
else
  echo "foundation-job-outcome unchanged"
fi
