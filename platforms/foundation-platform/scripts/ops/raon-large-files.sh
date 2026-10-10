#!/usr/bin/env bash
# VWorld 대용량 파일(RAON 선택 묶음)을 데이터 호스트의 RAON 에이전트로 받는다 (root ADR-0170, ADR-0168 개정).
#
# 제공자는 500MB 를 넘는 파일을 RAON K 에이전트로만 준다(SelectionArchive). 매일 훑기의 VWorld 레인은 그
# 파일들을 받지 않고, 원장이 갖지 않은 것만 증거의 selection_archives 에 deferred_selection_archive 로
# 남긴다. 이 스크립트가 그 증거를 받아:
#
#   1. 계획   plan-provider-acquisition-jobs 가 증거와 그 증거가 읽은 목록으로 작업을 만든다. 한 실행이 받을
#             바이트 상한(카탈로그 daily_collections.source_sweep.selection_archive_new_bytes_budget)을 넘으면
#             하나도 받지 않고 실패한다 — 밀린 것은 운영자가 상한을 올려 일부러 받는다(런북).
#   2. 이미지 작업자 소스로 빌드 맥락을 만들고, 맥락의 내용과 RAON 에이전트 패키지 고정값(제조사 주소와 sha256,
#             config/provider-agent-packages.contract.json)의 해시를 이름에 단 이미지를 없을 때만 빌드한다.
#             빌드는 그 주소에서 받은 바이트를 sha256 으로 확인한 뒤 설치한다.
#   3. 수집   그 이미지에서 브라우저가 RAON 페이지를 열어 내려받기 요청을 잡고, 이 릴리스의 publisher
#             (import-provider-acquisition-landing)가 내용 주소 키로 Bronze 에 바로 쓴다. 스풀은 데이터 디스크.
#   4. 요약   한 줄과 요약 JSON. 매일 훑기가 journal·슬랙에 싣는다.
#
#   raon-large-files.sh build              전제 확인 + 이미지
#   raon-large-files.sh plan <evidence>    전제 확인 + 계획만(아무것도 받지 않는다)
#   raon-large-files.sh run <evidence>     전제 확인 + 계획 + 이미지 + 수집
#
# 운영자 설정(그 실행 하나): FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET(상한 바이트),
# FOUNDATION_RAON_LARGE_FILES_MAX_FILES(앞에서 N 개만). 전제(환경, VWorld 로그인, 패키지 고정값, 도커와 그
# 데몬, 증거)가 없으면 무엇도 쓰기 전에 78 로 끝난다 — 조용히 건너뛰지 않는다. 이미지 빌드가 실패하면(주소가
# 답하지 않거나 바이트가 고정값과 다르면) 받기 전에 실패한다.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current
source "$(dirname "${BASH_SOURCE[0]}")/database-url.sh"
source "$(dirname "${BASH_SOURCE[0]}")/vworld-login.sh"
# journal 의 줄은 표준 출력에도 간다(루트 ADR-0174).
source "$(dirname "${BASH_SOURCE[0]}")/job-journal.sh"

refuse() {
  echo "raon-large-files: refused before any side effect: $1" >&2
  exit 78 # EX_CONFIG
}

mode="${1:-}"
evidence="${2:-}"
case "${mode}" in
  build) [ "$#" -eq 1 ] || refuse "usage: raon-large-files.sh build" ;;
  plan | run) [ "$#" -eq 2 ] || refuse "usage: raon-large-files.sh ${mode} <vworld ingest evidence>" ;;
  *) refuse "usage: raon-large-files.sh build | plan <evidence> | run <evidence>" ;;
esac

# 0. 부작용 전에 전부 확인한다(루트 ADR-0152 §5). 이 목록이 환경 파일에 다 있는지는 저장소 검사가 계약으로
#    본다(config/runtime-secrets.contract.json 의 run raon-large-files; ADR-0153).
required_env=(
  FOUNDATION_ADMIN_PASSWORD
  FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER
  FOUNDATION_PLATFORM_RUNTIME_ENV
  FOUNDATION_PLATFORM_EXECUTION_CONTEXT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
)
missing=()
for name in "${required_env[@]}"; do
  [ -n "${!name:-}" ] || missing+=("${name}")
done
NAMING="${RELEASE_ROOT}/config/environment-variable-naming.contract.json"
for name in $(vworld_login_missing "${NAMING}"); do missing+=("${name}"); done
[ "${#missing[@]}" -eq 0 ] || refuse "missing ${missing[*]}"

CATALOG="${RELEASE_ROOT}/docs/catalog/public-source-endpoint-catalog.v1.json"
PACKAGES="${RELEASE_ROOT}/config/provider-agent-packages.contract.json"
WORKER="services/foundation-provider-acquisition-worker"
ROOT_DIR="${FOUNDATION_RAON_LARGE_FILES_ROOT:-/data/foundation-platform/source-sweep/raon}"

# The pinned package: where its bytes come from and which bytes they must be. The image build
# downloads it and checks the sha256 before installing (Dockerfile.raon-batch).
read -r package_url package_sha256 < <(python3 -I - "${PACKAGES}" <<'PY'
import json, re, sys
package = json.load(open(sys.argv[1], encoding="utf-8"))["packages"]["raonk-2018"]
if not package["url"].startswith("https://") or not re.fullmatch(r"[0-9a-f]{64}", package["sha256"]):
    sys.exit(1)
print(package["url"], package["sha256"])
PY
) || refuse "the RAON agent package pin in ${PACKAGES} is missing or malformed"
command -v docker >/dev/null 2>&1 || refuse "docker is not installed"
# A stopped daemon answers nothing rather than an error; the timeout turns that into a refusal.
timeout 30 docker info >/dev/null 2>&1 || refuse "the docker daemon does not answer"
if [ "${mode}" != build ]; then
  [ -f "${evidence}" ] || refuse "no VWorld ingest evidence at ${evidence}"
fi

# The run's budget: the catalog's, or the operator's for this run.
budget="${FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET:-$(python3 -I - "${CATALOG}" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["daily_collections"]["source_sweep"][
    "selection_archive_new_bytes_budget"])
PY
)}"
[[ "${budget}" =~ ^[0-9]+$ ]] || refuse "the large-file budget must be a whole number of bytes, not ${budget}"
max_files="${FOUNDATION_RAON_LARGE_FILES_MAX_FILES:-}"
[[ -z "${max_files}" || "${max_files}" =~ ^[1-9][0-9]*$ ]] || refuse "FOUNDATION_RAON_LARGE_FILES_MAX_FILES must be a positive whole number"

# recovery.env 는 DATABASE_URL 을 들고 있지 않다 — 매일 훑기와 같은 재료로 조립한다.
if [ -z "${DATABASE_URL:-}" ]; then
  DATABASE_URL="$(foundation_database_url)"
  export DATABASE_URL
fi

# The image is named after the content of its build context (the worker, the Dockerfile, the
# naming contract) and the pinned package. A release that changes none of them reuses it.
image_inputs=(
  "${WORKER}/Dockerfile.raon-batch"
  "${WORKER}/pyproject.toml"
  "${WORKER}/requirements.lock"
  "${WORKER}/src"
  "${WORKER}/scripts"
  config/environment-variable-naming.contract.json
)
content_id="$(cd "${RELEASE_ROOT}" &&
  { find "${image_inputs[@]}" -type f ! -path '*/__pycache__/*' ! -name '*.pyc' -print0 |
      sort -z | xargs -0 sha256sum; printf '%s  %s\n' "${package_sha256}" "${package_url}"; } |
  sha256sum | cut -c1-16)"
RAON_IMAGE="foundation-platform/raon-batch-${content_id}:local"

ensure_image() {
  if docker image inspect "${RAON_IMAGE}" >/dev/null 2>&1; then
    echo "raon-large-files: image ${RAON_IMAGE} is present"
    return 0
  fi
  # The context is exactly the hashed inputs, so the name says what the image was built from.
  local context="${ROOT_DIR}/context"
  rm -rf "${context}"
  mkdir -p "${context}"
  (cd "${RELEASE_ROOT}" && cp -R --parents "${image_inputs[@]}" "${context}/")
  find "${context}" -name __pycache__ -prune -exec rm -rf {} +
  # The Docker CLI keeps its state under DOCKER_CONFIG; the unit's home is not writable.
  mkdir -p "${ROOT_DIR}/docker-config"
  # The command ends at its literal context: the container policy guard reads the logical line.
  (
    cd "${context}"
    DOCKER_CONFIG="${ROOT_DIR}/docker-config" DOCKER_BUILDKIT=1 docker build -f "${WORKER}/Dockerfile.raon-batch" --build-arg "RAON_DEB_URL=${package_url}" --build-arg "RAON_DEB_SHA256=${package_sha256}" -t "${RAON_IMAGE}" .
  )
  rm -rf "${context}"
}

if [ "${mode}" = build ]; then
  ensure_image
  exit 0
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="${ROOT_DIR}/runs/${run_id}"
summary_path="${FOUNDATION_RAON_LARGE_FILES_SUMMARY_PATH:-${run_dir}/summary.json}"
journal="${ROOT_DIR}/journal.log"
mkdir -p "${run_dir}"
rm -f "${summary_path}"
# A run killed mid-file leaves its staged bytes (up to a file of 1.5 GiB); the next run starts empty.
# runs/<run>/batch/<batch>/<job>/staging
find "${ROOT_DIR}/runs" -mindepth 5 -maxdepth 5 -type d -name staging -prune -exec rm -rf {} +

# 1. 계획.
plan_rc=0
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_BLOCKED_EVIDENCE_PATH="${evidence}" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_PLAN_OUTPUT_PATH="${run_dir}/plan.json" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_NEW_BYTES_BUDGET="${budget}" \
FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_MAX_FILES="${max_files}" \
  "${PUBLISHER_BIN}" plan-provider-acquisition-jobs >> "${run_dir}/run.log" 2>&1 || plan_rc=$?

job_count="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["job_count"])' \
  "${run_dir}/plan.json" 2>/dev/null || echo 0)"
batch_id="raon-${run_id}"
batch_rc=0
if [ "${plan_rc}" -eq 0 ] && [ "${mode}" = run ] && [ "${job_count}" -gt 0 ]; then
  # 2. 이미지.  3. 수집. 이 릴리스의 publisher 를 읽기 전용으로 넣는다 — 이미지는 Rust 를 빌드하지 않는다
  #    (릴리스 빌드는 혼자 돌고 메모리를 22g 쓴다, ADR-0137). 환경은 이름으로만 넘긴다(값을 찍지 않는다).
  ensure_image >> "${run_dir}/run.log" 2>&1 || batch_rc=$?
  if [ "${batch_rc}" -eq 0 ]; then
    # Names only: a line without `=` in an env file makes docker take the value from this
    # environment, so no value is written anywhere. The publisher reads the same settings it reads
    # natively in the sweep (every FOUNDATION_PLATFORM_* name), the database and the VWorld login.
    forwarded="${run_dir}/forwarded-env.names"
    {
      echo DATABASE_URL
      compgen -e | grep '^FOUNDATION_PLATFORM_' || true
      vworld_login_names "${NAMING}"
    } | sort -u | while read -r name; do [ -n "${!name:-}" ] && echo "${name}"; done > "${forwarded}" || true
    mkdir -p "${run_dir}/home"
    docker run --rm --network host --memory 4g --shm-size=1g --user "$(id -u):$(id -g)" \
      --env-file "${forwarded}" -e HOME=/work/run/home -e "BATCH_ID=${batch_id}" \
      -e PROVIDER_ACQUISITION_SELECTION_JSON=/work/run/plan.json -e PROVIDER_ACQUISITION_OUTPUT_ROOT=/work/run/batch \
      -v "${run_dir}:/work/run" -v "${PUBLISHER_BIN}:/usr/local/bin/foundation-outbox-publisher:ro" \
      "${RAON_IMAGE}" >> "${run_dir}/run.log" 2>&1 || batch_rc=$?
  fi
fi

# 4. 요약. 계획이 없으면 그 실행은 실패다.
line="$(python3 -I - "${run_dir}/plan.json" "${run_dir}/batch/${batch_id}/summary.json" "${plan_rc}" "${batch_rc}" \
  "${mode}" "${run_id}" "${budget}" "${FOUNDATION_RAON_LARGE_FILES_NEW_BYTES_BUDGET:+1}" "${summary_path}" <<'PY'
import json, sys
plan_path, batch_path, plan_rc, batch_rc, mode, run_id, budget, override, summary_path = sys.argv[1:10]

def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return None

plan, batch = load(plan_path), load(batch_path)
summary = {"run_id": run_id, "mode": mode, "budget": int(budget), "budget_override": override == "1",
           "plan_rc": int(plan_rc), "batch_rc": int(batch_rc), "committed": 0, "failed": 0, "files": []}
if plan is None:
    summary.update(status="no-plan", planned=None, listed_bytes=None, candidates=None)
else:
    summary.update(planned=plan.get("job_count"), listed_bytes=plan.get("listed_bytes_total"),
                   candidates=plan.get("candidate_count"))
    if plan.get("status") != "ready":
        summary["status"] = plan.get("status")
    elif plan.get("job_count") == 0:
        summary["status"] = "nothing-to-fetch"
    elif mode == "plan":
        summary["status"] = "planned"
    elif batch is None:
        summary["status"] = "no-batch-summary"
    else:
        summary.update(committed=batch.get("committed_count"), failed=batch.get("failed_count"),
                       files=[{k: r.get(k) for k in ("source_slug", "provider_file_id", "status", "bronze_object_key",
                                                     "error_kind")} for r in batch.get("results", [])])
        complete = batch.get("failed_count") == 0 and batch.get("committed_count") == plan.get("job_count")
        summary["status"] = "ready" if complete and batch_rc == "0" else "failed"
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, ensure_ascii=False)
s = summary
print(f"raon planned={s['planned']} committed={s['committed']} failed={s['failed']} "
      f"listed_bytes={s['listed_bytes']} budget={s['budget']} status={s['status']} run={run_id}"
      + (" budget_override=1" if s["budget_override"] else ""))
PY
)"
job_journal "${journal}" "${line}"
case "${line}" in
  *" status=ready "* | *" status=nothing-to-fetch "* | *" status=planned "*) exit 0 ;;
  *) exit 1 ;;
esac
