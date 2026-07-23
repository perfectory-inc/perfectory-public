#!/usr/bin/env bash
set -euo pipefail

: "${DOWNLOAD_DS_ID:?set DOWNLOAD_DS_ID}"
: "${FILE_NO:?set FILE_NO}"

staging_dir="${FOUNDATION_PLATFORM_PROVIDER_ACQUISITION_STAGING_DIR:-/work/staging}"
public_proof_path="${PUBLIC_PROOF_PATH:-${staging_dir}/raon-agent-proof.json}"
private_replay_request_path="${PRIVATE_REPLAY_REQUEST_PATH:-${staging_dir}/private-replay-request.json}"
landing_object_key="${LANDING_OBJECT_KEY:-landing/provider=vworldkr/acquisition=raon_kupload_browser/job_id=container-proof/download_ds_id=${DOWNLOAD_DS_ID}/file_no=${FILE_NO}/download.zip}"
display="${DISPLAY:-:99}"

mkdir -p "${staging_dir}"

cleanup() {
  if [[ -n "${xvfb_pid:-}" ]]; then
    kill "${xvfb_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${raon_pid:-}" ]]; then
    kill "${raon_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ ! -x /opt/raonk-2018/raonk-2018 ]]; then
  echo "RAON Linux agent binary is missing" >&2
  exit 20
fi

Xvfb "${display}" -screen 0 1280x720x24 >/tmp/raon-xvfb.out 2>/tmp/raon-xvfb.err &
xvfb_pid=$!
export DISPLAY="${display}"

/opt/raonk-2018/raonk-2018 --no-sandbox >/tmp/raon-agent.out 2>/tmp/raon-agent.err &
raon_pid=$!

sleep "${RAON_AGENT_BOOT_WAIT_SECONDS:-5}"

args=(
  -m foundation_provider_acquisition.raon
  --download-ds-id "${DOWNLOAD_DS_ID}"
  --file-no "${FILE_NO}"
  --output "${public_proof_path}"
  --prove-raon-replay
  --private-replay-request-output "${private_replay_request_path}"
  --landing-object-key "${landing_object_key}"
)

if [[ -n "${PROVIDER_ACQUISITION_ENV_FILE:-}" ]]; then
  args+=(--env-file "${PROVIDER_ACQUISITION_ENV_FILE}")
fi

if [[ "${PROVIDER_ACQUISITION_USE_VWORLD_LOGIN:-0}" == "1" ]]; then
  args+=(--use-vworld-login)
fi

python "${args[@]}"
