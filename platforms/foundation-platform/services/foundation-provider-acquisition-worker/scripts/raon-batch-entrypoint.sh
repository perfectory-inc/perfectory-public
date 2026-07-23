#!/usr/bin/env bash
set -euo pipefail

: "${BATCH_ID:?set BATCH_ID}"

output_root="${PROVIDER_ACQUISITION_OUTPUT_ROOT:-/work/staging}"
display="${DISPLAY:-:99}"

mkdir -p "${output_root}"

if [[ -n "${PROVIDER_ACQUISITION_SELECTION_JSON_BASE64:-}" ]]; then
  selection_json_path="${output_root}/selection.json"
  printf '%s' "${PROVIDER_ACQUISITION_SELECTION_JSON_BASE64}" | base64 -d > "${selection_json_path}"
elif [[ -n "${PROVIDER_ACQUISITION_SELECTION_JSON_INLINE:-}" ]]; then
  selection_json_path="${output_root}/selection.json"
  printf '%s' "${PROVIDER_ACQUISITION_SELECTION_JSON_INLINE}" > "${selection_json_path}"
elif [[ -n "${PROVIDER_ACQUISITION_SELECTION_JSON:-}" ]]; then
  selection_json_path="${PROVIDER_ACQUISITION_SELECTION_JSON}"
else
  echo "set PROVIDER_ACQUISITION_SELECTION_JSON_BASE64, PROVIDER_ACQUISITION_SELECTION_JSON, or PROVIDER_ACQUISITION_SELECTION_JSON_INLINE" >&2
  exit 21
fi

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
  -m foundation_provider_acquisition.raon_batch
  --selection "${selection_json_path}"
  --batch-id "${BATCH_ID}"
  --output-root "${output_root}"
  --rust-binary /usr/local/bin/foundation-outbox-publisher
)

if [[ -n "${PROVIDER_ACQUISITION_ENV_FILE:-}" ]]; then
  args+=(--env-file "${PROVIDER_ACQUISITION_ENV_FILE}")
fi

if [[ -n "${PROVIDER_ACQUISITION_SOURCE_SLUGS:-}" ]]; then
  IFS=',' read -r -a source_slugs <<< "${PROVIDER_ACQUISITION_SOURCE_SLUGS}"
  for source_slug in "${source_slugs[@]}"; do
    if [[ -n "${source_slug}" ]]; then
      args+=(--source-slug "${source_slug}")
    fi
  done
fi

if [[ -n "${PROVIDER_ACQUISITION_PROVIDER_FILE_IDS:-}" ]]; then
  IFS=',' read -r -a provider_file_ids <<< "${PROVIDER_ACQUISITION_PROVIDER_FILE_IDS}"
  for provider_file_id in "${provider_file_ids[@]}"; do
    if [[ -n "${provider_file_id}" ]]; then
      args+=(--provider-file-id "${provider_file_id}")
    fi
  done
fi

if [[ -n "${PROVIDER_ACQUISITION_LIMIT:-}" ]]; then
  args+=(--limit "${PROVIDER_ACQUISITION_LIMIT}")
fi

if [[ -n "${PROVIDER_ACQUISITION_SHARD_INDEX:-}" ]]; then
  args+=(--shard-index "${PROVIDER_ACQUISITION_SHARD_INDEX}")
fi

if [[ -n "${PROVIDER_ACQUISITION_SHARD_COUNT:-}" ]]; then
  args+=(--shard-count "${PROVIDER_ACQUISITION_SHARD_COUNT}")
fi

if [[ "${PROVIDER_ACQUISITION_HEADLESS:-1}" != "1" ]]; then
  args+=(--headed)
fi

if [[ "${PROVIDER_ACQUISITION_USE_VWORLD_LOGIN:-1}" != "1" ]]; then
  args+=(--no-vworld-login)
fi

python "${args[@]}"
