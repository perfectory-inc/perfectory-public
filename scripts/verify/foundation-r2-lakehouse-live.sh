#!/usr/bin/env bash
# Runs the opt-in live Cloudflare R2 and R2 Data Catalog lanes.
#
# This script deliberately does not start an object-storage emulator. Cloudflare R2 is the
# canonical backend in every environment; missing credentials are an error, never a skip.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$ROOT"

load_dotenv_file() {
  local path="$1"
  [ -f "$path" ] || return 0

  local line name value
  while IFS= read -r line || [ -n "$line" ]; do
    # Parse dotenv assignments without sourcing the file: values must never be able to execute
    # shell syntax. Existing process variables win, which keeps CI secret injection authoritative.
    line="${line#${line%%[![:space:]]*}}"
    [ -z "$line" ] || [ "${line:0:1}" = "#" ] || {
      if [[ "$line" =~ ^(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=(.*)$ ]]; then
        name="${BASH_REMATCH[2]}"
        value="${BASH_REMATCH[3]}"
        value="${value#${value%%[![:space:]]*}}"
        value="${value%${value##*[![:space:]]}}"
        if [[ "$value" == \"*\" && "$value" == *\" ]]; then
          value="${value:1:${#value}-2}"
        elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
          value="${value:1:${#value}-2}"
        fi
        if [ -z "${!name+x}" ]; then
          printf -v "$name" '%s' "$value"
          export "$name"
        fi
      fi
    }
  done < "$path"
}

ENV_FILE="${FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE:-$ROOT/platforms/foundation-platform/.env.local}"
if [ -n "${FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE:-}" ] && [ ! -f "$ENV_FILE" ]; then
  echo "foundation-r2-lakehouse-live: env file does not exist: $ENV_FILE" >&2
  exit 2
fi
load_dotenv_file "$ENV_FILE"

missing_env() {
  echo "foundation-r2-lakehouse-live: $1 is required" >&2
  exit 2
}

require_env() {
  local name="$1"
  [ -n "${!name:-}" ] || missing_env "$name"
}

require_env FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET
require_env FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
require_env FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY
require_env FOUNDATION_PLATFORM_RUNTIME_ENV
require_env FOUNDATION_PLATFORM_EXECUTION_CONTEXT
require_env FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET
require_env FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM
require_env FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER
require_env FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI
require_env FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE
require_env FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN

case "${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT:-}:${FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID:-}" in
  :)
    missing_env "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT or FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID"
    ;;
esac

case "$FOUNDATION_PLATFORM_RUNTIME_ENV:$FOUNDATION_PLATFORM_EXECUTION_CONTEXT" in
  local:developer|ci:ci) ;;
  production:developer)
    if [ "${FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION:-}" != "1" ]; then
      echo "foundation-r2-lakehouse-live: production requires FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION=1" >&2
      exit 2
    fi
    if [ "${FOUNDATION_PLATFORM_PRELAUNCH_SHARED:-}" != "1" ]; then
      echo "foundation-r2-lakehouse-live: production requires FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1" >&2
      exit 2
    fi
    ;;
  *)
    echo "foundation-r2-lakehouse-live: runtime/context must be local/developer, ci/ci, or explicitly approved production/developer" >&2
    exit 2
    ;;
esac

if [ "$FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM" != "1" ]; then
  echo "foundation-r2-lakehouse-live: FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM=1 is required" >&2
  exit 2
fi

if [ "$FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET" != "$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET" ]; then
  echo "foundation-r2-lakehouse-live: smoke bucket marker does not match R2 lakehouse bucket" >&2
  exit 2
fi

if [ "$FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER" != "r2_data_catalog" ]; then
  echo "foundation-r2-lakehouse-live: FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER must be r2_data_catalog" >&2
  exit 2
fi

command -v cargo >/dev/null 2>&1 || {
  echo "foundation-r2-lakehouse-live: cargo is required" >&2
  exit 2
}

export FOUNDATION_PLATFORM_R2_LIVE_SMOKE=1
export FOUNDATION_PLATFORM_R2_INVENTORY_LIVE_SMOKE=1
export FOUNDATION_PLATFORM_LAKEHOUSE_LIVE_SMOKE=1
export FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2

echo "foundation-r2-lakehouse-live: validating governed R2 runtime target"
cargo run --locked --manifest-path platforms/foundation-platform/Cargo.toml \
  -p foundation-outbox-publisher -- check-r2-runtime-target

echo "foundation-r2-lakehouse-live: running real Cloudflare R2 smoke and inventory"
cargo xtask integration foundation r2

echo "foundation-r2-lakehouse-live: running real R2 Data Catalog snapshot smoke"
cargo xtask integration foundation lakehouse

echo "foundation-r2-lakehouse-live: PASS (Cloudflare R2 and R2 Data Catalog)"
