#!/usr/bin/env bash
# Self-test for the Cloudflare R2/R2 Data Catalog live-lane fail-closed guard.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
unset \
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET \
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT \
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID \
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID \
  FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY \
  FOUNDATION_PLATFORM_RUNTIME_ENV \
  FOUNDATION_PLATFORM_EXECUTION_CONTEXT \
  FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET \
  FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM \
  FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION \
  FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI \
  FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE \
  FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN \
  FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER \
  FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER

empty_dotenv="$(mktemp)"
trap 'rm -f "$empty_dotenv"' EXIT
export FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE="$empty_dotenv"

set +e
output="$(bash "$ROOT/scripts/verify/foundation-r2-lakehouse-live.sh" 2>&1)"
status=$?
set -e

if [ "$status" -ne 2 ]; then
  echo "FAIL foundation-r2-lakehouse-live: expected exit 2, got $status" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

case "$output" in
  *"FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"*) ;;
  *)
    echo "FAIL foundation-r2-lakehouse-live: missing-variable error is not explicit" >&2
    printf '%s\n' "$output" >&2
    exit 1
    ;;
esac

echo "OK foundation-r2-lakehouse-live fail-closed self-test"

dotenv_fixture="$(mktemp)"
trap 'rm -f "$empty_dotenv" "$dotenv_fixture"' EXIT
cat >"$dotenv_fixture" <<'EOF'
FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev
FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=https://r2.example.invalid
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID=test-access
FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=test-secret
FOUNDATION_PLATFORM_RUNTIME_ENV=local
FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET=foundation-platform-lakehouse-dev
FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM=1
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=iceberg_rest
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=https://catalog.example.invalid
FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=test-warehouse
FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN=test-token
EOF
export FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE="$dotenv_fixture"
set +e
output="$(bash "$ROOT/scripts/verify/foundation-r2-lakehouse-live.sh" 2>&1)"
status=$?
set -e
if [ "$status" -ne 2 ] || [[ "$output" != *"must be r2_data_catalog"* ]]; then
  echo "FAIL foundation-r2-lakehouse-live: dotenv file was not loaded" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
export FOUNDATION_PLATFORM_R2_LIVE_ENV_FILE="$empty_dotenv"
echo "OK foundation-r2-lakehouse-live dotenv loading"

export FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-prod
export FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET=foundation-platform-lakehouse-prod
export FOUNDATION_PLATFORM_RUNTIME_ENV=production
export FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
export FOUNDATION_PLATFORM_PRELAUNCH_SHARED=1
export FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM=1
export FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=https://r2.example.invalid
export FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID=test-access
export FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=test-secret
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=r2_data_catalog
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=https://catalog.example.invalid
export FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=test-warehouse
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN=test-token
set +e
output="$(bash "$ROOT/scripts/verify/foundation-r2-lakehouse-live.sh" 2>&1)"
status=$?
set -e
if [ "$status" -ne 2 ] || [[ "$output" != *"FOUNDATION_PLATFORM_R2_LIVE_ALLOW_PRODUCTION=1"* ]]; then
  echo "FAIL foundation-r2-lakehouse-live: production lane was not explicitly gated" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi
echo "OK foundation-r2-lakehouse-live production gate"

export FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET=foundation-platform-lakehouse-dev
unset FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID
export FOUNDATION_PLATFORM_R2_LIVE_SMOKE_BUCKET=foundation-platform-lakehouse-dev
export FOUNDATION_PLATFORM_RUNTIME_ENV=local
export FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer
export FOUNDATION_PLATFORM_R2_LIVE_WRITE_CONFIRM=1
export FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2
export FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID=test-access
export FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY=test-secret
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=r2_data_catalog
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=https://catalog.example.invalid
export FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE=test-warehouse
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN=test-token

set +e
output="$(bash "$ROOT/scripts/verify/foundation-r2-lakehouse-live.sh" 2>&1)"
status=$?
set -e
if [ "$status" -ne 2 ] || [[ "$output" != *"ENDPOINT or FOUNDATION_PLATFORM_R2_LAKEHOUSE_ACCOUNT_ID"* ]]; then
  echo "FAIL foundation-r2-lakehouse-live: endpoint/account guard did not fail closed" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

export FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT=https://r2.example.invalid

unset FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID
set +e
output="$(bash "$ROOT/scripts/verify/foundation-r2-lakehouse-live.sh" 2>&1)"
status=$?
set -e
if [ "$status" -ne 2 ] || [[ "$output" != *"FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID"* ]]; then
  echo "FAIL foundation-r2-lakehouse-live: credential guard did not fail closed" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

export FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID=test-access
export FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER=iceberg_rest
set +e
output="$(bash "$ROOT/scripts/verify/foundation-r2-lakehouse-live.sh" 2>&1)"
status=$?
set -e
if [ "$status" -ne 2 ] || [[ "$output" != *"must be r2_data_catalog"* ]]; then
  echo "FAIL foundation-r2-lakehouse-live: provider guard did not fail closed" >&2
  printf '%s\n' "$output" >&2
  exit 1
fi

echo "OK foundation-r2-lakehouse-live endpoint/provider guards"
