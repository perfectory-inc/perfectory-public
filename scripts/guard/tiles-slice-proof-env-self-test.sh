#!/usr/bin/env bash
# Proves the tile proof loads its namespaced Foundation local R2 settings.
set -euo pipefail

dir="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$dir/../.." && pwd -P)"
proof="$root/scripts/tiles/tiles-slice-proof.sh"
home_dir="$(mktemp -d)"
trap 'rm -rf "$home_dir"' EXIT
fake_account='0123456789abcdef0123456789abcdef'

mkdir -p "$home_dir/foundation-platform"
printf '%s\n' \
  "export FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID='$fake_account'" \
  "export FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID='testaccess123'" \
  "export FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY='testsecret123'" \
  "export FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET='tiles-slice-proof-ci'" \
  "export FOUNDATION_PLATFORM_R2_TILE_PROOF_ENDPOINT='https://${fake_account}.r2.cloudflarestorage.com'" \
  "export FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_BASE_URL='https://r2.example.invalid'" \
  > "$home_dir/foundation-platform/.env.local"

unset R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_TILES_TEST_BUCKET_NAME \
  R2_ENDPOINT R2_TILES_READ_BASE_URL R2_TILES_READ_URL R2_TILES_OBJECT_KEY

output="$(TILES_SLICE_PROOF_ENV_FILE="$home_dir/foundation-platform/.env.local" HOME="$home_dir" bash "$proof" --validate-r2-config-only 2>&1)"
grep -Fq 'R2 configuration validation OK (REAL R2)' <<<"$output" \
  || { printf '%s\n' "$output" >&2; echo 'FAIL tiles-slice-proof-env-self-test: Foundation namespaced env was not loaded' >&2; exit 1; }

echo 'OK tiles-slice-proof-env-self-test'
