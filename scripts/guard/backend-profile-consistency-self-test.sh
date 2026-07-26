#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/backend-profile-consistency.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/platforms/foundation-platform" "$fixture/.github/workflows" "$fixture/docs" "$fixture/platforms/foundation-platform/docs/adr" "$fixture/platforms/foundation-platform/docs/runbooks"

write_profile() {
  local bucket="$1"
  local runtime="$2"
  local acknowledgement="$3"
  for file in "$fixture/platforms/foundation-platform/.env.example" "$fixture/platforms/foundation-platform/.env.local.example"; do
    printf '%s\n' \
      "FOUNDATION_PLATFORM_RUNTIME_ENV=$runtime" \
      'FOUNDATION_PLATFORM_EXECUTION_CONTEXT=developer' \
      "FOUNDATION_PLATFORM_PRELAUNCH_SHARED=$acknowledgement" \
      "R2_BUCKET_NAME=$bucket" \
      'FOUNDATION_PLATFORM_OBJECT_STORAGE_DRIVER=r2' \
      'FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER=r2' > "$file"
  done
}

write_profile foundation-platform-lakehouse-prod production 1
printf '%s\n' \
  'FOUNDATION_PLATFORM_RUNTIME_ENV: ci' \
  'FOUNDATION_PLATFORM_EXECUTION_CONTEXT: ci' \
  'FOUNDATION_PLATFORM_RUNTIME_ENV: ci' \
  'FOUNDATION_PLATFORM_EXECUTION_CONTEXT: ci' > "$fixture/.github/workflows/foundation-ci.yml"
printf '%s\n' \
  'FOUNDATION_PLATFORM_PRELAUNCH_SHARED' > \
  "$fixture/docs/technology-stack.md"
cp "$fixture/docs/technology-stack.md" "$fixture/platforms/foundation-platform/docs/adr/0029-runtime-environment-backend-separation.md"
cp "$fixture/docs/technology-stack.md" "$fixture/platforms/foundation-platform/docs/runbooks/runtime-environment-separation.md"
bash "$checker" "$fixture" >/dev/null

sed -i 's/foundation-platform-lakehouse-prod/foundation-platform-lakehouse-dev/' \
  "$fixture/platforms/foundation-platform/.env.example"
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL backend-profile-consistency-self-test: mixed production/dev tuple was accepted' >&2
  exit 1
fi

write_profile foundation-platform-lakehouse-dev local 0
bash "$checker" "$fixture" >/dev/null

echo 'OK backend-profile-consistency-self-test'
