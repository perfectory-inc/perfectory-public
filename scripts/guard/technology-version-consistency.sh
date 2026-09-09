#!/usr/bin/env bash
# Prevents runtime drift between local Compose files and the canonical role matrix.
# Local/CI/staging/production may change endpoints, credentials, tenancy, and capacity,
# but a different runtime version requires an explicit migration ADR.
#
# The JS manifest pins are not spelled here: they live once in
# tools/technology-versions.contract.json and this guard reads them (root ADR-0097). The pin
# values are read from the guard's own repository, so the self-test's fixture repositories are
# checked against the real pins rather than fixture copies.
set -euo pipefail

script_repo_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
version_pins_contract="$script_repo_root/tools/technology-versions.contract.json"
if [ ! -f "$version_pins_contract" ]; then
  echo "FAIL technology-version: missing $version_pins_contract" >&2
  exit 1
fi

read_pin() {
  python3 - "$version_pins_contract" "$1" <<'PY' | tr -d '\r'
import json
import sys

pins = json.load(open(sys.argv[1], encoding="utf-8"))["manifest_exact_pins"]
value = pins[sys.argv[2]]
if not isinstance(value, str) or value.strip() == "":
    raise SystemExit(f"pin {sys.argv[2]} must be a non-empty string")
print(value)
PY
}

node_pin="$(read_pin node)"

root="${1:-$script_repo_root}"
cd "$root"

fail=0
canonical_postgres='image: postgres:17-alpine@sha256:'
canonical_postgis='image: postgis/postgis:17-3.5'
canonical_valkey='image: valkey/valkey:8-alpine@sha256:'

compose_images=$(git ls-files \
  '*docker-compose*.yml' '*docker-compose*.yaml' '*compose*.yml' '*compose*.yaml' \
  '*Dockerfile' '*Dockerfile.*' 'scripts/tiles/compose.yaml' '.github/workflows/*.yml' '.github/workflows/*.yaml' |
  xargs -r grep -nE '^[[:space:]]*image: (postgres:|postgis/postgis:|redis:|valkey/valkey:)' || true)

if [ -n "$compose_images" ]; then
  while IFS= read -r line; do
    case "$line" in
      *"$canonical_postgres"*|*"$canonical_postgis"*|*"$canonical_valkey"*) ;;
      *)
        echo "FAIL technology-version: non-canonical database/cache image:" >&2
        echo "  $line" >&2
        fail=1
        ;;
    esac
  done <<< "$compose_images"
fi

package_files=$( {
  git ls-files 'products/gongzzang/**/package.json'
  git ls-files --error-unmatch 'products/gongzzang/package.json' 2>/dev/null || true
  git ls-files 'platforms/foundation-platform/**/package.json'
} | sort -u )
if [ -n "$package_files" ]; then
  check_exact_manifest_value() {
    local key="$1"
    local expected="$2"
    local lines
    lines=$(printf '%s\n' "$package_files" | xargs -r grep -nE "^[[:space:]]*\\\"${key}\\\":[[:space:]]*\\\"" || true)
    if [ -n "$lines" ] && printf '%s\n' "$lines" | grep -Fv "\"${key}\": \"${expected}\"" >/dev/null; then
      echo "FAIL technology-version: ${key} must be ${expected}:" >&2
      printf '%s\n' "$lines" | grep -Fv "\"${key}\": \"${expected}\"" >&2 || true
      fail=1
    fi
  }

  while IFS=$'\t' read -r pin_key pin_value; do
    check_exact_manifest_value "$pin_key" "$pin_value"
  done < <(python3 - "$version_pins_contract" <<'PY' | tr -d '\r'
import json
import sys

pins = json.load(open(sys.argv[1], encoding="utf-8"))["manifest_exact_pins"]
for key, value in pins.items():
    if not isinstance(value, str) or value.strip() == "":
        raise SystemExit(f"pin {key} must be a non-empty string")
    print(f"{key}\t{value}")
PY
)
fi

if [ -f products/gongzzang/.nvmrc ]; then
  nvmrc=$(tr -d '\r\n' < products/gongzzang/.nvmrc)
  if [ "$nvmrc" != "$node_pin" ]; then
    echo "FAIL technology-version: products/gongzzang/.nvmrc must pin $node_pin (found $nvmrc)" >&2
    fail=1
  fi
fi

workflow_node_versions=$(git grep -n -E '^[[:space:]]*node-version:' -- '.github/workflows/*.yml' '.github/workflows/*.yaml' || true)
if [ -n "$workflow_node_versions" ] && printf '%s\n' "$workflow_node_versions" | grep -Fv "node-version: \"$node_pin\"" >/dev/null; then
  echo "FAIL technology-version: CI node-version must be $node_pin:" >&2
  printf '%s\n' "$workflow_node_versions" | grep -Fv "node-version: \"$node_pin\"" >&2 || true
  fail=1
fi

# The verification image layers onto the harness's pinned toolchain, so its `FROM` and
# `RUST_TOOLCHAIN_IMAGE` are the same fact written twice. The digest has to be literal in the
# Dockerfile — `check-container-runtime-policy.sh` refuses a reference it cannot read, and a build
# argument is exactly that — so the copy is enforced here instead of avoided. Drift between the two
# would verify against a toolchain other than the one the harness runs everything else with.
verify_dockerfile=tools/verify-image/Dockerfile
pin_file=tools/container-images.env
if [ -f "$verify_dockerfile" ] && [ -f "$pin_file" ]; then
  pinned_toolchain=$(sed -n 's/^RUST_TOOLCHAIN_IMAGE=//p' "$pin_file" | tr -d '\r')
  verify_rust_base=$(sed -n 's/^FROM[[:space:]]\{1,\}\([^[:space:]]\{1,\}\).*/\1/p' "$verify_dockerfile" | grep '^rust:' | head -1)
  if [ -z "$pinned_toolchain" ] || [ -z "$verify_rust_base" ]; then
    echo "FAIL technology-version: could not read the toolchain pin or the verification image base" >&2
    fail=1
  elif [ "$pinned_toolchain" != "$verify_rust_base" ]; then
    echo 'FAIL technology-version: the verification image base does not match the pinned toolchain:' >&2
    echo "  $pin_file:           $pinned_toolchain" >&2
    echo "  $verify_dockerfile:  $verify_rust_base" >&2
    fail=1
  fi

  pinned_node=$(sed -n 's/^NODE_VERIFY_IMAGE=//p' "$pin_file" | tr -d '\r')
  verify_node_base=$(sed -n 's/^FROM[[:space:]]\{1,\}\([^[:space:]]\{1,\}\).*/\1/p' "$verify_dockerfile" | grep '^node:' | head -1)
  if [ -z "$pinned_node" ] || [ -z "$verify_node_base" ]; then
    echo "FAIL technology-version: could not read the Node pin or verification image Node stage" >&2
    fail=1
  elif [ "$pinned_node" != "$verify_node_base" ]; then
    echo 'FAIL technology-version: verification Node stage does not match NODE_VERIFY_IMAGE:' >&2
    echo "  $pin_file:           $pinned_node" >&2
    echo "  $verify_dockerfile:  $verify_node_base" >&2
    fail=1
  fi
fi

if [ "$fail" -eq 0 ]; then
  echo 'OK technology-version-consistency'
fi
exit "$fail"
