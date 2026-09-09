#!/usr/bin/env bash
set -euo pipefail
# Fixtures below are disposable repositories. A hook runs with GIT_DIR pointing
# at the real checkout, which would redirect their commits into it; sourcing the
# shared definition releases that binding for the rest of this script.
. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/technology-version-consistency.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

# Fixture manifests state whatever the pin contract currently says (root ADR-0097): the
# self-test proves the checker's failure paths, not a second copy of the pin values.
pin() {
  python3 - "$root/tools/technology-versions.contract.json" "$1" <<'PY' | tr -d '\r'
import json
import sys

print(json.load(open(sys.argv[1], encoding="utf-8"))["manifest_exact_pins"][sys.argv[2]])
PY
}
node_pin="$(pin node)"
pnpm_pin="$(pin pnpm)"
package_manager_pin="$(pin packageManager)"
typescript_pin="$(pin typescript)"
vitest_pin="$(pin vitest)"

git -C "$fixture" init -q
git -C "$fixture" config user.email guard@example.test
git -C "$fixture" config user.name guard
mkdir -p "$fixture/platforms/foundation-platform/services/foundation-profile-gateway" "$fixture/products/gongzzang"
printf '%s\n' 'services:' '  postgres:' '    image: postgres:17-alpine@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' '  valkey:' '    image: valkey/valkey:8-alpine@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' > "$fixture/platforms/foundation-platform/docker-compose.yml"
printf '%s\n' '{' "  \"packageManager\": \"$package_manager_pin\"," '  "engines": {' "    \"node\": \"$node_pin\"," "    \"pnpm\": \"$pnpm_pin\"" '  }' '}' > "$fixture/products/gongzzang/package.json"
printf '%s\n' '{' "  \"packageManager\": \"$package_manager_pin\"," '  "engines": {' "    \"node\": \"$node_pin\"," "    \"pnpm\": \"$pnpm_pin\"" '  },' '  "devDependencies": {' "    \"typescript\": \"$typescript_pin\"," "    \"vitest\": \"$vitest_pin\"" '  }' '}' > "$fixture/platforms/foundation-platform/services/foundation-profile-gateway/package.json"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

sed -i 's/postgres:17/postgres:16/' "$fixture/platforms/foundation-platform/docker-compose.yml"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: postgres drift was accepted' >&2
  exit 1
fi

sed -i 's/postgres:16/postgres:17/' "$fixture/platforms/foundation-platform/docker-compose.yml"
sed -i 's#valkey/valkey:8#redis:7#' "$fixture/platforms/foundation-platform/docker-compose.yml"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: cache drift was accepted' >&2
  exit 1
fi

sed -i 's#redis:7#valkey/valkey:8#' "$fixture/platforms/foundation-platform/docker-compose.yml"
sed -i "s/\"node\": \"$node_pin\"/\"node\": \"0.0.1\"/" "$fixture/products/gongzzang/package.json"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: node drift was accepted' >&2
  exit 1
fi

sed -i "s/\"node\": \"0.0.1\"/\"node\": \"$node_pin\"/" "$fixture/products/gongzzang/package.json"
sed -i "s/\"typescript\": \"$typescript_pin\"/\"typescript\": \"0.0.1\"/" \
  "$fixture/platforms/foundation-platform/services/foundation-profile-gateway/package.json"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: Foundation Worker TypeScript drift was accepted' >&2
  exit 1
fi

echo 'OK technology-version-consistency-self-test'
