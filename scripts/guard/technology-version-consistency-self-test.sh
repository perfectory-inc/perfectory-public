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

git -C "$fixture" init -q
git -C "$fixture" config user.email guard@example.test
git -C "$fixture" config user.name guard
mkdir -p "$fixture/platforms/foundation-platform" "$fixture/products/gongzzang"
printf '%s\n' 'services:' '  postgres:' '    image: postgres:17-alpine@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' '  valkey:' '    image: valkey/valkey:8-alpine@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' > "$fixture/platforms/foundation-platform/docker-compose.yml"
printf '%s\n' '{' '  "packageManager": "pnpm@9.12.0",' '  "engines": {' '    "node": "20.19.0",' '    "pnpm": "9.12.0"' '  }' '}' > "$fixture/products/gongzzang/package.json"
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
sed -i 's/20\.19\.0/22.12.0/' "$fixture/products/gongzzang/package.json"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: node drift was accepted' >&2
  exit 1
fi

echo 'OK technology-version-consistency-self-test'
