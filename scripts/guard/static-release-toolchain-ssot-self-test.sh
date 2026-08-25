#!/usr/bin/env bash
set -euo pipefail

. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/static-release-toolchain-ssot.py"
contract="$root/platforms/foundation-platform/config/static-release-toolchain.contract.json"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

test -f "$checker" || {
  echo 'FAIL static-release-toolchain-ssot-self-test: checker is missing' >&2
  exit 1
}
test -f "$contract" || {
  echo 'FAIL static-release-toolchain-ssot-self-test: contract is missing' >&2
  exit 1
}

mkdir -p "$fixture/platforms/foundation-platform/config" "$fixture/scripts/tiles"
cp -- "$contract" "$fixture/platforms/foundation-platform/config/static-release-toolchain.contract.json"
printf '%s\n' \
  'echo "$PERFECTORY_MARTIN_IMAGE"' \
  'echo "PMTiles bbox=127.1230,36.1230 price=0.20"' \
  > "$fixture/scripts/tiles/consumer.sh"
printf '%s\n' 'export const image = process.env.PERFECTORY_MARTIN_IMAGE;' \
  > "$fixture/scripts/tiles/consumer.ts"
printf '%s\n' '-- unrelated SQL text' > "$fixture/scripts/tiles/consumer.sql"
printf '%s\n' '# Contract consumer' > "$fixture/scripts/tiles/consumer.md"

git -C "$fixture" init -q
git -C "$fixture" config user.email guard@example.invalid
git -C "$fixture" config user.name guard
git -C "$fixture" add .
python3 "$checker" --root "$fixture" >/dev/null

tool_name='martin-cp'
new_version='9.9.9'
printf '%s %s\n' "$tool_name" "$new_version" >> "$fixture/scripts/tiles/consumer.sh"
git -C "$fixture" add .
if python3 "$checker" --root "$fixture" >/dev/null 2>&1; then
  echo 'FAIL static-release-toolchain-ssot-self-test: a new hardcoded tool version was accepted' >&2
  exit 1
fi
printf '%s\n' 'echo "$PERFECTORY_MARTIN_IMAGE"' > "$fixture/scripts/tiles/consumer.sh"

printf '%s\n%s\n' "$tool_name" "$new_version" > "$fixture/scripts/tiles/consumer.md"
git -C "$fixture" add .
if python3 "$checker" --root "$fixture" >/dev/null 2>&1; then
  echo 'FAIL static-release-toolchain-ssot-self-test: a newline-split version was accepted' >&2
  exit 1
fi
printf '%s\n' '# Contract consumer' > "$fixture/scripts/tiles/consumer.md"

printf '%s%s%s\n' 'export const MARTIN_' 'VERSION = "' "$new_version\";" \
  > "$fixture/scripts/tiles/consumer.ts"
git -C "$fixture" add .
if python3 "$checker" --root "$fixture" >/dev/null 2>&1; then
  echo 'FAIL static-release-toolchain-ssot-self-test: a TypeScript version mirror was accepted' >&2
  exit 1
fi
printf '%s\n' 'export const image = process.env.PERFECTORY_MARTIN_IMAGE;' \
  > "$fixture/scripts/tiles/consumer.ts"

printf '%s %s %s\n' '-- pmtiles' 'version' "$new_version" \
  > "$fixture/scripts/tiles/consumer.sql"
git -C "$fixture" add .
if python3 "$checker" --root "$fixture" >/dev/null 2>&1; then
  echo 'FAIL static-release-toolchain-ssot-self-test: an SQL version mirror was accepted' >&2
  exit 1
fi

printf '%s\n' 'echo "$PERFECTORY_MARTIN_IMAGE"' > "$fixture/scripts/tiles/consumer.sh"
printf '%s\n' 'export const image = process.env.PERFECTORY_MARTIN_IMAGE;' \
  > "$fixture/scripts/tiles/consumer.ts"
printf '%s\n' '-- unrelated SQL text' > "$fixture/scripts/tiles/consumer.sql"
printf '%s\n' '# Contract consumer' > "$fixture/scripts/tiles/consumer.md"
digest="$(python3 - "$contract" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    contract = json.load(source)
print(next(iter(contract["distributions"].values()))["oci"]["digest"])
PY
)"
printf '%s\n' "pinned_digest=$digest" >> "$fixture/scripts/tiles/consumer.sh"
git -C "$fixture" add .
if python3 "$checker" --root "$fixture" >/dev/null 2>&1; then
  echo 'FAIL static-release-toolchain-ssot-self-test: a copied contract digest was accepted' >&2
  exit 1
fi

echo 'OK static-release-toolchain-ssot-self-test'
