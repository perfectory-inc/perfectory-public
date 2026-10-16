#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
package_dir="$(cd "${script_dir}/.." && pwd)"
node_bin="${NODE_BIN:-}"
if [ -z "$node_bin" ]; then
  if command -v node >/dev/null 2>&1; then
    node_bin="node"
  elif command -v node.exe >/dev/null 2>&1; then
    node_bin="node.exe"
  else
    printf 'not ok - node is required\n' >&2
    exit 1
  fi
fi
node_package_dir="$package_dir"
if [[ "$node_bin" == *".exe" ]] && command -v cygpath >/dev/null 2>&1; then
  node_package_dir="$(cygpath -w "$package_dir")"
elif [[ "$node_bin" == *".exe" && "$package_dir" =~ ^/mnt/([a-zA-Z])/(.*)$ ]]; then
  drive="${BASH_REMATCH[1]}"
  rest="${BASH_REMATCH[2]}"
  node_package_dir="${drive^^}:/${rest}"
fi

schema_file="${package_dir}/generated/schema.ts"
committed="$(mktemp)"
trap 'rm -f "$committed"' EXIT
cp "$schema_file" "$committed"

if ! output="$("$node_bin" "$node_package_dir/node_modules/tsx/dist/cli.mjs" "$node_package_dir/scripts/generate.ts" 2>&1)"; then
  printf 'not ok - generate must succeed from services/gongzzang-api/openapi.json\n%s\n' "$output" >&2
  exit 1
fi

# The web derives its listing value lists from this file; a stale copy is a stale web.
if ! cmp -s "$committed" "$schema_file"; then
  printf 'not ok - generated/schema.ts is stale: run `pnpm --filter @gongzzang/api-types generate` and commit it\n' >&2
  diff "$committed" "$schema_file" >&2 || true
  exit 1
fi

printf 'api-types-generate-contract-ok\n'
