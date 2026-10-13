#!/usr/bin/env bash
# Proves xtask-path-coverage.sh refuses a filtered xtask workflow whose
# `paths:` list lost its tools/xtask/** entry even though the file still names
# tools/xtask elsewhere (the Rust cache `workspaces:`), which the guard used to
# accept; accepts the entry quoted or bare and a workflow with no path filter.
set -euo pipefail
cd "$(dirname "$0")/../.."

guard="$(pwd)/scripts/guard/xtask-path-coverage.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$test_root" ] || rm -rf -- "$test_root"
      ;;
    *) echo "xtask-path-coverage-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

# workflow <case> <paths entries...>: one filtered workflow that runs xtask and
# names tools/xtask in its cache, as every real one does.
workflow() {
  local dir="$test_root/$1" entry
  shift
  mkdir -p "$dir"
  {
    printf 'on:\n  push:\n'
    if [ "$#" -gt 0 ]; then
      printf '    paths:\n'
      for entry in "$@"; do
        printf '      - %s\n' "$entry"
      done
    fi
    printf 'jobs:\n  verify:\n    steps:\n'
    printf '      - uses: Swatinem/rust-cache@v2\n        with:\n          workspaces: |\n            tools/xtask\n'
    printf '      - run: cargo xtask verify area\n'
  } >"$dir/area-ci.yml"
  printf '%s\n' "$dir"
}

expect() {
  local verdict="$1" label="$2" dir="$3" rc=0
  bash "$guard" "$dir" >/dev/null 2>&1 || rc=$?
  if [ "$verdict" = accept ] && [ "$rc" -ne 0 ]; then
    echo "FAIL xtask-path-coverage-self-test: rejected $label" >&2
    exit 1
  fi
  if [ "$verdict" = reject ] && [ "$rc" -eq 0 ]; then
    echo "FAIL xtask-path-coverage-self-test: accepted $label" >&2
    exit 1
  fi
  echo "$verdict $label"
}

expect accept "a quoted tools/xtask/** entry" "$(workflow quoted '"products/area/**"' '"tools/xtask/**"')"
expect accept "a bare tools/xtask/** entry" "$(workflow bare 'products/area/**' 'tools/xtask/**')"
expect accept "a workflow with no path filter" "$(workflow unfiltered)"
expect reject "a filter without the entry, tools/xtask still in the cache" \
  "$(workflow missing '"products/area/**"')"
expect reject "a filter naming only one xtask file" \
  "$(workflow narrow '"products/area/**"' '"tools/xtask/Cargo.toml"')"

bash "$guard"
echo "OK xtask-path-coverage-self-test"
