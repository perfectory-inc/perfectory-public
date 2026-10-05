#!/usr/bin/env bash
# Planted violations for the three SP10 panel guards, in a fixture shaped like the monorepo:
# the area lives at products/gongzzang/ and the guard runs from there, as lefthook's `root:`
# and the CI step both do. Until 2026-10-05 the staged mode read repository-root paths and
# so never matched `^apps/web/` — every commit passed, and a framework->kind import sat on
# main unseen. Each case must be rejected in the staged mode and in the `--all` mode.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# A hook hands its children GIT_DIR pointing at the real repository; a fixture built under
# that binding writes into it (root scripts/guard/lib/fixture-repo.sh has the history).
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX

tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT

failures=0

# fixture <name>: a fresh monorepo-shaped repository with one clean commit.
fixture() {
  local repo="$tmp_root/$1"
  mkdir -p "$repo/products/gongzzang/apps/web/lib/panel" "$repo/products/gongzzang/apps/web/stores"
  git -C "$repo" init -q
  git -C "$repo" config user.email fixture@example.invalid
  git -C "$repo" config user.name fixture
  printf 'export const ok = 1;\n' >"$repo/products/gongzzang/apps/web/lib/panel/ok.ts"
  git -C "$repo" add -A
  git -C "$repo" commit -q -m clean
  printf '%s\n' "$repo/products/gongzzang"
}

# expect <guard> <mode> <want: pass|fail> <area dir>
expect() {
  local guard="$1" mode="$2" want="$3" area="$4" got
  # shellcheck disable=SC2086 # an empty mode means "no argument" (the staged mode)
  if (cd "$area" && bash "$script_dir/$guard" $mode >/dev/null 2>&1); then got=pass; else got=fail; fi
  if [ "$got" != "$want" ]; then
    echo "FAIL $guard ${mode:-staged}: expected $want, got $got" >&2
    failures=$((failures + 1))
  fi
}

# plant <guard> <relative file> <content>: stage a violation, then check both modes.
plant() {
  local guard="$1" file="$2" content="$3" area
  area="$(fixture "${guard%.sh}")"
  expect "$guard" "" pass "$area"
  expect "$guard" --all pass "$area"
  mkdir -p "$area/$(dirname "$file")"
  printf '%s\n' "$content" >"$area/$file"
  git -C "$area" add -A
  expect "$guard" "" fail "$area"
  git -C "$area" commit -q -m planted
  expect "$guard" --all fail "$area"
}

plant panel-no-framework-import-kind.sh apps/web/lib/panel/renderer.tsx \
  'import "@/components/panels/parcel/register";'
plant panel-no-direct-codec.sh apps/web/components/stack.ts \
  "export const parts = (s: string) => s.split('>');"
plant panel-no-state-without-router.sh apps/web/stores/panel.ts \
  'export const panelStack = [];'

if [ "$failures" -ne 0 ]; then
  exit 1
fi
echo "OK panel-guards.tests"
