#!/usr/bin/env bash
# Proves a-release-keeps-the-jobs-it-switches-off refuses a pull request that deletes an enabled job
# (the entry, or its whole definition), and accepts one that switches the job off, one that deletes
# a job already off, one that adds a job, and a run with no pull request base.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"
guard="$(cd "$(dirname "$0")" && pwd)/a-release-keeps-the-jobs-it-switches-off.sh"
fixture_root
repo="$FIXTURE_ROOT/repo"
trap 'rm -rf "$FIXTURE_ROOT"' EXIT
dir=platforms/foundation-platform/definitions
mkdir -p "$repo/$dir"
fixture_git "$repo" init -q
fixture_git "$repo" config user.email guard@example.invalid
fixture_git "$repo" config user.name guard
commit() {
  fixture_git "$repo" add -A
  fixture_git "$repo" -c commit.gpgsign=false commit -q -m fixture
}
on='{"id": "lane_rebuild", "enabled": true}'
off='{"id": "lane_rebuild", "enabled": false}'
old='{"id": "old_rebuild", "enabled": false}'
printf '{"jobs": [%s, %s]}\n' "$on" "$old" >"$repo/$dir/lane.definition.json"
printf '{"jobs": [{"id": "other_load", "enabled": true}]}\n' >"$repo/$dir/other.definition.json"
commit
base="$(fixture_git "$repo" rev-parse HEAD)"

# Starts a pull request from the base, applies "$@", commits it, and runs the guard on it.
expect() {
  local want=$1 label=$2 compare=$3
  shift 3
  fixture_git "$repo" checkout -q --detach "$base"
  (cd "$repo" && "$@")
  commit
  if CHANGE_SCOPE_BASE="$compare" CHANGE_SCOPE_HEAD=HEAD bash "$guard" "$repo" >"$FIXTURE_ROOT/out" 2>&1; then
    got=pass
  else
    got=fail
  fi
  if [ "$got" != "$want" ]; then
    cat "$FIXTURE_ROOT/out" >&2
    echo "FAIL a-release-keeps-the-jobs-it-switches-off-self-test: $label: expected $want, got $got" >&2
    exit 1
  fi
}

expect pass "a job switched off" "$base" \
  sh -c "printf '{\"jobs\": [%s, %s]}\n' '$off' '$old' >$dir/lane.definition.json"
expect pass "a job already off, deleted" "$base" \
  sh -c "printf '{\"jobs\": [%s]}\n' '$on' >$dir/lane.definition.json"
expect pass "a job added" "$base" \
  sh -c "printf '{\"jobs\": [{\"id\": \"new_build\", \"enabled\": true}]}\n' >$dir/new.definition.json"
expect pass "an enabled job moved to another definition" "$base" \
  sh -c "printf '{\"jobs\": [%s]}\n' '$old' >$dir/lane.definition.json; printf '{\"jobs\": [%s]}\n' '$on' >$dir/moved.definition.json"
expect fail "an enabled job deleted" "$base" \
  sh -c "printf '{\"jobs\": [%s]}\n' '$old' >$dir/lane.definition.json"
expect fail "the definition of an enabled job deleted" "$base" \
  rm "$dir/other.definition.json"
expect pass "no pull request base" "" \
  rm "$dir/other.definition.json"
echo "OK a-release-keeps-the-jobs-it-switches-off-self-test"
