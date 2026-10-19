#!/usr/bin/env bash
# Proves by-pnu-document-records-are-append-only refuses a pull request that modifies, renames or
# deletes a recorded by-PNU document version, and accepts one that adds a version, one that changes
# only files elsewhere, and a run with no pull request base.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"
guard="$(cd "$(dirname "$0")" && pwd)/by-pnu-document-records-are-append-only.sh"
fixture_root
repo="$FIXTURE_ROOT/repo"
trap 'rm -rf "$FIXTURE_ROOT"' EXIT
records="$repo/platforms/foundation-platform/contracts/by-pnu-documents"
mkdir -p "$records"
fixture_git "$repo" init -q
fixture_git "$repo" config user.email guard@example.invalid
fixture_git "$repo" config user.name guard
commit() {
  fixture_git "$repo" add -A
  fixture_git "$repo" -c commit.gpgsign=false commit -q -m fixture
}
printf '[\n  {"path": "pnu", "required": true}\n]\n' >"$records/doc.v1.json"
echo base >"$repo/README.md"
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
    echo "FAIL by-pnu-document-records-are-append-only-self-test: $label: expected $want, got $got" >&2
    exit 1
  fi
}
dir=platforms/foundation-platform/contracts/by-pnu-documents

expect pass "a new version's record" "$base" \
  sh -c "printf '[]\n' >$dir/doc.v2.json"
expect pass "a change elsewhere" "$base" \
  sh -c 'echo changed >README.md'
expect fail "an edited record" "$base" \
  sh -c "printf '[]\n' >$dir/doc.v1.json"
expect fail "a renamed record" "$base" \
  mv "$dir/doc.v1.json" "$dir/doc.v3.json"
expect fail "a deleted record" "$base" \
  rm "$dir/doc.v1.json"
expect pass "no pull request base" "" \
  sh -c "printf '[]\n' >$dir/doc.v1.json"
echo "OK by-pnu-document-records-are-append-only-self-test"
