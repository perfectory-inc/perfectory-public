#!/usr/bin/env bash
# Proves a-pull-request-undoes-no-merged-commit refuses the #1780 shape (a branch begun before a merged commit,
# squashed with `reset --soft` onto the newer main, so its one commit deletes what that commit added), and
# accepts the same branch rebased, the same branch never rebased, the merged lines moved to another file, a
# revert that names the commit by sha or by pull request, and a run with no pull request base.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"
guard="$(cd "$(dirname "$0")" && pwd)/a-pull-request-undoes-no-merged-commit.sh"
fixture_root
repo="$FIXTURE_ROOT/repo"
trap 'rm -rf "$FIXTURE_ROOT"' EXIT
mkdir -p "$repo"
fixture_git "$repo" init -q
fixture_git "$repo" config user.email guard@example.invalid
fixture_git "$repo" config user.name guard
commit() {
  fixture_git "$repo" add -A
  fixture_git "$repo" -c commit.gpgsign=false commit -q -m "$1"
}
printf 'download once\n' >"$repo/download.txt"
commit "the start"
start="$(fixture_git "$repo" rev-parse HEAD)"
# The merged commit (#1777's place): a rig of 25 lines, merged after the branch began.
seq 1 25 | sed 's/^/rig step /' >"$repo/rig.txt"
commit "feat: the dev rig (#1777)"
merged="$(fixture_git "$repo" rev-parse HEAD)"
# The branch (#1780's place), begun at the start: it changes the download only.
fixture_git "$repo" checkout -q --detach "$start"
printf 'download with retries\n' >"$repo/download.txt"
commit "fix: retry the download"
branch="$(fixture_git "$repo" rev-parse HEAD)"

# Builds a head from the branch by "$2"; runs the guard on it against the main that has the merged commit.
expect() {
  local want=$1 label=$2 how=$3 message=${4:-}
  case "$how" in
    squashed) # the incident: the branch's tree as one commit on the newer main
      fixture_git "$repo" checkout -q --detach "$branch"
      fixture_git "$repo" reset -q --soft "$merged"
      fixture_git "$repo" -c commit.gpgsign=false commit -q -m "fix: retry the download${message:+

$message}" ;;
    rebased)
      fixture_git "$repo" checkout -q --detach "$merged"
      fixture_git "$repo" -c commit.gpgsign=false cherry-pick "$branch" >/dev/null ;;
    stale)
      fixture_git "$repo" checkout -q --detach "$branch" ;;
    moved) # the merged commit's lines kept, in another file (#1786 moved #1775's rig into one stack)
      fixture_git "$repo" checkout -q --detach "$merged"
      fixture_git "$repo" mv rig.txt stack.txt
      fixture_git "$repo" -c commit.gpgsign=false commit -q -m "refactor: one stack" ;;
  esac
  local base=$merged
  [ "$label" != "no pull request base" ] || base=""
  if CHANGE_SCOPE_BASE="$base" CHANGE_SCOPE_HEAD=HEAD bash "$guard" "$repo" >"$FIXTURE_ROOT/out" 2>&1; then
    got=pass
  else
    got=fail
  fi
  if [ "$got" != "$want" ]; then
    cat "$FIXTURE_ROOT/out" >&2
    echo "FAIL a-pull-request-undoes-no-merged-commit-self-test: $label: expected $want, got $got" >&2
    exit 1
  fi
}

expect fail "squashed onto the newer main (#1780)" squashed
expect pass "rebased onto the newer main" rebased
expect pass "never rebased" stale
expect pass "the merged lines moved to another file" moved
expect pass "a revert that names the commit" squashed "Reverts ${merged:0:12}"
expect pass "a revert that names the pull request" squashed "Reverts #1777"
expect pass "git's own revert message" squashed "This reverts commit $merged."
expect pass "no pull request base" squashed
echo "OK a-pull-request-undoes-no-merged-commit-self-test"
