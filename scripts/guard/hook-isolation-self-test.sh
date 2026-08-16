#!/usr/bin/env bash
# Proves a guard cannot commit into the repository that is running it.
#
# On 2026-08-16 a `git push` ran the pre-push hook and came back with five
# `fixture` commits on the branch being pushed. One of them deleted 2,169 files,
# leaving a head tree of `crate/` and `scripts/`. Because that tree contained no
# .github/workflows, the pull request matched no workflow and reported zero CI
# runs -- the outage was read as a GitHub permissions problem for half an hour
# before the hook was suspected.
#
# The cause was environmental, not textual: git starts a hook with GIT_DIR set to
# the real repository and GIT_WORK_TREE unset, so a self-test's `git -C "$tmp"`
# addressed this checkout with $tmp as its working tree. `mktemp -d` isolated the
# filesystem; nothing isolated the git environment.
#
# WHAT THIS TEST ASSERTS, AND WHY IT CANNOT PASS VACUOUSLY
#
# Every case below runs against a *canary* repository -- a throwaway stand-in for
# the real checkout, complete with a .github/workflows file -- while the
# environment is poisoned exactly the way git poisons a hook's. Case 1 runs an
# unprotected fixture builder, written in the shape the guards used before the
# fix, and REQUIRES the canary to be destroyed. If the incident ever stops
# reproducing there, the poison no longer models a real hook and every later case
# is meaningless, so case 1 failing is itself a failure. The remaining cases then
# demand that the protected forms leave the canary untouched.
set -euo pipefail

# Read before anything scrubs it: whatever ran this script must not have handed
# us a repository binding. Asserted below, but only under the runner -- run by
# hand from a hook, or by the sweep, the binding is expected.
inherited_binding=""
for inherited_name in GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR \
                      GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES; do
  inherited_value="${!inherited_name:-}"
  if [ -n "$inherited_value" ]; then
    inherited_binding="$inherited_binding $inherited_name=$inherited_value"
  fi
done

. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
library="$root/scripts/guard/lib/fixture-repo.sh"
failures=0

report_failure() {
  echo "FAIL hook-isolation-self-test: $*" >&2
  failures=$((failures + 1))
}

fixture_root
test_root="$FIXTURE_ROOT"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
  if [ -n "${in_repository_probe:-}" ] && [ -d "$in_repository_probe" ]; then
    rm -rf -- "$in_repository_probe"
  fi
}
trap cleanup EXIT

# A stand-in for the real checkout: tracked files in several directories, one of
# them the workflow directory whose disappearance silenced CI.
#
# Reports through $CANARY for the same reason `fixture_root` does: called as
# `$(new_canary)` the serial would be incremented inside a subshell, every case
# would be handed the same directory, and the second `git commit` would find
# nothing to commit.
canary_serial=0
CANARY=""
new_canary() {
  canary_serial=$((canary_serial + 1))
  local canary="$test_root/canary$canary_serial"
  mkdir -p "$canary/.github/workflows" "$canary/platforms/foundation" "$canary/docs"
  printf 'name: required\non: [push]\n' >"$canary/.github/workflows/ci.yml"
  printf 'name: docs\non: [push]\n' >"$canary/.github/workflows/docs.yml"
  printf '# repository\n' >"$canary/README.md"
  printf 'fn main() {}\n' >"$canary/platforms/foundation/main.rs"
  printf '# design\n' >"$canary/docs/design.md"
  fixture_git "$canary" init -q
  fixture_git "$canary" config user.email canary@example.invalid
  fixture_git "$canary" config user.name canary
  fixture_git "$canary" add -A
  fixture_git "$canary" -c commit.gpgsign=false commit -q -m "canary baseline"
  CANARY="$canary"
}

# Everything an intruding commit would move: the branch tip, the index, the
# tracked set, and the identity the fixture commits were authored with.
canary_state() {
  local canary="$1"
  fixture_git "$canary" rev-parse HEAD
  fixture_git "$canary" status --porcelain
  fixture_git "$canary" config user.email
  fixture_git "$canary" ls-files
}

# The environment git hands a pre-push hook, reproduced: GIT_DIR names the
# repository, GIT_WORK_TREE is unset, so git takes the current directory as the
# top of the working tree.
run_poisoned() {
  local canary="$1" script="$2"
  (
    cd "$test_root"
    GIT_DIR="$canary/.git" bash "$script"
  ) >/dev/null 2>&1
}

# The fixture builder exactly as the guards wrote it before the fix.
unprotected_probe="$test_root/unprotected-probe.sh"
cat >"$unprotected_probe" <<'PROBE'
#!/usr/bin/env bash
set -euo pipefail
dir="$(mktemp -d)"
mkdir -p "$dir/crate/src" "$dir/crate0"
printf 'pub fn main() {}\n' >"$dir/crate/src/lib.rs"
printf 'fn main() {}\n' >"$dir/crate0/build.rs"
git -C "$dir" init -q
git -C "$dir" config user.email guard@example.invalid
git -C "$dir" config user.name guard
git -C "$dir" add -A
git -C "$dir" -c commit.gpgsign=false commit -q -m fixture
PROBE

# The same builder, adopting the shared definition -- the one-line change the
# other guards now carry.
protected_probe="$test_root/protected-probe.sh"
cat >"$protected_probe" <<PROBE
#!/usr/bin/env bash
set -euo pipefail
. "$library"
fixture_root
dir="\$FIXTURE_ROOT"
mkdir -p "\$dir/crate/src" "\$dir/crate0"
printf 'pub fn main() {}\n' >"\$dir/crate/src/lib.rs"
printf 'fn main() {}\n' >"\$dir/crate0/build.rs"
fixture_git "\$dir" init -q
fixture_git "\$dir" config user.email guard@example.invalid
fixture_git "\$dir" config user.name guard
fixture_git "\$dir" add -A
fixture_git "\$dir" -c commit.gpgsign=false commit -q -m fixture
# The fixture must really exist; isolation that works by doing nothing is not
# isolation.
test "\$(fixture_git "\$dir" rev-list --count HEAD)" = 1
test -n "\$(fixture_git "\$dir" ls-files)"
PROBE

# --- case 0: the runner must not hand its children a repository binding -------
if [ -n "${PERFECTORY_GUARD_RUNNER:-}" ] && [ -n "$inherited_binding" ]; then
  report_failure "run by ${PERFECTORY_GUARD_RUNNER} with a repository binding in the environment:${inherited_binding}"
fi

# --- case 1: positive control -- the incident must still reproduce ------------
new_canary
canary="$CANARY"
before="$(canary_state "$canary")"
run_poisoned "$canary" "$unprotected_probe" || true
after="$(canary_state "$canary")"
if [ "$before" = "$after" ]; then
  report_failure "positive control did not reproduce the incident: an unprotected fixture builder left the canary untouched, so this test proves nothing. Check that the poisoned environment still models a git hook."
else
  destroyed="$(fixture_git "$canary" ls-tree -r --name-only HEAD | wc -l)"
  survived_workflows="$(fixture_git "$canary" ls-tree -r --name-only HEAD | grep -c '^\.github/workflows/' || true)"
  echo "  hook-isolation-self-test: positive control reproduced the incident (canary head now holds $destroyed files, $survived_workflows of them workflows)"
fi

# --- case 2: the adopted form must leave the canary alone ---------------------
new_canary
canary="$CANARY"
before="$(canary_state "$canary")"
if ! run_poisoned "$canary" "$protected_probe"; then
  report_failure "the protected fixture builder failed to run under a poisoned environment"
fi
after="$(canary_state "$canary")"
if [ "$before" != "$after" ]; then
  report_failure "the protected fixture builder still mutated the canary repository"
fi

# --- cases 3-5: targeting anything but a fixture must be refused --------------
if ( fixture_git "" init -q ) >/dev/null 2>&1; then
  report_failure "fixture_git accepted an empty target; git treats \`-C ''\` as the current directory"
fi
if ( fixture_git "$root" status ) >/dev/null 2>&1; then
  report_failure "fixture_git accepted the repository root as a target"
fi
outside="$(mktemp -d)"
if ( fixture_git "$outside" init -q ) >/dev/null 2>&1; then
  report_failure "fixture_git accepted '$outside', which no fixture_root handed out"
fi
rm -rf -- "$outside"

# --- case 6: a temporary directory inside the checkout must be refused --------
in_repository_probe="$root/.hook-isolation-self-test-probe"
mkdir -p "$in_repository_probe"
if ( TMPDIR="$in_repository_probe" fixture_root ) >/dev/null 2>&1; then
  report_failure "fixture_root accepted a temporary directory inside the repository"
fi
rm -rf -- "$in_repository_probe"
in_repository_probe=""

# --- case 7: the guards that caused the incident, under the same poison -------
# Named individually because these two are the ones whose fixtures were found in
# the wreckage. The sweep below covers the rest when it is asked for.
for guard in build-coupling-baseline-self-test judgment-position-exit-codes-self-test; do
  new_canary
  canary="$CANARY"
  before="$(canary_state "$canary")"
  (
    cd "$root"
    GIT_DIR="$canary/.git" bash "$root/scripts/guard/$guard.sh"
  ) >/dev/null 2>&1 || report_failure "$guard failed under a poisoned environment"
  after="$(canary_state "$canary")"
  if [ "$before" != "$after" ]; then
    report_failure "$guard mutated the canary repository"
  fi
done

# --- case 8: every guard self-test, on request --------------------------------
# Off by default: the sweep re-runs the whole self-test suite and costs minutes,
# which the pre-push hook cannot absorb. Run it with
# PERFECTORY_HOOK_ISOLATION_SWEEP=1 after touching any fixture-building guard.
if [ "${PERFECTORY_HOOK_ISOLATION_SWEEP:-}" = "1" ]; then
  for guard in "$root"/scripts/guard/*-self-test.sh; do
    case "$(basename "$guard")" in
      hook-isolation-self-test.sh) continue ;;
    esac
    new_canary
    canary="$CANARY"
    before="$(canary_state "$canary")"
    (
      cd "$root"
      GIT_DIR="$canary/.git" bash "$guard"
    ) >/dev/null 2>&1 || true
    after="$(canary_state "$canary")"
    if [ "$before" != "$after" ]; then
      report_failure "$(basename "$guard") mutated the canary repository"
    else
      echo "  sweep ok: $(basename "$guard")"
    fi
  done
fi

if [ "$failures" -ne 0 ]; then
  echo "FAIL hook-isolation-self-test: $failures case(s) failed" >&2
  exit 1
fi
echo "OK hook-isolation-self-test"
