#!/usr/bin/env bash
# Proves each-guard-runs-once.py refuses a guard started twice per run (by the
# runner loop and another guard, or by two guards) and a loop entry that names no
# script, accepts a self-test running a guard on a fixture, then checks the tree.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="$(pwd)/scripts/guard/each-guard-runs-once.py"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$test_root" ] || rm -rf -- "$test_root"
      ;;
    *) echo "each-guard-runs-once-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

# fixture <name> <loop entries> then "<file>=<line>" pairs written into scripts/guard/.
fixture() {
  local name="$1" dir="$test_root/$1/scripts/guard" entries="$2" spec
  shift 2
  mkdir -p "$dir"
  printf 'for g in %s; do\n  bash "$dir/$g.sh" || rc=1\ndone\n' "$entries" >"$dir/monorepo-guard.sh"
  for entry in $entries; do
    printf '#!/usr/bin/env bash\n' >"$dir/$entry.sh"
  done
  for spec in "$@"; do
    printf '%s\n' "${spec#*=}" >>"$dir/${spec%%=*}"
  done
  printf '%s\n' "$test_root/$name"
}

expect() {
  local verdict="$1" label="$2" root="$3" rc=0
  python3 "$checker" "$root" >"$test_root/out" 2>&1 || rc=$?
  if [ "$verdict" = accept ] && [ "$rc" -ne 0 ]; then
    cat "$test_root/out" >&2
    echo "FAIL each-guard-runs-once-self-test: rejected $label" >&2
    exit 1
  fi
  if [ "$verdict" = reject ] && [ "$rc" -eq 0 ]; then
    echo "FAIL each-guard-runs-once-self-test: accepted $label" >&2
    exit 1
  fi
  echo "$verdict $label"
}

expect accept "a runner loop with distinct guards" \
  "$(fixture distinct "alpha beta")"
expect accept "a guard that alone starts its helper" \
  "$(fixture helper "alpha" 'alpha.sh=bash scripts/guard/check-alpha.sh .github/workflows')"
expect accept "a self-test running its guard on a fixture" \
  "$(fixture self-test "alpha alpha-self-test" 'alpha-self-test.sh=  bash "$root/alpha.sh"')"
expect reject "a guard the loop runs and another guard starts again" \
  "$(fixture loop-and-guard "alpha beta" 'alpha.sh=bash scripts/guard/beta.sh')"
expect reject "a helper started by two guards" \
  "$(fixture two-parents "alpha beta" 'alpha.sh=bash scripts/guard/check-x.sh' 'beta.sh=  bash "$dir/check-x.sh" || rc=1')"
expect reject "a python helper started by two guards" \
  "$(fixture two-python-parents "alpha beta" 'alpha.sh=python3 scripts/guard/check-x.py a' 'beta.sh=exec python3 scripts/guard/check-x.py b')"
expect reject "a loop entry that names no script" \
  "$(fixture missing "alpha" && rm "$test_root/missing/scripts/guard/alpha.sh")"

python3 "$checker"
echo "OK each-guard-runs-once-self-test"
