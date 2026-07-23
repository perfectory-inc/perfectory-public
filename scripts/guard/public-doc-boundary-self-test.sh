#!/usr/bin/env bash
# Proves private direction notes and competitive roadmaps cannot re-enter the
# public tracked tree without an explicit maintained-contract review marker.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
checker="$root/scripts/guard/public-doc-boundary.sh"
[ -f "$checker" ] || {
  echo "FAIL public-doc-boundary-self-test: missing $checker" >&2
  exit 1
}

test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) [ ! -e "$test_root" ] || rm -rf -- "$test_root" ;;
    *) echo "public-doc-boundary-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

git -C "$test_root" init --quiet
git -C "$test_root" config user.name Synthetic
git -C "$test_root" config user.email synthetic@example.invalid
mkdir -p "$test_root/docs/architecture" "$test_root/products/example/docs"
cat >"$test_root/docs/architecture/2026-07-23-reviewed-contract.md" <<'MARKDOWN'
<!-- public-repository-safety: reviewed-public-contract -->
# Maintained public contract
MARKDOWN
printf '# Architecture\n' >"$test_root/docs/architecture/README.md"
git -C "$test_root" add .
bash "$checker" "$test_root" >/dev/null

assert_rejected() {
  local label="$1"
  local expected="$2"
  local output status
  set +e
  output="$(bash "$checker" "$test_root" 2>&1)"
  status=$?
  set -e
  if [ "$status" -eq 0 ] || ! printf '%s\n' "$output" | grep -Fq "$expected"; then
    echo "FAIL public-doc-boundary-self-test: $label was not rejected as expected" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
}

printf '# Private direction note\n' \
  >"$test_root/docs/architecture/2026-07-23-private-direction.md"
git -C "$test_root" add .
assert_rejected unreviewed-dated-note "reviewed-public-contract"
git -C "$test_root" rm -q -f docs/architecture/2026-07-23-private-direction.md

printf '# Competitive roadmap\n' >"$test_root/products/example/docs/roadmap.md"
git -C "$test_root" add .
assert_rejected roadmap "competitive roadmap"
git -C "$test_root" rm -q -f products/example/docs/roadmap.md

mkdir -p "$test_root/products/example/docs"
printf '# Active queue\n' >"$test_root/products/example/docs/next-actions.md"
git -C "$test_root" add .
assert_rejected next-actions "competitive roadmap"

git -C "$test_root" rm -q -f products/example/docs/next-actions.md
mkdir -p "$test_root/products/example/docs"
printf '# Tracked then removed from candidate\n' >"$test_root/products/example/docs/roadmap.md"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m synthetic-roadmap
rm -f -- "$test_root/products/example/docs/roadmap.md"
bash "$checker" "$test_root" >/dev/null

echo "OK public-doc-boundary-self-test"
