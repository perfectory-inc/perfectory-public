#!/usr/bin/env bash
# The repository's OSV gate (root ADR-0414, revision 2026-11-25).
#
#   osv-scan.sh               a pull request or a merge group (CHANGE_SCOPE_EVENT, CHANGE_SCOPE_BASE)
#                             is judged by OSV-Scanner's own PR mode: the pinned upstream image scans the
#                             tracked lockfiles at the change's base and at its head, each under its own
#                             osv-scanner.toml, and its osv-reporter fails only on what the head adds.
#                             Main (push, schedule) runs the full scan, `cargo xtask ci osv-gate`, whose
#                             `advisory` annotations page #alerts (root ADR-0278).
#   osv-scan.sh sarif <path>  the full scan of the checkout as SARIF, for code scanning; findings are
#                             not a failure, a scan that did not complete is.
#
# Upstream's reusable PR workflow checks out $GITHUB_BASE_REF, which GitHub sets only for pull
# requests: in a merge group it would compare the head with itself. This runs the same two binaries
# with the base commit the event names.
set -euo pipefail

OSV_IMAGE="ghcr.io/google/osv-scanner-action:v2.7.1@sha256:3fa25659351c9a5527b30d73b6f3c2ee0bcac106d6220948211d87379d08f243"
REPORT=target/supply-chain/osv-report.json
LOCKFILES=('*Cargo.lock' '*pnpm-lock.yaml' '*requirements.txt')

work="${RUNNER_TEMP:?RUNNER_TEMP is unset; this runs in a GitHub Actions job}/osv"
rm -rf "$work"
mkdir -p "$work/out"

# scanner <tree> <format> <result>: the tracked lockfiles of <tree> under <tree>/osv-scanner.toml.
# Exit 0 is nothing outside the allow list, 1 findings; anything else is a scan that did not complete.
scanner() {
  local tree="$1" format="$2" result="$3" status=0 lockfile
  local lockfiles=() args=()
  git -C "$tree" ls-files -z -- "${LOCKFILES[@]}" >"$work/lockfiles"
  mapfile -d '' -t lockfiles <"$work/lockfiles"
  if [[ ${#lockfiles[@]} -eq 0 ]]; then
    echo "::error::no tracked lockfile in $tree"
    return 1
  fi
  for lockfile in "${lockfiles[@]}"; do
    args+=("--lockfile=/src/$lockfile")
  done
  docker run --rm -v "$tree:/src:ro" -v "$work/out:/out" --entrypoint /root/osv-scanner "$OSV_IMAGE" \
    scan --config=/src/osv-scanner.toml --format="$format" --output-file="/out/$result" \
    "${args[@]}" || status=$?
  if [[ $status -gt 1 || ! -s "$work/out/$result" ]]; then
    echo "::error::OSV-Scanner did not complete on $tree (exit $status), so there is no verdict"
    return 1
  fi
}

# reporter <old> <new>: osv-reporter's diff; fails when <new> holds an occurrence <old> does not.
reporter() {
  local old=()
  [[ -z "$1" ]] || old=("--old=/out/$1")
  docker run --rm -v "$work/out:/out" --entrypoint /root/osv-reporter "$OSV_IMAGE" \
    "${old[@]}" --new="/out/$2" --output="/out/$2.sarif" --gh-annotations=true --fail-on-vuln=true
}

if [[ "${1:-}" == sarif ]]; then
  scanner "$PWD" sarif osv.sarif
  mkdir -p "${2%/*}"
  cp "$work/out/osv.sarif" "$2"
  exit 0
fi

case "${CHANGE_SCOPE_EVENT:-}" in
  pull_request | merge_group) ;;
  *) exec cargo xtask ci osv-gate --report-path "$REPORT" ;;
esac

# A gate that can no longer fail would pass every change: a planted advisory must be refused first
# (smallvec 1.6.0 is RUSTSEC-2021-0003, measured in root ADR-0414).
mkdir -p "$work/planted"
git -C "$work/planted" init --quiet
printf '%s\n' 'version = 3' '' '[[package]]' 'name = "smallvec"' 'version = "1.6.0"' \
  'source = "registry+https://github.com/rust-lang/crates.io-index"' >"$work/planted/Cargo.lock"
: >"$work/planted/osv-scanner.toml"
git -C "$work/planted" add Cargo.lock osv-scanner.toml
scanner "$work/planted" json planted.json || true
if reporter "" planted.json >/dev/null 2>&1; then
  echo "::error::OSV-Scanner's PR mode passed a planted advisory (smallvec 1.6.0), so it cannot judge this change"
  exit 1
fi

base="${CHANGE_SCOPE_BASE:?the change names no base commit}"
git cat-file -e "$base^{commit}" 2>/dev/null || git fetch --quiet --no-tags --depth=1 origin "$base"
git worktree add --detach --quiet "$work/base" "$base"
trap 'git worktree remove --force "$work/base"' EXIT

scanner "$work/base" json old.json
scanner "$PWD" json new.json
# The npm fix proposal (cargo xtask npm prepare-advisory-fix) reads the head's findings.
mkdir -p "${REPORT%/*}"
cp "$work/out/new.json" "$REPORT"
reporter old.json new.json
