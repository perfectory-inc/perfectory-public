#!/usr/bin/env bash
# Turns a red OSV ratchet into a ready change (root ADR-0158): the contract floors the report
# asks for, every package.json re-rendered, every lockfile regenerated with the pinned
# toolchain, the overrides check, and a second OSV run to say whether the change closes the
# ratchet. The result is a patch and a summary in <out-dir>; nothing is committed or pushed.
#
# CI runs this after the ratchet fails and uploads <out-dir>; open-advisory-fix-pr.sh applies
# that artifact (or runs this locally) and opens the pull request.
#
# usage: bash tools/npm/prepare-advisory-fix.sh <osv-report.json> <out-dir>
# exit:  0 prepared, or nothing an override can fix (the summary says which); 1 on failure
set -euo pipefail

[ "$#" -eq 2 ] || {
  echo "usage: $0 <osv-report.json> <out-dir>" >&2
  exit 2
}
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
report="$1"
out="$2"
mkdir -p "$out"
out="$(cd "$out" && pwd -P)"
summary="$out/summary.md"
patch="$out/npm-advisory-fix.patch"
rm -f -- "$summary" "$patch"

if [ ! -s "$report" ]; then
  printf 'No OSV report at %s: the ratchet did not run, so there is nothing to prepare.\n' "$report" >"$summary"
  cat "$summary"
  exit 0
fi
if ! git -C "$root" diff --quiet || ! git -C "$root" diff --cached --quiet; then
  echo "FAIL prepare-advisory-fix: the working tree has changes; prepare from a clean checkout" >&2
  exit 1
fi

proposal="$out/proposal.txt"
propose_rc=0
node "$root/tools/npm/security-overrides.mjs" propose --root "$root" --osv-report "$report" --summary "$proposal" || propose_rc=$?
case "$propose_rc" in
  0) ;;
  3)
    {
      echo "The OSV ratchet failed, but no new npm advisory has a fixed release an override could pin."
      echo "It is a Cargo finding, an advisory without a fix (accept it in tools/osv-vulnerability-baseline.tsv with a reason), or a stale baseline row."
      [ -s "$proposal" ] && { echo; cat "$proposal"; }
    } >"$summary"
    cat "$summary"
    exit 0
    ;;
  *) exit "$propose_rc" ;;
esac

node "$root/tools/npm/security-overrides.mjs" render --root "$root"
bash "$root/tools/npm/refresh-locks.sh"

after_report="$out/osv-report-after.json"
if bash "$root/scripts/ci/osv-vulnerability-gate.sh" --report-path "$after_report" "$root" >"$out/osv-after.log" 2>&1; then
  ratchet="closes the OSV ratchet"
else
  ratchet="does NOT close the OSV ratchet on its own (see osv-after.log: a stale baseline row to delete, or a finding no override reaches)"
fi

git -C "$root" diff --binary >"$patch"
{
  echo "Raises npm advisory floors in tools/npm/security-overrides.contract.json (root ADR-0158),"
  echo "re-renders every package.json and regenerates every pnpm-lock.yaml with the pinned toolchain."
  echo
  cat "$proposal"
  echo
  echo "With this change applied the repository $ratchet."
  echo
  echo '```'
  git -C "$root" diff --stat
  echo '```'
} >"$summary"
cat "$summary"
