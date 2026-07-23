#!/usr/bin/env bash
# Mutation tests for prepare-before-decision, exact resume, and ambiguous-push locking.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/check-public-root-publisher.py"
publisher="scripts/github/publish-public-root.sh"
python3 "$checker" "$publisher" >/dev/null

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

mutate() {
  local output="$1"
  local old="$2"
  local new="$3"
  python3 - "$publisher" "$output" "$old" "$new" <<'PY'
from pathlib import Path
import sys

source = Path(sys.argv[1]).read_text(encoding="utf-8")
old, new = sys.argv[3], sys.argv[4]
output = Path(sys.argv[2])
if output.exists():
    source = output.read_text(encoding="utf-8")
if source.count(old) != 1:
    raise SystemExit(f"mutation anchor occurs {source.count(old)} times: {old!r}")
output.write_text(source.replace(old, new, 1), encoding="utf-8", newline="\n")
PY
}

expect_rejected() {
  local candidate="$1"
  local message="$2"
  if python3 "$checker" "$candidate" >/dev/null 2>&1; then
    echo "FAIL public-root-publisher-self-test: accepted $message" >&2
    exit 1
  fi
}

mutate "$test_root/armed-after-push.sh" \
  $'  publication_started=1\n  "$git_transport" --repository' \
  $'  "$git_transport" --repository'
mutate "$test_root/armed-after-push.sh" \
  $'    "$snapshot_commit:refs/heads/main"\n  "$configurator" lock' \
  $'    "$snapshot_commit:refs/heads/main"\n  publication_started=1\n  "$configurator" lock'
expect_rejected "$test_root/armed-after-push.sh" "post-push arming"

mutate "$test_root/no-retry.sh" \
  $'if [ "$publication_started" -eq 1 ] \\\n    && [ "$main_locked" -eq 0 ]' \
  $'if [ "$publication_started" -eq 0 ] \\\n    && [ "$main_locked" -eq 0 ]'
expect_rejected "$test_root/no-retry.sh" "disabled retry trap"

mutate "$test_root/claims-early-lock.sh" \
  $'  "$configurator" lock\n  main_locked=1' \
  $'  main_locked=1\n  "$configurator" lock'
expect_rejected "$test_root/claims-early-lock.sh" "lock confirmation before lock"

mutate "$test_root/no-prepare.sh" \
  '"$prepare" "$source_root" "$2" "$snapshot"' \
  ': # skipped trusted preparation'
expect_rejected "$test_root/no-prepare.sh" "publisher without in-process preparation"

mutate "$test_root/foreign-control-root.sh" \
  'if [ "$source_root" != "$control_root" ] || [ "$control_root" != "$root" ]; then' \
  'if false; then # admitted a foreign source/control tree'
expect_rejected "$test_root/foreign-control-root.sh" "foreign source/control worktree"

mutate "$test_root/no-publication-authority.sh" \
  '  "$publication_authority"' \
  '  : # skipped sole-writer authority check'
expect_rejected "$test_root/no-publication-authority.sh" "fresh push without immediate authority check"

mutate "$test_root/late-publication-authority.sh" \
  $'  "$publication_authority"\n  publication_started=1' \
  $'  publication_started=1\n  "$publication_authority"'
expect_rejected "$test_root/late-publication-authority.sh" "authority check after publication was armed"

mutate "$test_root/forged-audit.sh" \
  'source_tree="$' \
  $'audit_source="$("$git_transport" --repository "$snapshot" config --get perfectory.publicAudit.sourceTree)"\nsource_tree="$'
expect_rejected "$test_root/forged-audit.sh" "forgeable local audit marker"

mutate "$test_root/loose-resume.sh" \
  'elif [ "$remote_state" = "$expected_remote_state" ]; then' \
  'elif [[ "$remote_state" == *"$snapshot_commit"* ]]; then'
expect_rejected "$test_root/loose-resume.sh" "noncanonical remote resume state"

mutate "$test_root/wrong-resume-sha.sh" \
  '"$snapshot_commit" "$snapshot_commit")"' \
  '"$source_commit" "$source_commit")"'
expect_rejected "$test_root/wrong-resume-sha.sh" "resume at a non-snapshot SHA"

mutate "$test_root/admit-mismatch.sh" \
  $'  echo "FAIL public-root-publisher: remote is neither empty nor the exact expected root" >&2\n  printf \'%s\\n\' "$remote_state" >&2\n  exit 1' \
  $'  publication_mode=resume # unsafe fallback'
expect_rejected "$test_root/admit-mismatch.sh" "mismatched remote state"

mutate "$test_root/activate-before-clone-check.sh" \
  '"$configurator" activate' \
  ': # activation moved before independent verification'
mutate "$test_root/activate-before-clone-check.sh" \
  'clone_commit="$' \
  $'"$configurator" activate\nclone_commit="$'
expect_rejected "$test_root/activate-before-clone-check.sh" "activation before clone verification"

echo "OK public-root-publisher-self-test"
