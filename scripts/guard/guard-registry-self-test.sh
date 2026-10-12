#!/usr/bin/env bash
# Proves guard-registry.sh rejects each violation it claims to reject, on a copy of this
# repository's guard directory, and accepts the copy as it is plus a correctly registered guard.
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"

checker="$root/scripts/guard/guard-registry.sh"
test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

fail() {
  echo "FAIL guard-registry-self-test: $*" >&2
  exit 1
}

# A fresh copy of every file the guard reads.
fixture() {
  local dir="$test_root/$1"
  mkdir -p "$dir/scripts" "$dir/.github/workflows"
  cp -R "$root/scripts/guard" "$dir/scripts/guard"
  cp "$root/.github/workflows/foundation-ci.yml" "$dir/.github/workflows/"
  printf '%s\n' "$dir"
}

# Adds a guard name to the fixture runner's `for g in` list.
register() {
  sed -i "s/^for g in /for g in $2 /" "$1/scripts/guard/monorepo-guard.sh"
}

expect_accepted() {
  local label="$1" dir="$2"
  bash "$checker" "$dir" >/dev/null 2>&1 || fail "$label should have been accepted"
}

expect_rejected() {
  local label="$1" dir="$2" message="$3" output
  if output="$(bash "$checker" "$dir" 2>&1)"; then
    fail "$label should have been rejected: $output"
  fi
  grep -Fq -- "$message" <<<"$output" || fail "$label was rejected for another reason: $output"
}

dir="$(fixture baseline)"
expect_accepted "the repository's own guard directory" "$dir"

dir="$(fixture registered)"
printf '#!/usr/bin/env bash\n# Prevents: a synthetic new invariant.\necho new\n' \
  >"$dir/scripts/guard/synthetic-new-guard.sh"
register "$dir" synthetic-new-guard
expect_accepted "a new guard added to the runner list" "$dir"

# (i) a guard file nothing runs.
dir="$(fixture orphan)"
printf '#!/usr/bin/env bash\necho orphan\n' >"$dir/scripts/guard/synthetic-orphan.sh"
expect_rejected "an orphaned guard" "$dir" "synthetic-orphan.sh is run by nothing"

# (i) a RUN_ELSEWHERE caller that stopped invoking its guard.
dir="$(fixture stale-caller)"
sed -i '/^bash scripts\/guard\/public-doc-boundary\.sh/d' "$dir/scripts/guard/public-repository-safety.sh"
expect_rejected "a caller that no longer runs its guard" "$dir" "no longer invokes it"

# (i) a RUN_ELSEWHERE entry whose guard is gone.
dir="$(fixture stale-entry)"
rm "$dir/scripts/guard/public-doc-boundary.sh"
expect_rejected "a RUN_ELSEWHERE entry without a guard file" "$dir" "scripts/guard/public-doc-boundary.sh does not exist"

# (ii) the same body under another name; a changed comment must not hide it.
dir="$(fixture same-body)"
sed 's/^# Prevents:.*/# A reworded header./' "$dir/scripts/guard/no-subdir-github.sh" \
  >"$dir/scripts/guard/workflows-only-at-root.sh"
register "$dir" workflows-only-at-root
expect_rejected "a guard with a copied body" "$dir" "duplicate guards with the same body: no-subdir-github, workflows-only-at-root"

# (ii) the same `# Prevents:` line over a different body.
dir="$(fixture same-prevents)"
{
  sed -n '1,3p' "$dir/scripts/guard/no-subdir-github.sh"
  printf 'echo different body\n'
} >"$dir/scripts/guard/root-workflows.sh"
register "$dir" root-workflows
expect_rejected "a guard repeating a Prevents line" "$dir" "duplicate guards with the same \`# Prevents:\` line"

# (ii) the same name apart from a version suffix.
dir="$(fixture same-name)"
printf '#!/usr/bin/env bash\necho second toolchain check\n' \
  >"$dir/scripts/guard/toolchain-consistency-v2.sh"
register "$dir" toolchain-consistency-v2
expect_rejected "a -v2 copy of a guard" "$dir" "the same name apart from a suffix: toolchain-consistency, toolchain-consistency-v2"

# (iii) a runner-listed guard that another guard runs as well.
dir="$(fixture run-twice)"
printf 'bash scripts/guard/no-subdir-github.sh\n' >>"$dir/scripts/guard/public-github-policy.sh"
expect_rejected "a guard run twice" "$dir" "no-subdir-github runs 2 times"

# (iii) a guard outside the runner list that two guards both run.
dir="$(fixture run-twice-elsewhere)"
printf 'bash scripts/guard/public-doc-boundary.sh\n' >>"$dir/scripts/guard/public-github-policy.sh"
expect_rejected "a guard two callers run" "$dir" "public-doc-boundary runs 2 times"

echo "OK guard-registry-self-test"
