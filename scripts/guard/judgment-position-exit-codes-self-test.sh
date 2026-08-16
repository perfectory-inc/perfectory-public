#!/usr/bin/env bash
set -euo pipefail
# This test builds real Git repositories. Under a pre-push hook the inherited
# GIT_DIR made every `git -C "$root"` below address the real checkout instead,
# which is how a fixture named scripts/example.sh ended up staged in it.
# `fixture_git` pins the binding to the fixture and refuses any target outside
# the directory `fixture_root` created.
. "$(dirname "$0")/lib/fixture-repo.sh"
cd "$(dirname "$0")/../.."

checker="scripts/guard/judgment-position-exit-codes.sh"
fixture_root
test_root="$FIXTURE_ROOT"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL judgment-position-exit-codes-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# Builds a synthetic repository holding one script, written verbatim so a fixture can carry a shape
# the checker has to read exactly. `git ls-files` is the input, so the fixture has to be a real
# repository with the file staged.
fixture() {
  local root="$1"
  local body="$2"
  mkdir -p "$root/scripts"
  printf '%s\n' "$body" >"$root/scripts/example.sh"
  fixture_git "$root" init --quiet
  fixture_git "$root" add scripts/example.sh
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL judgment-position-exit-codes-self-test: rejected allowed fixture $1" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL judgment-position-exit-codes-self-test: accepted forbidden fixture $1" >&2
    exit 1
  fi
}

# The defect this guard exists for, in the shape it actually arrived in. The real command launched a
# container; the fixture uses a neutral name because `container-runtime-policy` reads this file too
# and would try to resolve a synthetic image reference — naming the tool even in a comment is enough
# to trip it, which is the third time in one session that prose about a guarded pattern set off its
# guard.
reads_status="$test_root/reads-status"
fixture "$reads_status" 'run-the-build --workspace | tail -40
echo "CHECK_EXIT=$?"'
expect_rejected "$reads_status"

# The same read, one blank line later. Statuses do not expire.
reads_status_gap="$test_root/reads-status-gap"
fixture "$reads_status_gap" 'cmd | tail -4

rc=$?'
expect_rejected "$reads_status_gap"

# The ADR'"'"'s own forbidden example.
consuming_or="$test_root/consuming-or"
fixture "$consuming_or" 'cmd | tail -6 || rc=1'
expect_rejected "$consuming_or"

consuming_exit="$test_root/consuming-exit"
fixture "$consuming_exit" 'cmd | tail -6 || exit 1'
expect_rejected "$consuming_exit"

# The adopted form: judge the command, tidy the output separately.
adopted="$test_root/adopted"
fixture "$adopted" 'if cmd >out.txt 2>&1; then tail -2 out.txt; else rc=1; tail -20 out.txt; fi
echo "EXIT=$?"'
expect_allowed "$adopted"

# `|| true` discards the status on purpose; that is not a judgment.
discarding="$test_root/discarding"
fixture "$discarding" 'git ls-files | sort -u || true'
expect_allowed "$discarding"

# A pipeline whose status nobody reads is not in a judgment position.
plain_pipeline="$test_root/plain-pipeline"
fixture "$plain_pipeline" 'cmd | sort >out.txt
echo done'
expect_allowed "$plain_pipeline"

# Out of scope on purpose: fail-closed conditions. A broken command yields no output, the filter
# finds nothing, and the assertion goes red.
fail_closed="$test_root/fail-closed"
fixture "$fail_closed" 'if ! psql "$DATABASE_URL" -c "select 1" | grep -q "^1$"; then exit 1; fi'
expect_allowed "$fail_closed"

# `|` inside a case pattern is alternation, not a pipeline.
case_arm="$test_root/case-arm"
fixture "$case_arm" 'case "$path" in
  /tmp/*|/var/tmp/*) rm -rf -- "$path" || exit 1 ;;
esac'
expect_allowed "$case_arm"

# So is `|` inside a `[[ ]]` regex.
regex_alternation="$test_root/regex-alternation"
fixture "$regex_alternation" 'if [[ "$line" =~ ^(alpha|beta)$ ]]; then rc=1; fi'
expect_allowed "$regex_alternation"

# A quoted pipe is data.
quoted_pipe="$test_root/quoted-pipe"
fixture "$quoted_pipe" 'awk "{print \$1|\"sort\"}" file || rc=1'
expect_allowed "$quoted_pipe"

# Shell strings cross lines, and the interior of one is data. The first version of this guard reset
# its quote state every line and flagged the `case` arm inside the fixture above as a live pipeline —
# in this very file. That is why the state is threaded through.
multiline_string="$test_root/multiline-string"
fixture "$multiline_string" 'body="first line
cmd | tail -6 || rc=1
last line"
printf "%s\n" "$body"'
expect_allowed "$multiline_string"

# A heredoc body belongs to whatever reads it. SQL uses `||` for concatenation.
heredoc="$test_root/heredoc"
fixture "$heredoc" 'psql "$DATABASE_URL" <<SQL
select a || b from t;
SQL'
expect_allowed "$heredoc"

# An audited exception is declared on the line, not argued in a commit message.
reviewed="$test_root/reviewed"
fixture "$reviewed" 'cmd | tail -6 || rc=1  # verification-judgment: reviewed-pipeline'
expect_allowed "$reviewed"

# A scan that reads nothing must not report a pass.
empty="$test_root/empty"
mkdir -p "$empty"
fixture_git "$empty" init --quiet
expect_rejected "$empty"

echo "OK judgment-position-exit-codes-self-test"
