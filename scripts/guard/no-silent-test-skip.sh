#!/usr/bin/env bash
# Prevents: a test that never ran being reported as a test that passed.
#
# ADR-0004 unified the *verification command*, but explicitly deferred live-backend
# tests to a Phase 2 that never landed. In that gap each area invented its own way
# to handle "the database/broker isn't here", and one of them green-washes: the
# test reads a connection env var, prints a notice, and `return`s. cargo reports
# PASSED. An unrun contract test is then indistinguishable from a verified one, so
# a whole transport can rot untested behind a green check.
#
# Only two gating patterns keep "did not run" distinguishable from "passed":
#   * `#[ignore]`   — excluded by default, listed as ignored, run via `--ignored`
#                     (foundation; `cargo xtask integration` refuses without a URL)
#   * fail-loud     — `#![cfg(feature = "...")]` + panic on the missing resource
#                     (gongzzang), so a misconfigured run is a failure, not a pass
#
# Non-test source may still log a skip: a runtime deciding to drop a stale event
# is not a test claiming to have verified something. Only test files are scanned.
set -euo pipefail
cd "$(dirname "$0")/../.."

scan_root="${1:-.}"

if [ ! -d "$scan_root" ]; then
  echo "FAIL no-silent-test-skip: scan root '$scan_root' is not a directory" >&2
  exit 1
fi

# Test code lives either under a `tests/` directory (integration tests) or in a
# `tests.rs` / `*_tests.rs` module beside the code it covers (the convention this
# repository uses for in-crate suites).
mapfile -t test_files < <(
  find "$scan_root" \
    -type d \( -name target -o -name node_modules -o -name .git \) -prune -o \
    -type f -name '*.rs' -print 2>/dev/null \
    | grep -E '(/tests/|/tests\.rs$|_tests\.rs$)' \
    | sort
)

rc=0
report() {
  local file="$1" hit="$2"
  echo "FAIL no-silent-test-skip: $file:$hit" >&2
  echo "    A missing backend must not read as a pass. Use #[ignore] (run it via" >&2
  echo "    'cargo xtask integration <area>'), or panic on the missing resource." >&2
  rc=1
}

# rustfmt splits a long call across lines, and a line-oriented rule then sees
# neither half. Both of these are the SAME defects as the one-line forms, and
# both went undetected in this repository:
#
#   eprintln!(                       std::env::var("…")
#       "skipping …"                     .ok()
#   );                                   .filter(|u| !u.is_empty())?;
#
# So each file is normalized first: a line whose first non-space character
# continues the previous expression (`.` or `"`) is folded onto it, and the
# ORIGINAL line number of the statement is kept so the report still points at
# real source. Rules 1 and 2 then match the joined statement.
joined_statements() {
  awk '
    function flush() { if (started) print bufno "\t" buf }
    {
      stripped = $0
      sub(/^[[:space:]]+/, "", stripped)
      if (started && (stripped ~ /^\./ || stripped ~ /^"/)) {
        buf = buf " " stripped
        next
      }
      flush()
      buf = stripped
      bufno = NR
      started = 1
    }
    END { flush() }
  ' "$1" 2>/dev/null || true
}

for f in "${test_files[@]}"; do
  [ -n "$f" ] || continue
  statements="$(joined_statements "$f")"

  # (1) A print whose message announces a skip is the loud form of the shape: the
  # accompanying early `return` is what makes cargo report a pass anyway.
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    report "$f" "$hit"
  done < <(
    printf '%s\n' "$statements" \
      | grep -Ei '(eprintln|println)!\(.*skip' \
      | sed 's/\t/:/' || true
  )

  # (2) The quiet form leaves no trace at all: `.ok()` demotes a missing required
  # resource to `None`, the caller returns early, and nothing is printed. A test
  # that needs a backend must state that need, not silently absorb its absence.
  #
  # A chain that ENDS in `.expect(`/`.unwrap(` is exempt: `.ok().filter(..)
  # .expect(..)` reads the variable, rejects an empty value, and panics when it
  # is absent. That is fail-loud — the sanctioned shape — and flagging it would
  # push authors back toward the silence this rule exists to remove.
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    report "$f" "$hit"
  done < <(
    printf '%s\n' "$statements" \
      | grep -E 'env::var\([^)]*\)[[:space:]]*\.ok\(\)' \
      | grep -vE '\.(expect|unwrap)\(' \
      | sed 's/\t/:/' || true
  )

  # (3) `let Ok(x) = env::var("…") else { return … }` reaches the same place by a
  # different road, and rules 1 and 2 both miss it: nothing is printed and `.ok()`
  # never appears. `Ok(_)` is a refutable pattern, so this construct is ALWAYS a
  # let-else, and its diverging block is where the resource requirement quietly
  # becomes a pass. foundation-outbox's publish_roundtrip carried it for six
  # `#[ignore]` contract tests. Fail loud instead: `env::var(..)?` or `.expect(..)`.
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    report "$f" "$hit"
  done < <(
    grep -nE 'let[[:space:]]+Ok\([^)]*\)[[:space:]]*=[[:space:]]*(std::)?env::var' "$f" \
      2>/dev/null || true
  )

  # (4) The conditional form: a probe on an env var guarding an early SUCCESS
  # return. `if env::var("X").as_deref() != Ok("1") { return Ok(()); }` reads like
  # an opt-in switch and behaves like a pass. Returning `Err` there is fine and is
  # what the sibling Kafka tests do — only a success return is the defect, so the
  # rule requires both halves and cannot fire on the fail-loud shape.
  #
  # A window rather than a single line, because the condition and the return are
  # on different lines. The window stops at the block's closing brace so a later
  # unrelated `return Ok(())` cannot be attributed to this condition.
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    report "$f" "$hit"
  done < <(
    awk '
      { line[NR] = $0 }
      END {
        for (i = 1; i <= NR; i++) {
          if (line[i] !~ /env::var(_os)?\(/) continue
          if (line[i] !~ /(^|[^A-Za-z_])if[[:space:]]/) continue
          for (j = i; j <= i + 3 && j <= NR; j++) {
            if (j > i && line[j] ~ /^[[:space:]]*\}/) break
            if (line[j] ~ /^[[:space:]]*return[[:space:]]*(Ok\(\(\)\))?[[:space:]]*;/) {
              sub(/^[[:space:]]+/, "", line[i])
              print i ":" line[i]
              break
            }
          }
        }
      }
    ' "$f" 2>/dev/null || true
  )
done

if [ "$rc" -eq 0 ]; then
  echo "OK no-silent-test-skip"
fi
exit "$rc"
