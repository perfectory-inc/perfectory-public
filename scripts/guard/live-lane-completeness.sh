#!/usr/bin/env bash
# Prevents: a live-backend test that belongs to no lane, and therefore runs
# nowhere while CI stays green.
#
# `cargo xtask integration <area>` used to run `--workspace -- --ignored`, which
# cannot tell a Kafka test from a Postgres one: the Postgres job ran both, the
# Kafka half found no broker, took its "resource absent" branch, and was counted
# as a pass. The fix is per-resource lanes in tools/xtask that name their targets
# instead of sweeping — but naming introduces the opposite risk. An enumerated
# lane silently stops covering a target the moment someone adds one and forgets
# the table, which is the same defect wearing the other face.
#
# So the enumeration must be provably complete: every test target carrying
# `#[ignore]` has to appear in exactly one lane. Package names are unique across
# the monorepo (scripts/guard/unique-package-names.sh), so `(package, target)` is
# a stable key.
set -euo pipefail
cd "$(dirname "$0")/../.."

xtask_src="${1:-tools/xtask/src/main.rs}"
# Every area, not just platforms/: gongzzang lives under products/ and owns the
# largest feature-gated live suite in the repository. Scanning one tree and
# reporting "complete" would be exactly the false confidence this guard exists
# to remove.
if [ "$#" -ge 2 ]; then
  scan_roots=("${@:2}")
else
  scan_roots=(platforms products)
fi

if [ ! -f "$xtask_src" ]; then
  echo "FAIL live-lane-completeness: missing $xtask_src" >&2
  exit 1
fi

for root in "${scan_roots[@]}"; do
  if [ ! -d "$root" ]; then
    echo "FAIL live-lane-completeness: scan root '$root' is not a directory" >&2
    exit 1
  fi
done

# Declared: LaneTarget { package: "P", test: "T" } — the two fields are adjacent,
# so pair them by reading the package line and attaching the next test line.
declared="$(
  grep -oE '(package|test): "[A-Za-z0-9_.-]+"' "$xtask_src" \
    | sed 's/.*: "//; s/"$//' \
    | paste - - 2>/dev/null \
    | sort -u || true
)"

# Actual: every integration test target that gates itself on a backend. Two
# gating styles are in use and both must count — foundation and identity mark
# such tests `#[ignore]`, while gongzzang compiles them out with
# `#![cfg(feature = "integration")]`. Recognising only the first would let a
# whole area's live suite sit outside every lane and still report completeness.
actual=""
while IFS= read -r file; do
  [ -n "$file" ] || continue
  grep -qE '#\[ignore|#!\[cfg\(feature' "$file" || continue
  crate_dir="${file%%/tests/*}"
  manifest="$crate_dir/Cargo.toml"
  [ -f "$manifest" ] || continue
  package="$(grep -m1 '^name' "$manifest" | sed 's/.*"\(.*\)".*/\1/')"
  [ -n "$package" ] || continue
  target="$(basename "$file" .rs)"
  # `tests/common.rs` is a shared helper module pulled in by `mod common;`, not a
  # test target cargo can address with `--test`. It inherits the suite's feature
  # gate, so it looks backend-gated without being runnable on its own.
  [ "$target" = "common" ] && continue
  actual="$actual$package	$target
"
done < <(
  find "${scan_roots[@]}" \
    -type d \( -name target -o -name node_modules \) -prune -o \
    -type f -name '*.rs' -print 2>/dev/null \
    | grep -E '/tests/[^/]+\.rs$' \
    | sort
)

missing="$(comm -23 <(printf '%s' "$actual" | sort -u | sed '/^$/d') <(printf '%s\n' "$declared" | sed '/^$/d') || true)"

if [ -n "$missing" ]; then
  echo "FAIL live-lane-completeness: these backend-gated test targets belong to no lane in $xtask_src," >&2
  echo "    so no harness runs them and nothing reports their absence:" >&2
  printf '%s\n' "$missing" | sed 's/^/      /' >&2
  echo "    Add each to the owning area's live_lanes, or remove the test." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Membership is not correctness.
#
# Belonging to a lane only guarantees SOME command names the target. It does not
# guarantee the command SELECTS it. The two gating styles need opposite cargo
# flags — `#[ignore]` needs `-- --ignored`, `#![cfg(feature = "…")]` needs
# `--features …` — and a lane that declares the wrong one runs zero tests while
# cargo exits 0. That is precisely what happened to gongzzang's twenty targets:
# fully declared, provably complete, and selected by nothing.
#
# xtask catches this at run time by reading the executed-test count back, which
# is the stronger check because it observes reality. But it can only fire for a
# lane that actually runs, and five lanes (foundation r2/lakehouse/data-go-kr,
# intelligence kafka/redis) run in no CI job at all. For those, a wrong
# declaration would sit undetected until someone finally provisioned the backend.
#
# Prior art for closing this statically: rust-lang/rust PR #108905 made unknown
# compiletest directives (`//@ ignore-<typo>`) a hard error instead of a silent
# no-op, and uncovered 79 tests whose declared gating had never applied.
# ---------------------------------------------------------------------------

# Every discoverable test target with the gating its SOURCE actually uses.
# Both forms are matched as attributes anchored at the start of a line, so a
# `#[ignore]` written inside a doc comment cannot be mistaken for a real gate.
actual_gating="$(
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    crate_dir="${file%%/tests/*}"
    manifest="$crate_dir/Cargo.toml"
    [ -f "$manifest" ] || continue
    package="$(grep -m1 '^name' "$manifest" | sed 's/.*"\(.*\)".*/\1/')"
    [ -n "$package" ] || continue
    target="$(basename "$file" .rs)"
    [ "$target" = "common" ] && continue
    feature="$(
      grep -m1 -oE '^#!\[cfg\(feature = "[^"]+"\)\]' "$file" 2>/dev/null \
        | sed 's/.*"\(.*\)".*/\1/' || true
    )"
    if [ -n "$feature" ]; then
      printf '%s\t%s\tFeature("%s")\n' "$package" "$target" "$feature"
    elif grep -qE '^[[:space:]]*#\[ignore' "$file" 2>/dev/null; then
      printf '%s\t%s\tIgnored\n' "$package" "$target"
    else
      printf '%s\t%s\tNone\n' "$package" "$target"
    fi
  done < <(
    find "${scan_roots[@]}" \
      -type d \( -name target -o -name node_modules \) -prune -o \
      -type f -name '*.rs' -print 2>/dev/null \
      | grep -E '/tests/[^/]+\.rs$' \
      | sort
  )
)"

# What the lane table CLAIMS. `gating:` precedes `targets:` in each LiveLane, so
# a one-state pass attaches the lane's gating to each of its targets. The
# `#[cfg(test)] mod tests` block is cut first: it constructs bare LaneTargets
# that would otherwise inherit the last real lane's gating.
declared_gating="$(
  sed '/^#\[cfg(test)\]/,$d' "$xtask_src" \
    | grep -oE 'gating: LaneGating::(Ignored|Feature\("[^"]+"\))|package: "[^"]+"|test: "[^"]+"' \
    | awk '
        /^gating:/  { g = $0; sub(/^gating: LaneGating::/, "", g); next }
        /^package:/ { p = $0; sub(/^package: "/, "", p); sub(/"$/, "", p); next }
        /^test:/    { t = $0; sub(/^test: "/, "", t); sub(/"$/, "", t)
                      if (g != "" && p != "") print p "\t" t "\t" g }'
)"

gating_report=""
while IFS="$(printf '\t')" read -r package target claimed; do
  [ -n "$package" ] || continue
  observed="$(
    printf '%s\n' "$actual_gating" \
      | awk -F"$(printf '\t')" -v p="$package" -v t="$target" '$1 == p && $2 == t { print $3 }'
  )"
  if [ -z "$observed" ]; then
    gating_report="${gating_report}${package} --test ${target}: declared in a lane, but no such test target exists
"
  elif [ "$observed" != "$claimed" ]; then
    gating_report="${gating_report}${package} --test ${target}: lane declares ${claimed}, source uses ${observed}
"
  fi
done <<EOF
$declared_gating
EOF

if [ -n "$gating_report" ]; then
  echo "FAIL live-lane-completeness: a lane's declared gating does not match its target's source," >&2
  echo "    so the lane's cargo flags select nothing and the run still exits 0:" >&2
  printf '%s' "$gating_report" | sed 's/^/      /' >&2
  echo "    Ignored needs '#[ignore]'; Feature(\"f\") needs '#![cfg(feature = \"f\")]'." >&2
  exit 1
fi

echo "OK live-lane-completeness"
