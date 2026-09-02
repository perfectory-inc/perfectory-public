#!/usr/bin/env bash
# Plants the violations the guard exists to catch and checks it refuses each one.
#
# A guard only ever observed passing has not been shown to be able to fail. This one was written
# after the same string was found in three callers, so the case that matters is a fourth caller
# appearing — and a guard that named the three would have reported OK on it.
set -uo pipefail

guard="$(cd "$(dirname "$0")" && pwd -P)/the-contract-names-where-its-objects-live.sh"
repo_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
failures=0

PREFIX='silver-handoff/somewhere'

# Builds a tree shaped like the real one: a contract, plus whatever extra files the case needs.
fixture() {
  local label="$1" declares="$2"
  local root="$test_root/$label"
  local contracts="$root/platforms/foundation-platform/infra/lakehouse/contracts"
  mkdir -p "$contracts"
  if [ "$declares" = "declares" ]; then
    cat >"$contracts/vworld-parcel-source-objects.json" <<JSON
{
  "schema_version": 1,
  "handoff_prefix": "$PREFIX",
  "handoff_suffix": ".jsonl.gz"
}
JSON
  else
    cat >"$contracts/vworld-parcel-source-objects.json" <<'JSON'
{
  "schema_version": 1,
  "handoff_suffix": ".jsonl.gz"
}
JSON
  fi
  printf '%s\n' "$root"
}

expect() {
  local label="$1" want="$2" root="$3" needle="${4:-}"
  local output status
  output="$("$guard" "$root" 2>&1)"
  status=$?

  if [ "$want" = accept ] && [ "$status" -ne 0 ]; then
    printf 'FAIL self-test %s: expected accept, got refusal:\n%s\n' "$label" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  if [ "$want" = reject ] && [ "$status" -eq 0 ]; then
    printf 'FAIL self-test %s: expected refusal, guard reported OK\n' "$label" >&2
    failures=$((failures + 1))
    return
  fi
  if [ -n "$needle" ] && ! grep -qF -- "$needle" <<<"$output"; then
    printf 'FAIL self-test %s: refusal did not mention %s:\n%s\n' "$label" "$needle" "$output" >&2
    failures=$((failures + 1))
    return
  fi
  printf 'ok self-test %s\n' "$label"
}

# A contract that declares it, and nothing else restating it.
root="$(fixture declared-once declares)"
expect declared-once accept "$root"

# A fourth caller. Not one of the three the fix touched — the case a list of readers would miss.
root="$(fixture a-new-caller declares)"
mkdir -p "$root/platforms/foundation-platform/scripts/load"
printf '#!/usr/bin/env bash\nPREFIX="%s"\n' "$PREFIX" \
  >"$root/platforms/foundation-platform/scripts/load/some-new-export.sh"
expect a-new-caller reject "$root" "some-new-export.sh"

# A default in Rust, above the test module: the exact shape that was removed.
root="$(fixture a-rust-default declares)"
mkdir -p "$root/platforms/foundation-platform/services/x/src"
cat >"$root/platforms/foundation-platform/services/x/src/load.rs" <<RS
const DEFAULT_HANDOFF_PREFIX: &str = "$PREFIX";

#[cfg(test)]
mod tests {}
RS
expect a-rust-default reject "$root" "load.rs"

# The same string below `mod tests` is a fixture, not a second source of truth.
root="$(fixture a-rust-fixture declares)"
mkdir -p "$root/platforms/foundation-platform/services/x/src"
cat >"$root/platforms/foundation-platform/services/x/src/load.rs" <<RS
fn main() {}

#[cfg(test)]
mod tests {
    const EXPECTED: &str = "$PREFIX";
}
RS
expect a-rust-fixture accept "$root"

# The guard must refuse when it cannot look: no declaration means no value to search for, and
# reporting OK there is the same defect one level up.
root="$(fixture no-declaration absent)"
expect no-declaration reject "$root" "does not declare handoff_prefix"

# No contract at all.
mkdir -p "$test_root/no-contract"
expect no-contract reject "$test_root/no-contract" "missing"

# And the real tree passes.
expect the-repository accept "$repo_root"

if [ "$failures" -ne 0 ]; then
  printf 'FAIL the-contract-names-where-its-objects-live-self-test: %s case(s) failed\n' "$failures" >&2
  exit 1
fi
printf 'OK the-contract-names-where-its-objects-live-self-test\n'
