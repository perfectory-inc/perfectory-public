#!/usr/bin/env bash
# Proves scripts/ci/cargo-advisories.sh can fail (root ADR-0271): a planted lockfile with a published
# advisory is refused and named, a clean one passes, and the issue step opens, rewrites only on new
# findings, and closes. The fixture workspaces are written here, not tracked: a tracked Cargo.lock with
# a vulnerable crate would fail the repository OSV ratchet and this script's own default scan.
# Needs network (crates.io index, advisory database) and cargo-deny on PATH.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
script="$root/scripts/ci/cargo-advisories.sh"
work="$(mktemp -d)"
trap 'rm -rf -- "$work"' EXIT

fail() {
  echo "FAIL cargo-advisories-self-test: $*" >&2
  exit 1
}

workspace() {
  local dir="$work/$1"
  mkdir -p "$dir/src"
  : >"$dir/src/lib.rs"
  printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n[workspace]\n' "$1" >"$dir/Cargo.toml"
}

# smallvec 0.6.13 carries RUSTSEC-2021-0003 (fixed in 0.6.14).
workspace planted
cat >>"$work/planted/Cargo.toml" <<'TOML'

[dependencies]
smallvec = "=0.6.13"
TOML
cat >"$work/planted/Cargo.lock" <<'LOCK'
version = 4

[[package]]
name = "maybe-uninit"
version = "2.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "60302e4db3a61da70c0cb7991976248362f30319e88850c487b9b95bbf059e00"

[[package]]
name = "planted"
version = "0.0.0"
dependencies = [
 "smallvec",
]

[[package]]
name = "smallvec"
version = "0.6.13"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "f7b0758c52e15a8b5e3691eae6cc559f08eee9406e548a4477ba4e67770a82b6"
dependencies = [
 "maybe-uninit",
]
LOCK
workspace clean
printf 'version = 4\n\n[[package]]\nname = "clean"\nversion = "0.0.0"\n' >"$work/clean/Cargo.lock"

if bash "$script" check "$work/planted.md" "$work/planted" >"$work/planted.log" 2>&1; then
  fail "passed a lockfile with RUSTSEC-2021-0003"
fi
grep -q 'RUSTSEC-2021-0003' "$work/planted.md" || fail "the summary does not name RUSTSEC-2021-0003"
grep -q "planted" "$work/planted.md" || fail "the summary does not name the workspace"
if ! bash "$script" check "$work/clean.md" "$work/clean" >"$work/clean.log" 2>&1; then
  cat "$work/clean.log" >&2
  fail "refused a workspace with no dependency"
fi
[ ! -s "$work/clean.md" ] || fail "a clean workspace wrote a finding"
# One refusing workspace refuses the run even when another passes.
if bash "$script" check "$work/both.md" "$work/clean" "$work/planted" >/dev/null 2>&1; then
  fail "a clean workspace hid a refused one"
fi

# The issue step against a fake gh that records its calls and answers from files.
mkdir "$work/bin"
cat >"$work/bin/gh" <<'GH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_GH/calls"
case "$1 $2" in
  "issue list") cat "$FAKE_GH/open" 2>/dev/null || true ;;
  "issue view") cat "$FAKE_GH/body" ;;
esac
GH
chmod +x "$work/bin/gh"
export FAKE_GH="$work/gh" GH_REPO=example/repository PATH="$work/bin:$PATH"
issue_case() {
  rm -rf "$FAKE_GH"
  mkdir "$FAKE_GH"
  : >"$FAKE_GH/calls"
  if [ -n "$2" ]; then printf '%s\n' "$2" >"$FAKE_GH/open"; fi
  if [ -n "$3" ]; then printf '%s' "$3" >"$FAKE_GH/body"; fi
  RUN_URL="run-$4" bash "$script" issue "$1" "$work/planted.md" >/dev/null
}
expect_calls() {
  local got
  got="$(cut -d' ' -f1-2 "$FAKE_GH/calls" | tr '\n' ',')"
  [ "$got" = "$1" ] || fail "$2: gh calls were '$got', expected '$1'"
}

issue_case failure "" "" 1
expect_calls "issue list,issue create," "a first failure"
grep -q -- "--title Cargo advisories on main" "$FAKE_GH/calls" || fail "the issue is not created under the one title"

same_body="$(printf 'first line run-0\n\n'; cat "$work/planted.md")"
issue_case failure 7 "$same_body" 2
expect_calls "issue list,issue view," "an unchanged finding"

issue_case failure 7 "$(printf 'first line\n\n### `elsewhere`\n')" 3
expect_calls "issue list,issue view,issue edit,issue comment," "a changed finding"

issue_case success 7 "" 4
expect_calls "issue list,issue close," "a cleared check"

issue_case success "" "" 5
expect_calls "issue list," "a clean day with no issue"

issue_case cancelled 7 "" 6
expect_calls "issue list," "a cancelled check"

echo "OK cargo-advisories-self-test"
