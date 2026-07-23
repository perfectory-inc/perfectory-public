#!/usr/bin/env bash
# Proves comments and unrelated TOML/JSON sections cannot spoof package policy.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/check-package-publication-policy.py"
test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

make_valid() {
  local root="$1"
  mkdir -p "$root/LICENSES" "$root/workspace/member" "$root/web" "$root/worker"
  printf 'proprietary\n' >"$root/LICENSES/LicenseRef-Proprietary.txt"
  cat >"$root/workspace/Cargo.toml" <<'TOML'
[workspace]
members = ["member"]
[workspace.package]
license-file = "../LICENSES/LicenseRef-Proprietary.txt"
publish = false
TOML
  cat >"$root/workspace/member/Cargo.toml" <<'TOML'
[package]
name = "synthetic-member"
version = "0.0.0"
license-file.workspace = true
publish.workspace = true
TOML
  cat >"$root/web/package.json" <<'JSON'
{"name":"synthetic-web","private":true,"license":"UNLICENSED"}
JSON
  cat >"$root/worker/pyproject.toml" <<'TOML'
[project]
name = "synthetic-worker"
version = "0.0.0"
license = "LicenseRef-Proprietary"
classifiers = ["Private :: Do Not Upload"]
TOML
  git -C "$root" init -q --initial-branch=main
  git -C "$root" add .
}

valid="$test_root/valid"
make_valid "$valid"
python3 "$checker" "$valid" >/dev/null

expect_rejected() {
  local label="$1"
  local root="$test_root/$label"
  make_valid "$root"
  "$2" "$root"
  git -C "$root" add .
  if python3 "$checker" "$root" >/dev/null 2>&1; then
    echo "FAIL package-publication-policy-self-test: accepted $label spoof" >&2
    exit 1
  fi
}

spoof_cargo_comment() {
  local root="$1"
  cat >"$root/workspace/member/Cargo.toml" <<'TOML'
[package]
name = "synthetic-member"
version = "0.0.0"
# license-file.workspace = true
# publish.workspace = true
TOML
}
expect_rejected cargo-comment spoof_cargo_comment

spoof_cargo_section() {
  local root="$1"
  cat >"$root/workspace/member/Cargo.toml" <<'TOML'
[package]
name = "synthetic-member"
version = "0.0.0"
[package.metadata.fake]
license-file = "../../../LICENSES/LicenseRef-Proprietary.txt"
publish = false
TOML
}
expect_rejected cargo-wrong-section spoof_cargo_section

spoof_nested_license() {
  local root="$1"
  mkdir -p "$root/workspace/LICENSES"
  printf 'counterfeit\n' >"$root/workspace/LICENSES/LicenseRef-Proprietary.txt"
  sed 's#\.\./LICENSES/LicenseRef-Proprietary.txt#LICENSES/LicenseRef-Proprietary.txt#' \
    "$root/workspace/Cargo.toml" >"$root/workspace/Cargo.toml.tmp"
  mv "$root/workspace/Cargo.toml.tmp" "$root/workspace/Cargo.toml"
}
expect_rejected nested-counterfeit-license spoof_nested_license

spoof_json_nested() {
  local root="$1"
  printf '%s\n' '{"name":"synthetic-web","metadata":{"private":true,"license":"UNLICENSED"}}' \
    >"$root/web/package.json"
}
expect_rejected json-wrong-section spoof_json_nested

spoof_python_section() {
  local root="$1"
  cat >"$root/worker/pyproject.toml" <<'TOML'
[project]
name = "synthetic-worker"
version = "0.0.0"
[tool.fake]
license = "LicenseRef-Proprietary"
classifiers = ["Private :: Do Not Upload"]
TOML
}
expect_rejected pyproject-wrong-section spoof_python_section

echo "OK package-publication-policy-self-test"
