#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/no-env-access-in-domain-layers.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL no-env-access-in-domain-layers-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# Writes one crate under a synthetic repository. The crate directory name is
# what selects the rule, so each fixture names it explicitly.
fixture() {
  local root="$1"
  local crate="$2"
  local body="$3"
  local crate_dir="$root/platforms/example-platform/crates/example/$crate"
  mkdir -p "$crate_dir/src"
  printf '[package]\nname = "%s"\n' "$crate" >"$crate_dir/Cargo.toml"
  printf '%s\n' "$body" >"$crate_dir/src/lib.rs"
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL no-env-access-in-domain-layers-self-test: rejected allowed fixture $1" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL no-env-access-in-domain-layers-self-test: accepted forbidden fixture $1" >&2
    exit 1
  fi
}

clean="$test_root/clean"
fixture "$clean" "example-application" '
pub struct PublicationCapability(bool);
pub fn enabled(capability: PublicationCapability) -> bool { capability.0 }
'
expect_allowed "$clean"

# Infrastructure is where a deployment switch is resolved, so the same read is
# allowed one layer out. Without this case the guard could be over-broad and the
# suite would not notice.
infrastructure="$test_root/infrastructure"
fixture "$infrastructure" "example-infrastructure" '
pub fn capability() -> bool { std::env::var("EXAMPLE_SWITCH").is_ok() }
'
expect_allowed "$infrastructure"

commented="$test_root/commented"
fixture "$commented" "example-domain" '
// The switch used to be read here with std::env::var("EXAMPLE_SWITCH").
pub const SCHEMA_VERSION: u32 = 2;
'
expect_allowed "$commented"

qualified="$test_root/qualified"
fixture "$qualified" "example-application" '
pub fn capability() -> bool { std::env::var("EXAMPLE_SWITCH").is_ok() }
'
expect_rejected "$qualified"

imported="$test_root/imported"
fixture "$imported" "example-domain" '
use std::env;
pub fn capability() -> bool { env::var_os("EXAMPLE_SWITCH").is_some() }
'
expect_rejected "$imported"

iterated="$test_root/iterated"
fixture "$iterated" "example-application" '
pub fn switches() -> usize { std::env::vars().count() }
'
expect_rejected "$iterated"

# Compile-time reads bake one machine's value into the artifact, so they are
# refused rather than treated as a way around the runtime rule.
compile_time="$test_root/compile-time"
fixture "$compile_time" "example-domain" '
pub const SWITCH: Option<&str> = option_env!("EXAMPLE_SWITCH");
'
expect_rejected "$compile_time"

# A crate whose name merely contains the word must not be caught; the rule is
# about the layer, which the directory suffix names.
lookalike="$test_root/lookalike"
fixture "$lookalike" "example-application-support" '
pub fn capability() -> bool { std::env::var("EXAMPLE_SWITCH").is_ok() }
'
expect_allowed "$lookalike"

echo "OK no-env-access-in-domain-layers-self-test"
