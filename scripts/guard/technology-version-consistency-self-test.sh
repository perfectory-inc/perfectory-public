#!/usr/bin/env bash
set -euo pipefail
# Fixtures below are disposable repositories. A hook runs with GIT_DIR pointing
# at the real checkout, which would redirect their commits into it; sourcing the
# shared definition releases that binding for the rest of this script.
. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/technology-version-consistency.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

# Fixture manifests state whatever the pin contract currently says (root ADR-0097): the
# self-test proves the checker's failure paths, not a second copy of the pin values.
pin() {
  python3 - "$root/tools/technology-versions.contract.json" "$1" <<'PY' | tr -d '\r'
import json
import sys

print(json.load(open(sys.argv[1], encoding="utf-8"))["manifest_exact_pins"][sys.argv[2]])
PY
}
node_pin="$(pin node)"
pnpm_pin="$(pin pnpm)"
package_manager_pin="$(pin packageManager)"
typescript_pin="$(pin typescript)"
vitest_pin="$(pin vitest)"
rust_pin="$(python3 - "$root/tools/technology-versions.contract.json" <<'PY' | tr -d '\r'
import json
import sys

print(json.load(open(sys.argv[1], encoding="utf-8"))["rust_toolchain"])
PY
)"

# The guard reads the Rust pin from the contract; a literal copy of it in the guard is a second
# source that a bump would leave behind.
if grep -Fq "$rust_pin" "$checker"; then
  echo "FAIL technology-version-consistency-self-test: the guard restates the Rust pin $rust_pin instead of reading the contract" >&2
  exit 1
fi

git -C "$fixture" init -q
git -C "$fixture" config user.email guard@example.test
git -C "$fixture" config user.name guard
mkdir -p "$fixture/platforms/foundation-platform/services/foundation-profile-gateway" "$fixture/products/gongzzang"
printf '%s\n' 'services:' '  postgres:' '    image: postgres:17-alpine@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' '  valkey:' '    image: valkey/valkey:8-alpine@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' > "$fixture/platforms/foundation-platform/docker-compose.yml"
printf '%s\n' '{' "  \"packageManager\": \"$package_manager_pin\"," '  "engines": {' "    \"node\": \"$node_pin\"," "    \"pnpm\": \"$pnpm_pin\"" '  }' '}' > "$fixture/products/gongzzang/package.json"
printf '%s\n' '{' "  \"packageManager\": \"$package_manager_pin\"," '  "engines": {' "    \"node\": \"$node_pin\"," "    \"pnpm\": \"$pnpm_pin\"" '  },' '  "devDependencies": {' "    \"typescript\": \"$typescript_pin\"," "    \"vitest\": \"$vitest_pin\"" '  }' '}' > "$fixture/platforms/foundation-platform/services/foundation-profile-gateway/package.json"
printf '%s\n' '[toolchain]' "channel = \"$rust_pin\"" > "$fixture/rust-toolchain.toml"
mkdir -p "$fixture/products/gongzzang/crates/member"
printf '%s\n' '[workspace]' 'members = ["crates/member"]' '' '[workspace.package]' "rust-version = \"$rust_pin\"" > "$fixture/products/gongzzang/Cargo.toml"
printf '%s\n' '[package]' 'name = "member"' 'rust-version.workspace = true' > "$fixture/products/gongzzang/crates/member/Cargo.toml"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

expect_refused() {
  git -C "$fixture" add -A
  if bash "$checker" "$fixture" >/dev/null 2>&1; then
    echo "FAIL technology-version-consistency-self-test: $1 was accepted" >&2
    exit 1
  fi
}

sed -i "s/rust-version = \"$rust_pin\"/rust-version = \"0.0.1\"/" "$fixture/products/gongzzang/Cargo.toml"
expect_refused 'workspace rust-version drift'
sed -i "s/rust-version = \"0.0.1\"/rust-version = \"$rust_pin\"/" "$fixture/products/gongzzang/Cargo.toml"

sed -i "s/rust-version.workspace = true/rust-version = \"$rust_pin\"/" "$fixture/products/gongzzang/crates/member/Cargo.toml"
expect_refused 'a member crate restating rust-version'
sed -i "s/rust-version = \"$rust_pin\"/rust-version.workspace = true/" "$fixture/products/gongzzang/crates/member/Cargo.toml"

sed -i "s/channel = \"$rust_pin\"/channel = \"0.0.1\"/" "$fixture/rust-toolchain.toml"
expect_refused 'rust-toolchain.toml drift'
sed -i "s/channel = \"0.0.1\"/channel = \"$rust_pin\"/" "$fixture/rust-toolchain.toml"

printf '%s\n' '[toolchain]' "channel = \"$rust_pin\"" > "$fixture/products/gongzzang/rust-toolchain.toml"
expect_refused 'an area-local rust-toolchain.toml'
rm "$fixture/products/gongzzang/rust-toolchain.toml"

utoipa_requirement="$(python3 - "$root/tools/technology-versions.contract.json" <<'PY' | tr -d '\r'
import json
import sys

print(json.load(open(sys.argv[1], encoding="utf-8"))["cargo_version_requirements"]["utoipa"])
PY
)"
printf '%s\n' '' '[workspace.dependencies]' "utoipa = { version = \"$utoipa_requirement\", features = [\"chrono\"] }" >> "$fixture/products/gongzzang/Cargo.toml"
printf '%s\n' '' '[dependencies]' 'utoipa = { workspace = true }' >> "$fixture/products/gongzzang/crates/member/Cargo.toml"
git -C "$fixture" add -A
bash "$checker" "$fixture" >/dev/null
sed -i "s/utoipa = { version = \"$utoipa_requirement\"/utoipa = { version = \"0.0.1\"/" "$fixture/products/gongzzang/Cargo.toml"
expect_refused 'a shared crate requirement off the contract'
sed -i "s/utoipa = { version = \"0.0.1\"/utoipa = { version = \"$utoipa_requirement\"/" "$fixture/products/gongzzang/Cargo.toml"
printf '%s\n' 'utoipa = "0.0.1"' >> "$fixture/products/gongzzang/crates/member/Cargo.toml"
expect_refused 'a member crate restating a shared crate requirement'
sed -i '$d' "$fixture/products/gongzzang/crates/member/Cargo.toml"

printf '%s\n' 'FROM rust:0.0.1-bookworm' > "$fixture/products/gongzzang/Dockerfile"
expect_refused 'a Dockerfile rust base off the pin'
rm "$fixture/products/gongzzang/Dockerfile"
git -C "$fixture" add -A
bash "$checker" "$fixture" >/dev/null

sed -i 's/postgres:17/postgres:16/' "$fixture/platforms/foundation-platform/docker-compose.yml"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: postgres drift was accepted' >&2
  exit 1
fi

sed -i 's/postgres:16/postgres:17/' "$fixture/platforms/foundation-platform/docker-compose.yml"
sed -i 's#valkey/valkey:8#redis:7#' "$fixture/platforms/foundation-platform/docker-compose.yml"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: cache drift was accepted' >&2
  exit 1
fi

sed -i 's#redis:7#valkey/valkey:8#' "$fixture/platforms/foundation-platform/docker-compose.yml"
sed -i "s/\"node\": \"$node_pin\"/\"node\": \"0.0.1\"/" "$fixture/products/gongzzang/package.json"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: node drift was accepted' >&2
  exit 1
fi

sed -i "s/\"node\": \"0.0.1\"/\"node\": \"$node_pin\"/" "$fixture/products/gongzzang/package.json"
sed -i "s/\"typescript\": \"$typescript_pin\"/\"typescript\": \"0.0.1\"/" \
  "$fixture/platforms/foundation-platform/services/foundation-profile-gateway/package.json"
git -C "$fixture" add .
if bash "$checker" "$fixture" >/dev/null 2>&1; then
  echo 'FAIL technology-version-consistency-self-test: Foundation Worker TypeScript drift was accepted' >&2
  exit 1
fi

echo 'OK technology-version-consistency-self-test'
