#!/usr/bin/env bash
# Proves container-images-match-the-contract refuses a drifted digest, an unlisted image and an
# unused entry, and that --write brings a drifted reference back to the contract.
set -euo pipefail
dir="$(cd "$(dirname "$0")" && pwd -P)"
. "$dir/lib/fixture-repo.sh"
root="$(cd "$dir/../.." && pwd -P)"
checker="$root/scripts/guard/container-images-match-the-contract.sh"
sync="$root/scripts/catalog/sync-container-images.py"
name="container-images-match-the-contract-self-test"

fixture_root
repo="$FIXTURE_ROOT/repo"
mkdir -p "$repo/tools" "$repo/stack"
fixture_git "$repo" init -q
fixture_git "$repo" config user.email guard@example.invalid
fixture_git "$repo" config user.name guard

a="$(printf 'a%.0s' $(seq 64))"
b="$(printf 'b%.0s' $(seq 64))"
contract() {
  printf '{"container_images": {%s}}\n' "$1" >"$repo/tools/technology-versions.contract.json"
}
compose() {
  printf 'services:\n  db:\n    image: fixture/db:1.0@sha256:%s\n' "$1" >"$repo/stack/compose.yml"
  fixture_git "$repo" add -A
}

expect() {
  local want="$1" why="$2"
  if bash "$checker" "$repo" >/dev/null 2>&1; then got=pass; else got=fail; fi
  [[ "$got" == "$want" ]] || {
    echo "FAIL $name: $why: expected $want, got $got" >&2
    exit 1
  }
}

contract "\"fixture/db:1.0\": \"sha256:$a\""
compose "$a"
expect pass "a reference that matches the contract"

compose "$b"
expect fail "a reference pinned to another digest"

python3 "$sync" --write --root "$repo" >/dev/null
grep -q "sha256:$a" "$repo/stack/compose.yml" || {
  echo "FAIL $name: --write did not restore the contract digest" >&2
  exit 1
}
expect pass "the reference --write rewrote"

contract "\"fixture/other:2.0\": \"sha256:$a\""
expect fail "an image the contract does not list"

contract "\"fixture/db:1.0\": \"sha256:$a\", \"fixture/unused:1.0\": \"sha256:$b\""
expect fail "a contract entry no file uses"

# An OpenTofu stack requires the version of the list's OpenTofu image (2026-11-25: #1620 moved the image
# and left four stacks on the old required_version, which that image refuses).
mkdir -p "$repo/infra/stack"
stack() {
  printf 'terraform {\n  required_version = "%s"\n}\n' "$1" >"$repo/infra/stack/main.tf"
  printf '# applied in ghcr.io/opentofu/opentofu:1.13.10@sha256:%s\n' "$a" >"$repo/stack/apply.sh"
  fixture_git "$repo" add -A
}
contract "\"fixture/db:1.0\": \"sha256:$a\", \"ghcr.io/opentofu/opentofu:1.13.10\": \"sha256:$a\""
stack 1.13.10
expect pass "a stack that requires the image's OpenTofu"

stack 1.13.9
expect fail "a stack that requires another OpenTofu than the image's"

python3 "$sync" --write --root "$repo" >/dev/null
grep -q 'required_version = "1.13.10"' "$repo/infra/stack/main.tf" || {
  echo "FAIL $name: --write did not move required_version to the image's OpenTofu" >&2
  exit 1
}
expect pass "the required_version --write rewrote"

echo "OK $name"
