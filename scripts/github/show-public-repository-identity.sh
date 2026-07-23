#!/usr/bin/env bash
# Prints the immutable identity candidate for the one approved GitHub.com
# repository. It validates but never edits the checked-in policy.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
target="perfectory-inc/perfectory-public"
helper="$root/scripts/github/github-policy-json.py"
if [ -n "${GH_HOST:-}" ] && [ "$GH_HOST" != github.com ]; then
  echo "FAIL public-repository-identity: GH_HOST must be unset or github.com" >&2
  exit 1
fi
for command_name in gh mktemp python3; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-repository-identity: missing command '$command_name'" >&2
    exit 1
  }
done

candidate="$(mktemp)"
cleanup() {
  [ ! -e "${candidate:-}" ] || rm -f -- "$candidate"
}
trap cleanup EXIT

env -u GH_HOST gh api --hostname github.com "repos/$target" --jq '{
  hostname: "github.com",
  full_name,
  repository_id: .id,
  repository_node_id: .node_id,
  owner: {login: .owner.login, id: .owner.id, node_id: .owner.node_id}
}' >"$candidate"
python3 "$helper" validate-repository-identity "$candidate"
python3 "$helper" canonical "$candidate"
