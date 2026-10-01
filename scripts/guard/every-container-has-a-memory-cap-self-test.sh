#!/usr/bin/env bash
# Proves every-container-has-a-memory-cap refuses what it exists to refuse, and counts the host
# total the way its header says: standing services at their cap, only the largest one-shot job,
# nothing a profile the host does not run.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/every-container-has-a-memory-cap.sh"
name="every-container-has-a-memory-cap-self-test"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

write_contract() {
  local physical="$1"
  mkdir -p "$fixture/tools" "$fixture/stack" "$fixture/local"
  cat > "$fixture/tools/host-memory-budget.contract.json" <<JSON
{
  "schema_version": 1,
  "host": {"name": "fixture-host", "physical_memory": "$physical", "host_reserved": "1g", "uncapped": []},
  "projects": [{"name": "stack", "files": ["stack/docker-compose.yml"], "profiles": ["batch"]}],
  "not_on_host": [{"files": ["local/compose.yaml"], "why": "fixture"}],
  "outside_scope": [{"path_prefix": "elsewhere/", "why": "fixture"}]
}
JSON
}

# One standing service (1g), two one-shot jobs (3g each: one by restart, one by being waited on),
# one service behind a profile the host does not run (8g).
write_stack() {
  local api_cap="$1"
  cat > "$fixture/stack/docker-compose.yml" <<YAML
services:
  migrate:
    image: fixture/migrate:1
    mem_limit: 3g
    restart: "no"
  seed:
    image: fixture/seed:1
    mem_limit: 3g
  api:
    image: fixture/api:1
${api_cap}
    restart: unless-stopped
    depends_on:
      seed:
        condition: service_completed_successfully
  debug:
    image: fixture/debug:1
    mem_limit: 8g
    profiles:
      - debug
YAML
  cat > "$fixture/local/compose.yaml" <<YAML
services:
  proof:
    image: fixture/proof:1
    mem_limit: 512m
YAML
}

expect_pass() {
  bash "$checker" "$fixture" >/dev/null 2>&1 || {
    echo "FAIL $name: $1 was refused" >&2
    bash "$checker" "$fixture" >&2 || true
    exit 1
  }
}

expect_fail() {
  if bash "$checker" "$fixture" >/dev/null 2>&1; then
    echo "FAIL $name: $1 was accepted" >&2
    exit 1
  fi
}

# 1g standing + 3g largest job + 1g reserved = 5g. Summing both jobs (8g) or counting the
# profiled service (13g) would exceed 6g, so passing proves the arithmetic.
write_contract 6g
write_stack "    mem_limit: 1g"
expect_pass "a host whose standing caps, largest job and reserve fit"

write_contract 4g
expect_fail "a host whose caps add up to more than it has"

write_contract 6g
write_stack ""
expect_fail "a service with no mem_limit"

write_stack "    mem_limit: lots"
expect_fail "a mem_limit that is not a size"

write_stack "    mem_limit: 1g"
mkdir -p "$fixture/new-stack"
printf 'services:\n  x:\n    image: fixture/x:1\n    mem_limit: 64m\n' > "$fixture/new-stack/compose.yml"
expect_fail "a compose file the contract does not place"

mv "$fixture/new-stack" "$fixture/elsewhere"
expect_pass "a compose file under an outside_scope prefix"

printf 'services:\n  proof:\n    image: fixture/proof:1\n' > "$fixture/local/compose.yaml"
expect_fail "an uncapped service in a stack that never runs on the host"

echo "OK $name"
