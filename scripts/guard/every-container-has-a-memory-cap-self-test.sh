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

# The overlay's required parameter reads its one numeric source, never the caller's environment.
write_parameter_fixture() {
  write_contract 6g
  write_stack "    mem_limit: 1g"
  printf '{"execution_profile":{"memory_mib":1024}}\n' > "$fixture/tools/engine.json"
  python3 - "$fixture" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
path = root / "tools/host-memory-budget.contract.json"
contract = json.loads(path.read_text())
contract["projects"][0]["files"].append("stack/compose.native.yml")
contract["memory_parameters"] = {
    "NATIVE_MEMORY_MIB": {"contract": "tools/engine.json", "json_path": ["execution_profile", "memory_mib"]}
}
path.write_text(json.dumps(contract))
PY
  write_parameter_overlay '${NATIVE_MEMORY_MIB:?native memory is required}m'
}

write_parameter_overlay() {
  printf 'services:\n  api:\n    mem_limit: "%s"\n' "$1" > "$fixture/stack/compose.native.yml"
}

write_parameter_fixture
export NATIVE_MEMORY_MIB=999999
expect_pass "a required cap parameter resolved from its source contract instead of ambient environment"
printf '{"execution_profile":{"memory_mib":3072}}\n' > "$fixture/tools/engine.json"
expect_fail "the source contract increases the host total beyond physical memory"

for source in '{}' '{"execution_profile":{"memory_mib":true}}' \
  '{"execution_profile":{"memory_mib":0}}' '{"execution_profile":{"memory_mib":-1}}' \
  '{"execution_profile":{"memory_mib":1.5}}' '{"execution_profile":{"memory_mib":"1024"}}' 'invalid-json'; do
  printf '%s\n' "$source" > "$fixture/tools/engine.json"
  expect_fail "a missing, non-positive or non-integer source memory value"
done

write_parameter_fixture
export UNKNOWN_MEMORY=1024
for interpolation in '${UNKNOWN_MEMORY:?required}m' '${NATIVE_MEMORY_MIB:-1024}m' \
  '${NATIVE_MEMORY_MIB}m' '${NATIVE_MEMORY_MIB:?}m' '${NATIVE_MEMORY_MIB:?${OTHER}}m' \
  '${NATIVE_MEMORY_MIB:?required}m extra'; do
  write_parameter_overlay "$interpolation"
  expect_fail "unsupported or unbound memory interpolation"
done
unset NATIVE_MEMORY_MIB UNKNOWN_MEMORY

for defect in missing-file absolute escape symlink missing-path wrong-path-type wrong-binding-type extra-field wrong-map-type; do
  write_parameter_fixture
  python3 - "$fixture" "$defect" <<'PY'
import json, pathlib, sys
root, defect = pathlib.Path(sys.argv[1]), sys.argv[2]
path = root / "tools/host-memory-budget.contract.json"
contract = json.loads(path.read_text())
binding = contract["memory_parameters"]["NATIVE_MEMORY_MIB"]
if defect == "missing-file":
    binding["contract"] = "tools/absent.json"
elif defect == "absolute":
    binding["contract"] = str(root / "tools/engine.json")
elif defect == "escape":
    binding["contract"] = "../engine.json"
elif defect == "symlink":
    (root / "tools/escape.json").symlink_to(root.parent)
    binding["contract"] = "tools/escape.json"
elif defect == "missing-path":
    binding.pop("json_path")
elif defect == "wrong-path-type":
    binding["json_path"] = "execution_profile.memory_mib"
elif defect == "wrong-binding-type":
    contract["memory_parameters"]["NATIVE_MEMORY_MIB"] = 1024
elif defect == "extra-field":
    binding["fallback"] = 1024
elif defect == "wrong-map-type":
    contract["memory_parameters"] = []
path.write_text(json.dumps(contract))
PY
  expect_fail "invalid memory parameter binding: $defect"
done

echo "OK $name"
