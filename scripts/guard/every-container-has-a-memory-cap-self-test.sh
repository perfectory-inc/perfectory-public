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
  local api_cap="$1" api_restart="${2-    restart: unless-stopped}" api_logging="${3-    logging: *log-cap}"
  cat > "$fixture/stack/docker-compose.yml" <<YAML
x-log-cap: &log-cap
  driver: json-file
  options:
    max-size: "50m"
    max-file: "3"
services:
  migrate:
    image: fixture/migrate:1
    mem_limit: 3g
    logging: *log-cap
    restart: "no"
  seed:
    image: fixture/seed:1
    logging: *log-cap
    mem_limit: 3g
  api:
    image: fixture/api:1
${api_logging}
${api_cap}
${api_restart}
    depends_on:
      seed:
        condition: service_completed_successfully
  debug:
    image: fixture/debug:1
    logging: *log-cap
    mem_limit: 8g
    profiles:
      - debug
YAML
  cat > "$fixture/local/compose.yaml" <<YAML
x-log-cap: &log-cap
  driver: json-file
  options:
    max-size: "50m"
    max-file: "3"
services:
  proof:
    image: fixture/proof:1
    logging: *log-cap
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

write_stack "    mem_limit: 1g" "    restart: \"no\"" "    logging: *log-cap"
expect_pass "a one-shot service needs no restart policy"
write_stack "    mem_limit: 1g" "" "    logging: *log-cap"
expect_fail "a service that stays up on the host but does not restart"
write_stack "    mem_limit: 1g" "    restart: unless-stopped" ""
expect_fail "a service that names no log cap"
write_stack "    mem_limit: 1g"
sed -i 's/^    max-file: "3"$//' "$fixture/stack/docker-compose.yml"
expect_fail "a log cap anchor without max-file"

write_stack "    mem_limit: 1g"
mkdir -p "$fixture/new-stack"
printf 'services:\n  x:\n    image: fixture/x:1\n    mem_limit: 64m\n' > "$fixture/new-stack/compose.yml"
expect_fail "a compose file the contract does not place"

mv "$fixture/new-stack" "$fixture/elsewhere"
expect_pass "a compose file under an outside_scope prefix"

printf 'services:\n  proof:\n    image: fixture/proof:1\n' > "$fixture/local/compose.yaml"
expect_fail "an uncapped service in a stack that never runs on the host"

# Containers that code starts itself count as one-shot jobs at the caps their contract states.
write_one_shot_fixture() {
  local containers="$1" entry="${2:-}"
  write_contract 6g
  write_stack "    mem_limit: 1g"
  printf '{"images": %s}\n' "$containers" > "$fixture/tools/bake.json"
  python3 - "$fixture" "$entry" <<'PY'
import json, pathlib, sys
root, entry = pathlib.Path(sys.argv[1]), sys.argv[2]
path = root / "tools/host-memory-budget.contract.json"
contract = json.loads(path.read_text())
contract["one_shot_contracts"] = [
    json.loads(entry) if entry else {"contract": "tools/bake.json", "containers": ["images"], "why": "fixture"}
]
path.write_text(json.dumps(contract))
PY
}

# 1g standing + 3g compose job + 1g reserved leaves 1g of the 6g host: a 4g contract job fits, 5g not.
write_one_shot_fixture '{"gdal": {"memory_limit": "2g"}, "tile": {"memory_limit": "4g"}}'
expect_pass "contract one-shot caps that do not exceed what the host has left"
write_one_shot_fixture '{"gdal": {"memory_limit": "2g"}, "tile": {"memory_limit": "5g"}}'
expect_fail "a contract one-shot cap larger than the host has left"
write_one_shot_fixture '{"gdal": {"memory_limit": "2g"}, "tile": {"image": "fixture/tile:1"}}'
expect_fail "a contract container without a memory_limit"
write_one_shot_fixture '{"gdal": {"memory_limit": "plenty"}}'
expect_fail "a contract memory_limit that is not a size"
write_one_shot_fixture '{}'
expect_fail "a one-shot contract that names no containers"
write_one_shot_fixture '{"gdal": {"memory_limit": "2g"}}' '{"contract": "../bake.json", "containers": ["images"], "why": "x"}'
expect_fail "a one-shot contract outside the repository"
write_one_shot_fixture '{"gdal": {"memory_limit": "2g"}}' '{"contract": "tools/bake.json", "containers": ["absent"], "why": "x"}'
expect_fail "a one-shot contract whose containers member is missing"
write_one_shot_fixture '{"gdal": {"memory_limit": "2g"}}' '{"contract": "tools/bake.json", "containers": ["images"]}'
expect_fail "a one-shot contract entry without its reason"
rm -f "$fixture/tools/bake.json"

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

# Scheduled jobs in a shared pool count at every combination its slots allow (root ADR-0138).
# Host 6g: 1g standing + 1g reserved leaves 4g. One-shots: migrate 3g, seed 3g, small 2g.
# Pool "heavy" (3 slots): floor takes 3 (migrate 3g), each fold 2 (small 2g), the bake 1
# (its unit's MemoryMax 2G). The worst sets are fold + bake = 4g and floor alone = 3g: they fit.
# With a fifth argument a Gold rebuild joins the pool on the big Spark (migrate 3g) with that many
# slots (root ADR-0139).
write_scheduled_fixture() {
  local bake_memory="${1-MemoryMax=2G}" fold_slots="${2:-2}" small_cap="${3:-2g}" extra_memory="${4:-}"
  local gold_slots="${5:-0}"
  rm -f "$fixture/stack/compose.native.yml" "$fixture/tools/engine.json"
  write_contract 6g
  write_stack "    mem_limit: 1g"
  cat >> "$fixture/stack/docker-compose.yml" <<YAML
  small:
    image: fixture/small:1
    logging: *log-cap
    mem_limit: ${small_cap}
    restart: "no"
YAML
  mkdir -p "$fixture/units"
  printf '[Service]\nExecStart=/bin/true\n%s\n' "$bake_memory" > "$fixture/units/fixture-bake.service"
  for unit in fixture-floor fixture-fold-a fixture-fold-b fixture-gold; do
    printf '[Service]\nExecStart=/bin/true\n' > "$fixture/units/$unit.service"
  done
  python3 - "$fixture" "$fold_slots" "$extra_memory" "$gold_slots" <<'PY'
import json, pathlib, sys
root, fold_slots, extra, gold_slots = pathlib.Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
def job(job_id, slots, pool="heavy"):
    return {"id": job_id, "pool": pool, "pool_slots": slots, "systemd_service": f"fixture-{job_id.replace('_', '-')}.service"}
(root / "tools/jobs.json").write_text(json.dumps({
    "pools": {"heavy": {"slots": 3, "description": "fixture"}},
    "jobs": [job("floor", 3), job("fold_a", fold_slots), job("fold_b", fold_slots), job("bake", 1),
             job("sweep", 1, "default_pool")] + ([job("gold", gold_slots)] if gold_slots else []),
}))
memory = {
    "floor": [{"project": "stack", "service": "migrate"}],
    "fold_a": [{"project": "stack", "service": "small"}],
    "fold_b": [{"project": "stack", "service": "small"}],
    "bake": [{"unit": "MemoryMax"}],
}
if gold_slots:
    memory["gold"] = [{"project": "stack", "service": "migrate"}]
if extra:
    memory.update(json.loads(extra))
path = root / "tools/host-memory-budget.contract.json"
contract = json.loads(path.read_text())
contract["scheduled_jobs"] = {"jobs": "tools/jobs.json", "units": "units", "memory": memory, "why": "fixture"}
path.write_text(json.dumps(contract))
PY
}

write_scheduled_fixture
expect_pass "scheduled jobs whose every slot combination fits"
write_scheduled_fixture ""
expect_fail "a bake unit without MemoryMax"
write_scheduled_fixture "MemoryMax=3G"
expect_fail "a bake MemoryMax that pushes a fold and the bake over the host"
write_scheduled_fixture "MemoryMax=2G" 1
expect_fail "two one-slot folds and the bake whose sum the slots allow but the host does not"
write_scheduled_fixture "MemoryMax=2G" 2 3g
expect_fail "a small Spark cap that pushes a fold and the bake over the host"
write_scheduled_fixture "MemoryMax=2G" 2 2g '{"fold_b": []}'
expect_fail "a pooled job that names no memory source"
write_scheduled_fixture "MemoryMax=2G" 2 2g '{"gone": [{"unit": "MemoryMax"}]}'
expect_fail "a memory entry for a job the list does not have"
write_scheduled_fixture "MemoryMax=2G" 2 2g '{"fold_a": [{"project": "stack", "service": "absent"}]}'
expect_fail "a memory source naming a service the host does not run"
write_scheduled_fixture "MemoryMax=infinity"
expect_fail "a MemoryMax that is not a size"
write_scheduled_fixture "MemoryMax=2G" 2 2g "" 3
expect_pass "a Gold rebuild on the big Spark that takes every slot and runs alone"
write_scheduled_fixture "MemoryMax=2G" 2 2g "" 1
expect_fail "a Gold rebuild on the big Spark that takes one slot, so a fold runs beside it over the host"

echo "OK $name"
