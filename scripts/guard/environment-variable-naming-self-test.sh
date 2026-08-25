#!/usr/bin/env bash
set -euo pipefail

. "$(dirname "$0")/lib/fixture-repo.sh"

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/environment-variable-naming.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

mkdir -p \
  "$fixture/platforms/foundation-platform/config" \
  "$fixture/platforms/foundation-platform/services/foundation-outbox-publisher/src" \
  "$fixture/platforms/foundation-platform/services/foundation-provider-acquisition-worker/src/foundation_provider_acquisition"
cp "$root/platforms/foundation-platform/config/environment-variable-naming.contract.json" \
  "$fixture/platforms/foundation-platform/config/environment-variable-naming.contract.json"
cat > "$fixture/platforms/foundation-platform/config/r2-connections.contract.json" <<'EOF'
{
  "schema_version": 2,
  "profile_gateway": {"r2_binding": "FOUNDATION_PLATFORM_LAKEHOUSE"}
}
EOF
for example in .env.example .env.local.example; do
  cat > "$fixture/platforms/foundation-platform/$example" <<'EOF'
FOUNDATION_PLATFORM_VWORLD_API_KEY=
FOUNDATION_PLATFORM_VWORLD_DOMAIN=
FOUNDATION_PLATFORM_VWORLD_USERNAME=
FOUNDATION_PLATFORM_VWORLD_PASSWORD=
DATA_GO_KR_SERVICE_KEY=
EOF
done
cat > "$fixture/platforms/foundation-platform/services/foundation-outbox-publisher/src/vworld_credentials.rs" <<'EOF'
const CONTRACT: &str = include_str!("../../../config/environment-variable-naming.contract.json");
fn read_contract() {
    let _: serde_json::Value = serde_json::from_str(CONTRACT).unwrap();
    let deprecated_aliases: Vec<String> = Vec::new();
}
EOF
cat > "$fixture/platforms/foundation-platform/services/foundation-provider-acquisition-worker/src/foundation_provider_acquisition/vworld_credentials.py" <<'EOF'
import json
from pathlib import Path

_CONTRACT_FILENAME = "environment-variable-naming.contract.json"

def _contract_path() -> Path:
    return Path("config") / _CONTRACT_FILENAME

def read_contract():
    with _contract_path().open(encoding="utf-8") as handle:
        contract = json.load(handle)
    return contract["compatibility_migrations"]["foundation-vworld-credentials"]["credentials"]["username"]["deprecated_aliases"]
EOF

git -C "$fixture" init -q
git -C "$fixture" config user.email guard@example.invalid
git -C "$fixture" config user.name guard
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

expect_failure() {
  local label="$1"
  if bash "$checker" "$fixture" >/dev/null 2>&1; then
    echo "FAIL environment-variable-naming-self-test: $label was accepted" >&2
    exit 1
  fi
}

rust_adapter="$fixture/platforms/foundation-platform/services/foundation-outbox-publisher/src/vworld_credentials.rs"
python_adapter="$fixture/platforms/foundation-platform/services/foundation-provider-acquisition-worker/src/foundation_provider_acquisition/vworld_credentials.py"
cp "$rust_adapter" "$fixture/rust-adapter.clean.rs"
cp "$python_adapter" "$fixture/python-adapter.clean.py"
cat > "$python_adapter" <<'EOF'
# json.load reads environment-variable-naming.contract.json and resolves canonical before deprecated_aliases.
NAMES = {"username": "VWORLD_USERNAME"}
EOF
git -C "$fixture" add .
expect_failure "a hardcoded adapter disguised by contract-marker comments"
cp "$fixture/python-adapter.clean.py" "$python_adapter"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

cat > "$rust_adapter" <<'EOF'
// include_str!("environment-variable-naming.contract.json") and serde_json::from_str(CONTRACT)
// would normally resolve deprecated_aliases.
const USERNAME: &str = "VWORLD_USERNAME";
EOF
git -C "$fixture" add .
expect_failure "a hardcoded Rust adapter disguised by contract-marker comments"
cp "$fixture/rust-adapter.clean.rs" "$rust_adapter"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

cat > "$fixture/platforms/foundation-platform/Dockerfile.contract-bypass" <<'EOF'
ENV VWORLD_DOMAIN=example.invalid
EOF
git -C "$fixture" add .
expect_failure "a deprecated alias in a Dockerfile"
rm "$fixture/platforms/foundation-platform/Dockerfile.contract-bypass"
git -C "$fixture" add -u
bash "$checker" "$fixture" >/dev/null

r2_contract="$fixture/platforms/foundation-platform/config/r2-connections.contract.json"
cp "$r2_contract" "$fixture/r2.clean.json"
sed 's/FOUNDATION_PLATFORM_LAKEHOUSE/LAKEHOUSE/' "$fixture/r2.clean.json" > "$r2_contract"
git -C "$fixture" add .
expect_failure "an ownerless Worker binding"
cp "$fixture/r2.clean.json" "$r2_contract"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

printf '%s\n' 'let key = std::env::var("VWORLD_API_KEY");' \
  > "$fixture/platforms/foundation-platform/services/foundation-outbox-publisher/src/direct.rs"
git -C "$fixture" add .
expect_failure "a deprecated alias outside the compatibility adapter"
rm "$fixture/platforms/foundation-platform/services/foundation-outbox-publisher/src/direct.rs"
git -C "$fixture" add -u
bash "$checker" "$fixture" >/dev/null

sed 's/FOUNDATION_PLATFORM_VWORLD_API_KEY/VWORLD_API_KEY/' \
  "$fixture/platforms/foundation-platform/.env.example" \
  > "$fixture/platforms/foundation-platform/.env.example.bad"
mv "$fixture/platforms/foundation-platform/.env.example.bad" \
  "$fixture/platforms/foundation-platform/.env.example"
git -C "$fixture" add .
expect_failure "a deprecated alias in the tracked example"
cp "$fixture/platforms/foundation-platform/.env.local.example" \
  "$fixture/platforms/foundation-platform/.env.example"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

naming_contract="$fixture/platforms/foundation-platform/config/environment-variable-naming.contract.json"
cp "$naming_contract" "$fixture/naming.clean.json"
python3 - "$naming_contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["external_tool_exceptions"][0]["reference"] = ""
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY
git -C "$fixture" add .
expect_failure "an external-tool exception without primary evidence"
cp "$fixture/naming.clean.json" "$naming_contract"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

python3 - "$naming_contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["compatibility_migrations"]["foundation-vworld-credentials"]["status"] = "permanent"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY
git -C "$fixture" add .
expect_failure "a permanent compatibility alias migration"
cp "$fixture/naming.clean.json" "$naming_contract"
git -C "$fixture" add .
bash "$checker" "$fixture" >/dev/null

echo 'OK environment-variable-naming-self-test'
