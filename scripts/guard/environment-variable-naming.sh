#!/usr/bin/env bash
# Threat model: honest-mistake detection, not deliberate-bypass prevention.
# Prevents: tracked Foundation VWorld credentials and the profile-gateway binding from losing their
# owner namespace, and keeps externally owned tool-name exceptions reviewable in one contract.
# Does not prevent: a deliberate edit that preserves the checked syntax while changing runtime
# behavior, or ownerless provider inputs outside the enumerated VWorld contract. The expected-pass
# fixture includes DATA_GO_KR_SERVICE_KEY to keep that lexical/path boundary explicit.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"

if python3 - "$root" <<'PY'
from __future__ import annotations

import ast
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path


root = Path(sys.argv[1])
naming_path = root / "platforms/foundation-platform/config/environment-variable-naming.contract.json"
r2_path = root / "platforms/foundation-platform/config/r2-connections.contract.json"


def fail(message: str) -> None:
    raise SystemExit(message)


def read_json(path: Path) -> dict[str, object]:
    try:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path.relative_to(root)}: {error}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(root)} must contain a JSON object")
    return value


naming = read_json(naming_path)
if naming.get("schema_version") != 1:
    fail("environment-variable naming contract schema_version must be 1")

namespaces = naming.get("owned_namespaces")
if namespaces != {"foundation-platform": "FOUNDATION_PLATFORM_"}:
    fail("foundation-platform owned namespace must be FOUNDATION_PLATFORM_")

r2 = read_json(r2_path)
profile_gateway = r2.get("profile_gateway")
if not isinstance(profile_gateway, dict):
    fail("R2 connection contract must declare profile_gateway")
canonical_binding = profile_gateway.get("r2_binding")
if not isinstance(canonical_binding, str) or not canonical_binding.startswith(
    namespaces["foundation-platform"]
):
    fail("profile gateway R2 binding must carry the Foundation owner namespace")
if "_R2_" in canonical_binding:
    fail("profile gateway R2 binding must not repeat its declared R2 resource type")

wrangler_path = root / "platforms/foundation-platform/services/foundation-profile-gateway/wrangler.jsonc"
if wrangler_path.is_file():
    wrangler = read_json(wrangler_path)
    buckets = wrangler.get("r2_buckets")
    if (
        not isinstance(buckets, list)
        or len(buckets) != 1
        or not isinstance(buckets[0], dict)
        or buckets[0].get("binding") != canonical_binding
    ):
        fail("generated Wrangler configuration drifted from the canonical R2 binding")

external = naming.get("external_tool_exceptions")
if not isinstance(external, list) or not external:
    fail("external_tool_exceptions must be a list")
external_selectors: set[tuple[str, str]] = set()
for item in external:
    if not isinstance(item, dict):
        fail("every external tool exception must be an object")
    selector = item.get("selector")
    selector_kind = item.get("selector_kind")
    consumer = item.get("consumer")
    if not isinstance(selector, str) or not selector:
        fail("every external tool exception must name a selector")
    if selector_kind not in {"exact", "prefix"}:
        fail("every external tool exception selector_kind must be exact or prefix")
    if not isinstance(consumer, str) or not consumer:
        fail("every external tool exception must name its direct consumer")
    if not isinstance(item.get("reference"), str) or not item["reference"].startswith("https://"):
        fail("every external tool exception must cite an HTTPS primary reference")
    key = (selector, selector_kind)
    if key in external_selectors:
        fail(f"external tool exception selector is duplicated: {selector}")
    external_selectors.add(key)

migrations = naming.get("compatibility_migrations")
if not isinstance(migrations, dict):
    fail("compatibility_migrations must be an object")
vworld = migrations.get("foundation-vworld-credentials")
if not isinstance(vworld, dict):
    fail("foundation-vworld-credentials migration must be an object")
if vworld.get("status") != "temporary" or vworld.get("precedence") != "canonical-first":
    fail("foundation-vworld-credentials must be temporary and canonical-first")
if vworld.get("warning") != {
    "emit": "names-only",
    "when": "deprecated-alias-supplies-value",
}:
    fail("foundation-vworld-credentials warning must emit names only on alias fallback")
if vworld.get("removal_gate") != {
    "tracked_examples": "canonical-only",
    "private_operator_profiles": "canonical-only",
    "alias_warning_executions": 0,
}:
    fail("foundation-vworld-credentials removal gate must require completed migration evidence")
credentials = vworld.get("credentials")
if not isinstance(credentials, dict):
    fail("foundation-vworld-credentials.credentials must be an object")

credential_fields = {"api_key", "domain", "username", "password"}
if set(credentials) != credential_fields:
    fail("VWorld credential contract must declare api_key, domain, username, and password")
canonical_names: list[str] = []
deprecated_aliases: list[str] = []
environment_name = re.compile(r"^[A-Z][A-Z0-9_]*$")
for field in sorted(credential_fields):
    value = credentials.get(field)
    if not isinstance(value, dict):
        fail(f"VWorld credential contract is missing {field}")
    canonical = value.get("canonical")
    aliases = value.get("deprecated_aliases")
    expected_canonical = f"{namespaces['foundation-platform']}VWORLD_{field.upper()}"
    if canonical != expected_canonical:
        fail(f"VWorld canonical credential does not follow the owner namespace for {field}")
    if (
        not isinstance(aliases, list)
        or not aliases
        or not all(isinstance(alias, str) and environment_name.fullmatch(alias) for alias in aliases)
        or len(set(aliases)) != len(aliases)
        or canonical in aliases
    ):
        fail(f"VWorld deprecated aliases are invalid for {field}")
    if not environment_name.fullmatch(canonical):
        fail(f"VWorld canonical credential is not a valid environment name for {field}")
    if not isinstance(value.get("sensitive"), bool):
        fail(f"VWorld credential sensitivity must be boolean for {field}")
    canonical_names.append(canonical)
    deprecated_aliases.extend(aliases)

if len(set(canonical_names + deprecated_aliases)) != len(canonical_names + deprecated_aliases):
    fail("VWorld canonical names and deprecated aliases must be globally unique")

for relative in [
    "platforms/foundation-platform/.env.example",
    "platforms/foundation-platform/.env.local.example",
]:
    path = root / relative
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        fail(f"cannot read {relative}: {error}")
    assignments = [line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.lstrip().startswith("#")]
    for canonical in canonical_names:
        if assignments.count(canonical) != 1:
            fail(f"{relative} must declare {canonical} exactly once")
    for alias in deprecated_aliases:
        if alias in assignments:
            fail(f"{relative} must not declare deprecated alias {alias}")

adapter_paths = {
    "platforms/foundation-platform/services/foundation-outbox-publisher/src/vworld_credentials.rs",
    "platforms/foundation-platform/services/foundation-provider-acquisition-worker/src/foundation_provider_acquisition/vworld_credentials.py",
}
direct_name_allowed_paths = {
    "platforms/foundation-platform/config/environment-variable-naming.contract.json",
    "scripts/guard/environment-variable-naming-self-test.sh",
}
rust_adapter = "platforms/foundation-platform/services/foundation-outbox-publisher/src/vworld_credentials.rs"
python_adapter = "platforms/foundation-platform/services/foundation-provider-acquisition-worker/src/foundation_provider_acquisition/vworld_credentials.py"
for relative in adapter_paths:
    path = root / relative
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as error:
        fail(f"cannot read compatibility adapter {relative}: {error}")
    if relative == rust_adapter:
        rust_code = re.sub(r"/\*.*?\*/|//[^\n]*", "", content, flags=re.DOTALL)
        embedded = re.search(
            r'const\s+([A-Z][A-Z0-9_]*)\s*:\s*&str\s*=\s*include_str!\s*\(\s*"[^"]*environment-variable-naming\.contract\.json"\s*\)',
            rust_code,
        )
        if embedded is None or re.search(
            rf"serde_json::from_str\s*\(\s*{re.escape(embedded.group(1))}\s*\)",
            rust_code,
        ) is None:
            fail("Rust compatibility adapter must parse the embedded naming contract")
    elif relative == python_adapter:
        try:
            tree = ast.parse(content, filename=relative)
        except SyntaxError as error:
            fail(f"cannot parse Python compatibility adapter: {error}")
        constants = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        loads_json = any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "json"
            and node.func.attr == "load"
            for node in ast.walk(tree)
        )
        required_constants = {
            "environment-variable-naming.contract.json",
            "compatibility_migrations",
            "foundation-vworld-credentials",
            "credentials",
            "deprecated_aliases",
        }
        if not loads_json or not required_constants.issubset(constants):
            fail("Python compatibility adapter must parse and traverse the naming contract")

try:
    raw_paths = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "-z"], stderr=subprocess.STDOUT
    )
except subprocess.CalledProcessError as posix_error:
    if shutil.which("git.exe") is None or shutil.which("wslpath") is None:
        fail(f"git ls-files failed: {posix_error.output.decode(errors='replace')}")
    windows_root = subprocess.check_output(
        ["wslpath", "-m", str(root)], text=True
    ).strip()
    try:
        raw_paths = subprocess.check_output(
            ["git.exe", "-C", windows_root, "ls-files", "-z"], stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as windows_error:
        fail(
            "git ls-files failed with POSIX and Windows Git: "
            + windows_error.output.decode(errors="replace")
        )

source_suffixes = {".rs", ".py", ".sh", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".json", ".jsonc", ".yaml", ".yml", ".sql", ".toml"}
violations: list[str] = []
for raw_path in raw_paths.decode().split("\0"):
    if not raw_path or raw_path in direct_name_allowed_paths:
        continue
    normalized = raw_path.replace("\\", "/")
    if not (
        normalized.startswith("platforms/foundation-platform/")
        or normalized.startswith("scripts/")
        or normalized.startswith(".github/")
        or normalized.startswith("tools/")
    ):
        continue
    if "/test/" in normalized or "/tests/" in normalized or "/fixtures/" in normalized:
        continue
    if "/docs/" in normalized or normalized.startswith("docs/"):
        continue
    if normalized.endswith(".env.example") or normalized.endswith(".env.local.example"):
        continue
    if Path(normalized).suffix not in source_suffixes and not Path(normalized).name.startswith("Dockerfile"):
        continue
    try:
        content = (root / raw_path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        continue
    if normalized == rust_adapter:
        content = content.split("#[cfg(test)]", 1)[0]
    for name in [*canonical_names, *deprecated_aliases]:
        if name in content:
            violations.append(f"{normalized}: direct VWorld credential name {name}; use the compatibility adapter")

if violations:
    fail("direct VWorld credential names remain outside the adapters:\n" + "\n".join(violations))
PY
then
  echo 'OK environment-variable-naming'
else
  echo 'FAIL environment-variable-naming' >&2
  exit 1
fi
