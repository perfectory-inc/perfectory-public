#!/usr/bin/env bash
# Proves provider UUID detection requires credential-assignment context and
# exceptions remain rule/path/value scoped.
set -euo pipefail
cd "$(dirname "$0")/../.."

python3 - <<'PY'
import re
import sys
import tomllib
from pathlib import Path

with Path(".gitleaks.toml").open("rb") as handle:
    policy = tomllib.load(handle)
rules = {rule.get("id"): rule for rule in policy.get("rules", [])}
provider = rules.get("vworld-api-key")
if not provider:
    print("FAIL gitleaks-policy-self-test: missing VWorld provider rule", file=sys.stderr)
    raise SystemExit(1)
pattern = re.compile(provider["regex"])
lower = "01234567-89ab-cdef-0123-456789abcdef"
upper = lower.upper()
provider_name = "_".join(("VWORLD", "API", "KEY"))
provider_property = "-".join(("vworld", "api", "key"))
positive = (
    provider_name + "=" + lower,
    '{"' + provider_property + '": "' + upper + '"}',
    '.env("' + provider_name + '", "' + lower + '")',
    "https://api." + "vworld.kr/req/data?key=" + lower,
)
for fixture in positive:
    match = pattern.search(fixture)
    if match is None or match.group(provider.get("secretGroup", 0)) not in (lower, upper):
        print(f"FAIL gitleaks-policy-self-test: missed credential context: {fixture}", file=sys.stderr)
        raise SystemExit(1)
negative = (
    lower,
    "let vworld_run_id = \"" + lower + "\";",
    "source=vworld/run_id=" + lower,
    "api_key = \"" + lower + "\"",
    provider_name + "=01234567-89ab-cdef-0123-456789abcdeg",
)
if any(pattern.search(fixture) for fixture in negative):
    print("FAIL gitleaks-policy-self-test: provider rule confuses ordinary UUIDs with credentials", file=sys.stderr)
    raise SystemExit(1)

if any("vworld-api-key" in allowlist.get("targetRules", [])
       for allowlist in policy.get("allowlists", [])):
    print("FAIL gitleaks-policy-self-test: provider rule must not need path allowlists", file=sys.stderr)
    raise SystemExit(1)

for allowlist in policy.get("allowlists", []):
    if allowlist.get("condition") != "AND" \
            or not allowlist.get("targetRules") \
            or not allowlist.get("paths") \
            or not allowlist.get("regexes") \
            or any(key in allowlist for key in ("commits", "stopwords")):
        print("FAIL gitleaks-policy-self-test: allowlists must bind rule AND path AND exact value", file=sys.stderr)
        raise SystemExit(1)
print("OK gitleaks-policy-self-test")
PY
