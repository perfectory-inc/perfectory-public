#!/usr/bin/env bash
# The identity endpoint constants are decided once, in the identity platform's
# endpoint contract; every carried copy must match it.
#
# What real incident does failing this prevent? On 2026-09-04 the first identity
# bring-up wired the issuer port by hand into four files, and the same night a
# sidecar that was not where its consumer looked turned every policy call into a
# 500 behind a healthy readyz — this guard fails the moment any carried copy of
# those constants drifts from the contract, before a deploy can strand a
# listener where nothing answers.
set -uo pipefail

root="${1:-$(git rev-parse --show-toplevel)}"
name="identity-endpoints-match-the-contract"

python3 - "${root}" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
name = "identity-endpoints-match-the-contract"
failures = []


def read(relative):
    path = root / relative
    if not path.is_file():
        failures.append(f"missing {relative}")
        return None
    return path.read_text(encoding="utf-8")


def code_lines(text):
    return [line for line in text.split("\n") if not line.lstrip().startswith("#")]


contract_text = read("platforms/identity-platform/config/identity-runtime-endpoints.contract.json")
if contract_text is None:
    print(f"FAIL {name}: the contract itself is missing", file=sys.stderr)
    sys.exit(1)
contract = json.loads(contract_text)
issuer_port = str(contract["issuer"]["loopback_port"])
issuer_alias = contract["issuer"]["network_alias"]
api_port = str(contract["identity_api"]["loopback_port"])
api_container_port = str(contract["identity_api"]["container_port"])
api_alias = contract["identity_api"]["network_alias"]

zitadel_compose = read("platforms/identity-platform/infra/zitadel/docker-compose.yml")
if zitadel_compose is not None:
    lines = code_lines(zitadel_compose)
    body = "\n".join(lines)
    for key in ("ZITADEL_PORT:", "ZITADEL_EXTERNALPORT:"):
        if not re.search(rf"{key} \$\{{IDENTITY_ZITADEL_LOOPBACK_PORT", body):
            failures.append(f"zitadel compose does not derive {key.rstrip(':')} from the contract variable")
    # The runtime policy forces literal numbers into the published-port entry;
    # they are allowed there and only there, and must equal the contract.
    ports_entry = f'- "127.0.0.1:${{IDENTITY_ZITADEL_LOOPBACK_PORT:-{issuer_port}}}:{issuer_port}"'
    for line in lines:
        if issuer_port in line and line.strip() != ports_entry:
            failures.append(
                f"zitadel compose carries the issuer port outside the guarded ports entry: {line.strip()}"
            )
    if not any(line.strip() == ports_entry for line in lines):
        failures.append("zitadel compose ports entry does not match the contract-guarded form")
    if not re.search(rf"^\s+- {issuer_alias}$", body, re.M):
        failures.append(f"zitadel compose does not alias the issuer as '{issuer_alias}'")

overlay = read("platforms/identity-platform/compose.server.yml")
if overlay is not None:
    body = "\n".join(code_lines(overlay))
    if re.search(rf"\b{issuer_port}\b", body):
        failures.append("identity server overlay carries the issuer port as a literal")
    if "TCP-LISTEN:${IDENTITY_ZITADEL_LOOPBACK_PORT" not in body:
        failures.append("identity server overlay sidecar does not listen on the contract variable")
    if f"TCP:{issuer_alias}:${{IDENTITY_ZITADEL_LOOPBACK_PORT" not in body:
        failures.append(f"identity server overlay sidecar does not target '{issuer_alias}' on the contract variable")

bridge = read("platforms/foundation-platform/compose.identity-bridge.yml")
if bridge is not None:
    body = "\n".join(code_lines(bridge))
    expected_defaults = {
        "FOUNDATION_PLATFORM_IDENTITY_ISSUER_LOOPBACK_PORT": issuer_port,
        "FOUNDATION_PLATFORM_IDENTITY_API_LOOPBACK_PORT": api_port,
        "FOUNDATION_PLATFORM_IDENTITY_API_CONTAINER_PORT": api_container_port,
    }
    for variable, expected in expected_defaults.items():
        defaults = set(re.findall(rf"\$\{{{variable}:-(\d+)\}}", body))
        if not defaults:
            failures.append(f"bridge overlay never uses {variable} with a guarded default")
        elif defaults != {expected}:
            failures.append(
                f"bridge overlay default for {variable} is {sorted(defaults)}, contract says {expected}"
            )
    if f"TCP:{issuer_alias}:" not in body:
        failures.append(f"bridge overlay does not target the issuer alias '{issuer_alias}'")
    if f"TCP:{api_alias}:" not in body:
        failures.append(f"bridge overlay does not target the identity api alias '{api_alias}'")

for wrapper in (
    "platforms/identity-platform/scripts/deploy/zitadel-runtime.sh",
    "platforms/identity-platform/scripts/deploy/identity-runtime.sh",
):
    text = read(wrapper)
    if text is not None and "identity-runtime-endpoints.contract.json" not in text:
        failures.append(f"{wrapper} does not derive its ports from the contract")

foundation_wrapper = read("platforms/foundation-platform/scripts/deploy/foundation-runtime.sh")
if foundation_wrapper is not None:
    body = "\n".join(code_lines(foundation_wrapper))
    for key in ("ZITADEL_ISSUER_URL", "IDENTITY_API_BASE_URL"):
        if key not in body:
            failures.append(f"foundation-runtime.sh does not derive the bridge port from {key}")

if failures:
    for failure in failures:
        print(f"FAIL {name}: {failure}", file=sys.stderr)
    sys.exit(1)
print(f"OK {name} (issuer={issuer_port}, api={api_port})")
PY
