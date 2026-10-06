#!/usr/bin/env bash
# The foundation units, their scripts and the runbooks agree with the one runtime-secrets contract
# (root ADR-0153, platforms/foundation-platform/config/runtime-secrets.contract.json).
#
# What real incident does failing this prevent? On 2026-10-05 the 필지고유번호변동연혁 unit's script
# required the lakehouse reader key and none of the unit's environment files held it; the first
# run wrote 20 Bronze objects before it found out. The day before, a measurement script lost a key
# its Gold read needed. Each unit's EnvironmentFile lines, each script's needs and each runbook's
# systemd-run were hand-kept lists. This refuses a unit whose EnvironmentFile lines are not the
# contract's, a script requirement no loaded file holds, and a hand-written EnvironmentFile in a
# runbook or script. The checker is scripts/deploy/runtime_secrets.py; this only calls it.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL runtime-secrets-contract: python3 is required" >&2
  exit 1
}
area="${root}/platforms/foundation-platform"
python3 "${area}/scripts/deploy/runtime_secrets.py" --area "${area}" check
