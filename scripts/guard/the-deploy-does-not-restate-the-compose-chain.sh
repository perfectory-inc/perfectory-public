#!/usr/bin/env bash
# The deploy asks compose for an end state, not the chain compose already declares (2026-09-02:
# grants never ran, the catalog answered 500 for a day). The rule is the Rego in
# tools/conftest/compose/chain.rego, judged on docker-compose.yml as the pinned `docker compose
# config` normalizes it, with foundation-release.sh's non-comment lines as data.
set -euo pipefail
guard_root="$(cd "$(dirname "$0")/../.." && { pwd -W 2>/dev/null || pwd -P; })"
exec python3 - "${1:-$guard_root}" "$guard_root" <<'PY'
import pathlib
import re
import sys

root, guard_root = (pathlib.Path(arg).resolve() for arg in sys.argv[1:3])
sys.path.insert(0, str(guard_root / "scripts" / "ci"))
import compose_model  # noqa: E402

NAME = "the-deploy-does-not-restate-the-compose-chain"
try:
    document = compose_model.model([root / "platforms/foundation-platform/docker-compose.yml"], ["*"])
    release = (root / "platforms/foundation-platform/scripts/deploy/foundation-release.sh").read_text(
        encoding="utf-8")
    commands = [line for line in release.splitlines() if not re.match(r"\s*#", line)]
    failures = compose_model.judge(guard_root / "tools/conftest/compose", "compose_chain",
                                   compose_model.write_model("chain", document),
                                   {"context": {"commands": commands}})
except (OSError, compose_model.ComposeError) as error:
    failures = [str(error)]
for message in failures:
    print(f"FAIL {NAME}: {message}", file=sys.stderr)
if failures:
    sys.exit(1)
print(f"OK {NAME}")
PY
