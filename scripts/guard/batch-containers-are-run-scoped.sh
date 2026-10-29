#!/usr/bin/env bash
# Every container a job starts is its own: no fixed name two jobs can both create.
#
# What real incident does failing this prevent? On 2026-10-29 at 07:41Z the by-PNU bake's
# parcel-forest-ledger lane failed, "the change set job exited 1; nothing was published". Its Spark
# run started compose.lakehouse.yml's write check (lakehouse-target-init) as a dependency, and that
# service had `container_name: foundation-platform-lakehouse-target-init`: one name for every job on
# the host. The deploy's lakehouse migration had created it two seconds earlier, so Docker refused
# the bake ("The container name ... is already in use"). Each job removed the name before starting
# (`docker rm ... || true`), which only moved the race. The by-PNU lanes now run as separate jobs at
# once (root ADR-0303, ADR-0333), so every new parallel job would have met it again.
#
# The rule, in three parts:
#   1. A compose service that runs on the host (tools/host-memory-budget.contract.json) may state
#      a container_name only if it stays up (`restart: unless-stopped` or `always`): one standing
#      container, started by `up`, which Compose converges on instead of creating twice. A batch
#      service gets the one-off name `compose run` gives it, <project>-<service>-run-<id>.
#   2. A host job script starts compose.lakehouse.yml only through admitted-writer-runtime.sh's
#      lakehouse_spark_run, which runs the write check and the job as `run --rm --no-deps`: no
#      dependency container, which `run` would create under the service's name.
#   3. A host job script never names a container itself: no `docker rm <fixed name>` and no
#      `docker run|create --name <fixed name>`. A name built from the run ($...) is its own.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
name="batch-containers-are-run-scoped"
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL ${name}: python3 is required" >&2
  exit 1
}

python3 - "$root" "$name" <<'PY'
import json
import pathlib
import re
import sys

root, name = pathlib.Path(sys.argv[1]), sys.argv[2]
ops = root / "platforms/foundation-platform/scripts/ops"
helper = ops / "admitted-writer-runtime.sh"
failures = []


def services(text):
    """Each top-level service's block, comments dropped."""
    body = re.search(r"(?ms)^services:\n(.*?)(?=^\S|\Z)", text)
    found = {}
    if body:
        for match in re.finditer(r"(?ms)^  ([A-Za-z0-9_.-]+):\n(.*?)(?=^  \S|\Z)", body.group(1)):
            found[match.group(1)] = "\n".join(
                line for line in match.group(2).splitlines() if not line.lstrip().startswith("#"))
    return found


# The compose files that run on the host, as the host memory budget places them.
budget = json.loads((root / "tools/host-memory-budget.contract.json").read_text(encoding="utf-8"))
for path in sorted({root / f for project in budget["projects"] for f in project["files"]}):
    for service, body in services(path.read_text(encoding="utf-8")).items():
        if re.search(r"(?m)^    container_name:", body) and not re.search(
                r"(?m)^    restart: \"?(unless-stopped|always)\"?$", body):
            failures.append(f"{path.relative_to(root)}: {service} has a container_name but does not stay up")

for path in sorted(ops.glob("*.sh")):
    lines = path.read_text(encoding="utf-8").splitlines()
    code = [(n, line) for n, line in enumerate(lines, 1) if not line.lstrip().startswith("#")]
    where = lambda n: f"{path.relative_to(root)}:{n}"
    for n, line in code:
        if path != helper and "compose.lakehouse.yml" in line:
            failures.append(f"{where(n)}: starts compose.lakehouse.yml itself; use lakehouse_spark_run")
        if re.search(r"\bdocker\s+(container\s+)?rm\s+(-f\s+)?[A-Za-z0-9]", line):
            failures.append(f"{where(n)}: removes a container by a fixed name")
        named = re.search(r"\bdocker\s+(run|create)\b.*--name[ =]([^\s\\]+)", line)
        if named and "$" not in named.group(2):
            failures.append(f"{where(n)}: starts a container under the fixed name {named.group(2)}")

text = helper.read_text(encoding="utf-8") if helper.is_file() else ""
run = re.search(r"(?ms)^lakehouse_spark_run\(\) \{\n(.*?)^\}", text)
if not run or "run --rm --no-deps" not in run.group(1) or "lakehouse-target-init" not in run.group(1):
    failures.append(f"{helper.relative_to(root)}: lakehouse_spark_run must run the check and the job as `run --rm --no-deps`")

if failures:
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    print(f"FAIL {name}: a container two jobs can both create by one name "
          "(2026-10-29, the by-PNU bake against the lakehouse migration)", file=sys.stderr)
    sys.exit(1)
print(f"OK {name}")
PY
