#!/usr/bin/env bash
# Repository contracts for the GitHub Actions workflows (root ADR-0422): actionlint checks the YAML
# and the Actions schema, then the Rego policies in tools/conftest/workflow judge every workflow as
# Conftest's YAML parser reads it — permission shapes, triggers, the terminal required/* job, area
# working directories, contexts against the main ruleset, and third-party Actions against the
# audited allowlist. General Actions security is zizmor's (scripts/guard/zizmor-workflows.sh).
set -euo pipefail

workflow_dir="${1:-.github/workflows}"
ruleset="${2:-tools/github/main-ruleset.json}"
selected_actions="${3:-tools/github/selected-actions.json}"
self_test_mode="${4:-}"
repository_root="$(cd "$(dirname "$0")/../.." && pwd)"

for command_name in mktemp python3 realpath; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL workflow-policy: missing command '$command_name'" >&2
    exit 1
  }
done
if [ ! -d "$workflow_dir" ] || [ ! -f "$ruleset" ] || [ ! -f "$selected_actions" ]; then
  echo "FAIL workflow-policy: workflow directory or policy file is missing" >&2
  exit 1
fi

# The self-test parses its one valid fixture through actionlint once, then exercises the policy
# without launching actionlint for every fixture. That mode is admitted only for fixture and policy
# paths sharing one external temporary root.
if [ -n "$self_test_mode" ]; then
  [ "$self_test_mode" = --semantic-self-test ] || {
    echo "FAIL workflow-policy: unknown fourth argument" >&2
    exit 2
  }
  fixture_root="$(dirname "$(realpath "$ruleset")")"
  workflow_real="$(realpath "$workflow_dir")"
  actions_root="$(dirname "$(realpath "$selected_actions")")"
  case "$workflow_real" in "$fixture_root"/*) ;; *) exit 2 ;; esac
  if [ "$actions_root" != "$fixture_root" ] || [ "$fixture_root" = "$repository_root" ]; then
    echo "FAIL workflow-policy: semantic self-test paths are not isolated fixtures" >&2
    exit 2
  fi
else
  bash "$repository_root/scripts/ci/actionlint.sh" "$workflow_dir" >/dev/null
fi

# Conftest is a native binary: on Windows it needs native paths, not the shell's /c/... form.
native() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}

shopt -s nullglob
workflows=()
for workflow in "$workflow_dir"/*.yml "$workflow_dir"/*.yaml; do
  workflows+=("$(native "$(realpath "$workflow")")")
done
if [ "${#workflows[@]}" -eq 0 ]; then
  echo "FAIL workflow-policy: no workflow files found in $workflow_dir" >&2
  exit 1
fi

# Conftest loads a data file by its extension, so the context is a `.json` file.
context_dir="$(mktemp -d)"
trap 'rm -rf -- "$context_dir"' EXIT
context="$context_dir/context.json"
python3 - "$repository_root/tools/areas.contract.json" "$ruleset" "$selected_actions" >"$context" <<'PY'
import json
import re
import sys

areas, ruleset, selected = sys.argv[1:4]


def contexts(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "context" and isinstance(value, str) and value.startswith("required/"):
                yield value
            else:
                yield from contexts(value)
    elif isinstance(node, list):
        for item in node:
            yield from contexts(item)


with open(areas, encoding="utf-8") as handle:
    registry = json.load(handle)
area_dirs = sorted({area["dir"] for area in registry.get("areas", []) if area.get("dir")})
with open(ruleset, encoding="utf-8") as handle:
    required = list(contexts(json.load(handle)))
with open(selected, encoding="utf-8") as handle:
    actions = sorted(set(re.findall(r'"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40})"', handle.read())))
sys.stdout.reconfigure(newline="\n")
json.dump({"context": {"area_dirs": area_dirs, "required_contexts": required,
                       "selected_actions": actions}}, sys.stdout)
PY
if ! python3 -c 'import json, sys; sys.exit(0 if json.load(open(sys.argv[1]))["context"]["area_dirs"] else 1)' "$context"; then
  echo "FAIL workflow-policy: tools/areas.contract.json lists no area dirs" >&2
  exit 1
fi

if ! bash "$repository_root/scripts/ci/conftest.sh" test --combine --no-color \
  --policy tools/conftest/workflow --namespace workflow --data "$(native "$context")" \
  "${workflows[@]}" >&2; then
  echo "FAIL workflow-policy: a workflow breaks a repository contract (see the failures above)" >&2
  exit 1
fi
echo "OK workflow-policy"
