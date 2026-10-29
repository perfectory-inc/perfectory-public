#!/usr/bin/env bash
# Prevents: a published document's version string breaking its consumers one by one. 2026-10-27..29
# the public auction document went from v1 to v2 (an optional media index per round, #877) and four
# consumers broke, each on its own copy of the string: the hourly serving monitor, the manifest
# record a whole generation copied (#929), the web preview's z.enum, and the Worker deploy whose
# smoke is that monitor. The schema registry (platforms/foundation-platform/contracts/schema-registry,
# root ADR-0336) is the one place a subject's versions live. This guard holds each subject's
# revisions to FULL_TRANSITIVE compatibility within a major, each consumer to its pact (every
# revision guarantees the paths it requires), merged revisions and their files to append-only
# against the pull request base (CHANGE_SCOPE_BASE), and refuses a registered subject's versioned
# string spelled by hand outside the registry. The rules live in the Python checker; this wrapper
# only finds Python.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"
checker="$root/platforms/foundation-platform/scripts/schema_registry.py"
args=(check --platform "$root/platforms/foundation-platform")
base="${CHANGE_SCOPE_BASE:-}"
if [ -n "$base" ] && [ -n "${base//0/}" ]; then
  args+=(--base "$base")
fi
if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "${args[@]}"
fi
exec python "$checker" "${args[@]}"
