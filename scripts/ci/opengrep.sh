#!/usr/bin/env bash
# Runs the checksum-pinned Opengrep release (tools/opengrep.env) with the given arguments, from the
# repository root. Opengrep is the LGPL Semgrep engine fork: the rule files under
# tools/semgrep/rules are ordinary Semgrep rules, and `--test` runs their annotated fixtures.
# The download, its checksum and the cache live behind the one pinned fetcher (root ADR-0197).
set -euo pipefail
cd "$(dirname "$0")/../.."

for command_name in mktemp python3 uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL opengrep: missing command '$command_name'" >&2
    exit 1
  }
done
case "$(uname -s)" in
  Linux|Darwin) executable_name=opengrep ;;
  MINGW*|MSYS*|CYGWIN*) executable_name=opengrep.exe ;;
  *) echo "FAIL opengrep: unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac

runtime_root="$(mktemp -d)"
trap 'rm -rf -- "$runtime_root"' EXIT
python3 scripts/ci/fetch_pinned.py tool opengrep --output "$runtime_root/$executable_name"
chmod +x "$runtime_root/$executable_name"
"$runtime_root/$executable_name" "$@"
