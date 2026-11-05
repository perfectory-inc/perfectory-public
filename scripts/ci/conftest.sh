#!/usr/bin/env bash
# Runs the checksum-pinned Conftest release (tools/conftest.env) with the given arguments, from the
# repository root. Conftest evaluates the Rego policies under tools/conftest/ against structured
# inputs; `conftest verify` runs their `_test.rego` unit tests. The download, its checksum and the
# cache live behind the one pinned fetcher (root ADR-0197).
set -euo pipefail
cd "$(dirname "$0")/../.."

for command_name in mktemp python3 uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL conftest: missing command '$command_name'" >&2
    exit 1
  }
done
case "$(uname -s)" in
  Linux|Darwin) archive_type=tar.gz; executable_name=conftest ;;
  MINGW*|MSYS*|CYGWIN*) archive_type=zip; executable_name=conftest.exe ;;
  *) echo "FAIL conftest: unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac

runtime_root="$(mktemp -d)"
trap 'rm -rf -- "$runtime_root"' EXIT
python3 scripts/ci/fetch_pinned.py tool conftest --member "$executable_name" \
  --output "$runtime_root/$executable_name"
"$runtime_root/$executable_name" "$@"
