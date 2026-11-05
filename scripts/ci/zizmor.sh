#!/usr/bin/env bash
# Runs the checksum-pinned zizmor release (tools/zizmor.env) with the given arguments, from the
# repository root. The download, its checksum and the cache live behind the one pinned fetcher
# (root ADR-0197).
set -euo pipefail
cd "$(dirname "$0")/../.."

for command_name in mktemp python3 uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL zizmor: missing command '$command_name'" >&2
    exit 1
  }
done
case "$(uname -s)" in
  Linux|Darwin) executable_name=zizmor ;;
  MINGW*|MSYS*|CYGWIN*) executable_name=zizmor.exe ;;
  *) echo "FAIL zizmor: unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac

runtime_root="$(mktemp -d)"
trap 'rm -rf -- "$runtime_root"' EXIT
python3 scripts/ci/fetch_pinned.py tool zizmor --member "$executable_name" \
  --output "$runtime_root/$executable_name"
chmod +x "$runtime_root/$executable_name"
"$runtime_root/$executable_name" "$@"
