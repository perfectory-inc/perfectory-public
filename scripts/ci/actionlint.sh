#!/usr/bin/env bash
# Parses workflow YAML/schema with checksum-pinned upstream actionlint before
# repository-specific trust-boundary checks run.
set -euo pipefail
cd "$(dirname "$0")/../.."

workflow_dir="${1:-.github/workflows}"
for command_name in mktemp python3 realpath uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL actionlint: missing command '$command_name'" >&2
    exit 1
  }
done

kernel="$(uname -s)"
case "$kernel" in
  Linux|Darwin) archive_type=tar.gz; executable_name=actionlint ;;
  MINGW*|MSYS*|CYGWIN*) archive_type=zip; executable_name=actionlint.exe ;;
  *) echo "FAIL actionlint: unsupported operating system $kernel" >&2; exit 1 ;;
esac

runtime_root="$(mktemp -d)"
cleanup() {
  if [ -n "${runtime_root:-}" ] && [ -d "$runtime_root" ]; then
    rm -rf -- "$runtime_root"
  fi
}
trap cleanup EXIT
# The release, its checksum and the cache live behind the one pinned fetcher (root ADR-0197);
# the pin itself stays in tools/actionlint.env.
archive="$runtime_root/actionlint.$archive_type"
python3 scripts/ci/fetch_pinned.py tool actionlint --output "$archive"
if [ "$archive_type" = zip ]; then
  command -v unzip >/dev/null || { echo "FAIL actionlint: unzip is required" >&2; exit 1; }
  unzip -q "$archive" "$executable_name" -d "$runtime_root"
else
  command -v tar >/dev/null || { echo "FAIL actionlint: tar is required" >&2; exit 1; }
  tar -xzf "$archive" -C "$runtime_root" "$executable_name"
fi

workflow_dir="$(realpath "$workflow_dir")"
shopt -s nullglob
workflows=("$workflow_dir"/*.yml "$workflow_dir"/*.yaml)
if [ "${#workflows[@]}" -eq 0 ]; then
  echo "FAIL actionlint: no workflows found in $workflow_dir" >&2
  exit 1
fi
sanitized_env=(env -i "PATH=$PATH")
for system_name in SYSTEMROOT WINDIR COMSPEC TMP TEMP; do
  if [ -n "${!system_name:-}" ]; then
    sanitized_env+=("$system_name=${!system_name}")
  fi
done
"${sanitized_env[@]}" "$runtime_root/$executable_name" "${workflows[@]}"
echo "OK actionlint workflows=${#workflows[@]}"
