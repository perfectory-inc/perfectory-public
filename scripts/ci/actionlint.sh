#!/usr/bin/env bash
# Parses workflow YAML/schema with checksum-pinned upstream actionlint before
# repository-specific trust-boundary checks run.
set -euo pipefail
cd "$(dirname "$0")/../.."

workflow_dir="${1:-.github/workflows}"
for command_name in curl mktemp realpath sha256sum uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL actionlint: missing command '$command_name'" >&2
    exit 1
  }
done
policy=tools/actionlint.env
[ -f "$policy" ] || { echo "FAIL actionlint: missing $policy" >&2; exit 1; }
# shellcheck disable=SC1090 -- versioned scalar/checksum policy.
source "$policy"

kernel="$(uname -s)"
machine="$(uname -m)"
case "$machine" in
  x86_64|amd64) architecture=amd64 ;;
  arm64|aarch64) architecture=arm64 ;;
  *) echo "FAIL actionlint: unsupported architecture $machine" >&2; exit 1 ;;
esac
case "$kernel" in
  Linux) platform=linux; archive_type=tar.gz; executable_name=actionlint ;;
  Darwin) platform=darwin; archive_type=tar.gz; executable_name=actionlint ;;
  MINGW*|MSYS*|CYGWIN*) platform=windows; archive_type=zip; executable_name=actionlint.exe ;;
  *) echo "FAIL actionlint: unsupported operating system $kernel" >&2; exit 1 ;;
esac
checksum_name="ACTIONLINT_$(printf '%s_%s' "$platform" "$architecture" | tr '[:lower:]' '[:upper:]')_SHA256"
expected_sha="${!checksum_name:-}"
if ! printf '%s\n' "$ACTIONLINT_VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' \
  || ! printf '%s\n' "$expected_sha" | grep -Eq '^[0-9a-f]{64}$'; then
  echo "FAIL actionlint: malformed release/checksum policy" >&2
  exit 1
fi

archive_name="actionlint_${ACTIONLINT_VERSION}_${platform}_${architecture}.${archive_type}"
cache_root="${TMPDIR:-/tmp}/perfectory-actionlint-cache"
archive="$cache_root/$archive_name"
mkdir -p "$cache_root"
if [ ! -f "$archive" ] \
  || ! printf '%s  %s\n' "$expected_sha" "$archive" | sha256sum --check --strict - >/dev/null 2>&1; then
  partial="$archive.partial.$$"
  rm -f -- "$partial"
  curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
    --output "$partial" \
    "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/${archive_name}"
  printf '%s  %s\n' "$expected_sha" "$partial" | sha256sum --check --strict -
  mv "$partial" "$archive"
fi
printf '%s  %s\n' "$expected_sha" "$archive" | sha256sum --check --strict - >/dev/null

runtime_root="$(mktemp -d)"
cleanup() {
  if [ -n "${runtime_root:-}" ] && [ -d "$runtime_root" ]; then
    rm -rf -- "$runtime_root"
  fi
}
trap cleanup EXIT
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
