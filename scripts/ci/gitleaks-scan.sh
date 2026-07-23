#!/usr/bin/env bash
# Runs the repository's secret scan with one checksum-pinned gitleaks binary.
# Usage: gitleaks-scan.sh <tree|all> [repository]
set -euo pipefail

mode="${1:?usage: $0 <tree|all> [repository]}"
target="${2:-.}"
case "$mode" in
  tree|all) ;;
  *) echo "FAIL gitleaks-scan: mode must be tree or all" >&2; exit 2 ;;
esac

for command_name in curl git mktemp realpath sha256sum tar uname; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL gitleaks-scan: missing command '$command_name'" >&2
    exit 1
  }
done

root="$(cd "$(dirname "$0")/../.." && pwd)"
policy="$root/tools/gitleaks.env"
config="$root/.gitleaks.toml"
if [ ! -f "$policy" ] || [ ! -f "$config" ]; then
  echo "FAIL gitleaks-scan: pinned tool policy or root config is missing" >&2
  exit 1
fi
# shellcheck disable=SC1090 -- policy is a versioned two-scalar shell contract.
source "$policy"
if ! printf '%s\n' "$GITLEAKS_VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "FAIL gitleaks-scan: malformed pinned release policy" >&2
  exit 1
fi
for checksum_name in \
  GITLEAKS_LINUX_X64_SHA256 GITLEAKS_LINUX_ARM64_SHA256 \
  GITLEAKS_DARWIN_X64_SHA256 GITLEAKS_DARWIN_ARM64_SHA256 \
  GITLEAKS_WINDOWS_X64_SHA256 GITLEAKS_WINDOWS_ARM64_SHA256; do
  if ! printf '%s\n' "${!checksum_name:-}" | grep -Eq '^[0-9a-f]{64}$'; then
    echo "FAIL gitleaks-scan: malformed checksum policy for $checksum_name" >&2
    exit 1
  fi
done

kernel="$(uname -s)"
machine="$(uname -m)"
case "$machine" in
  x86_64|amd64) architecture=x64 ;;
  arm64|aarch64) architecture=arm64 ;;
  *) echo "FAIL gitleaks-scan: unsupported architecture $machine" >&2; exit 1 ;;
esac
case "$kernel" in
  Linux) platform=linux; archive_type=tar.gz; executable_name=gitleaks ;;
  Darwin) platform=darwin; archive_type=tar.gz; executable_name=gitleaks ;;
  MINGW*|MSYS*|CYGWIN*) platform=windows; archive_type=zip; executable_name=gitleaks.exe ;;
  *) echo "FAIL gitleaks-scan: unsupported operating system $kernel" >&2; exit 1 ;;
esac
checksum_name="GITLEAKS_$(printf '%s_%s' "$platform" "$architecture" | tr '[:lower:]' '[:upper:]')_SHA256"
expected_sha="${!checksum_name:-}"
if ! printf '%s\n' "$expected_sha" | grep -Eq '^[0-9a-f]{64}$'; then
  echo "FAIL gitleaks-scan: missing checksum for ${platform}_${architecture}" >&2
  exit 1
fi

target="$(realpath "$target")"
git -C "$target" rev-parse --git-dir >/dev/null 2>&1 || {
  echo "FAIL gitleaks-scan: target is not a Git repository: $target" >&2
  exit 1
}

scan_temp="$(mktemp -d)"
cleanup() {
  if [ -n "${scan_temp:-}" ] && [ -d "$scan_temp" ]; then
    rm -rf -- "$scan_temp"
  fi
}
trap cleanup EXIT

tracked_snapshot="$scan_temp/tracked-tree"
bash "$root/scripts/ci/materialize-public-candidate-tree.sh" \
  "$target" "$tracked_snapshot" >/dev/null

archive_name="gitleaks_${GITLEAKS_VERSION}_${platform}_${architecture}.${archive_type}"
archive="$scan_temp/$archive_name"
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
  --output "$archive" \
  "https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/${archive_name}"
printf '%s  %s\n' "$expected_sha" "$archive" \
  | sha256sum --check --strict -
if [ "$archive_type" = zip ]; then
  command -v unzip >/dev/null || {
    echo "FAIL gitleaks-scan: unzip is required on Windows" >&2
    exit 1
  }
  unzip -q "$archive" "$executable_name" -d "$scan_temp"
else
  command -v tar >/dev/null || {
    echo "FAIL gitleaks-scan: tar is required on $platform" >&2
    exit 1
  }
  tar -xzf "$archive" -C "$scan_temp" "$executable_name"
fi

sanitized_env=(
  env -i
  "PATH=$PATH"
  GIT_CONFIG_NOSYSTEM=1
  GIT_CONFIG_GLOBAL=/dev/null
)
for system_name in SYSTEMROOT WINDIR COMSPEC TMP TEMP; do
  if [ -n "${!system_name:-}" ]; then
    sanitized_env+=("$system_name=${!system_name}")
  fi
done

(
  cd "$tracked_snapshot"
  "${sanitized_env[@]}" "$scan_temp/$executable_name" \
    dir . --config "$config" --redact -v
)
if [ "$mode" = all ]; then
  (
    cd "$target"
    "${sanitized_env[@]}" "$scan_temp/$executable_name" \
      git . --config "$config" --redact -v
  )
fi

echo "OK gitleaks-scan mode=$mode"
