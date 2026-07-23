#!/usr/bin/env bash
# Applies only a private feature's tree delta to a clean public-clone branch.
# It never fetches, bundles, pushes, or imports private commit objects.
# Usage: import-private-feature-diff.sh <private-repo> <base-ref> <feature-ref> [public-clone]
set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <private-repo> <base-ref> <feature-ref> [public-clone]" >&2
  exit 2
fi
for command_name in awk bash git grep mktemp realpath sed tr wc; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL private-feature-import: missing command '$command_name'" >&2
    exit 1
  }
done
root="$(cd "$(dirname "$0")/../.." && pwd)"
git_transport="$root/scripts/github/safe-git-transport.sh"
canonical_remote_url="https://github.com/perfectory-inc/perfectory-public.git"
[ -x "$git_transport" ] || {
  echo "FAIL private-feature-import: safe Git transport is unavailable" >&2
  exit 1
}

private_root="$("$git_transport" --repository "$1" rev-parse --show-toplevel 2>/dev/null)" || {
  echo "FAIL private-feature-import: private source is not a Git worktree" >&2
  exit 1
}
private_root="$(realpath "$private_root")"
public_root="$("$git_transport" --repository "${4:-.}" rev-parse --show-toplevel 2>/dev/null)" || {
  echo "FAIL private-feature-import: destination is not a Git worktree" >&2
  exit 1
}
public_root="$(realpath "$public_root")"
if [ "$private_root" = "$public_root" ]; then
  echo "FAIL private-feature-import: private source and public destination must be different repositories" >&2
  exit 1
fi

base_commit="$("$git_transport" --repository "$private_root" rev-parse --verify "$2^{commit}" 2>/dev/null)" || {
  echo "FAIL private-feature-import: base ref is not a private commit" >&2
  exit 1
}
feature_commit="$("$git_transport" --repository "$private_root" rev-parse --verify "$3^{commit}" 2>/dev/null)" || {
  echo "FAIL private-feature-import: feature ref is not a private commit" >&2
  exit 1
}
if [ "$base_commit" = "$feature_commit" ]; then
  echo "FAIL private-feature-import: private feature has no commit range" >&2
  exit 1
fi

origin_url="$("$git_transport" --repository "$public_root" remote get-url origin 2>/dev/null || true)"
case "$origin_url" in
  https://github.com/perfectory-inc/perfectory-public|https://github.com/perfectory-inc/perfectory-public.git|git@github.com:perfectory-inc/perfectory-public.git|ssh://git@github.com/perfectory-inc/perfectory-public.git) ;;
  *)
    echo "FAIL private-feature-import: origin is not the canonical public repository" >&2
    exit 1
    ;;
esac
branch="$("$git_transport" --repository "$public_root" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
if [ -z "$branch" ] || [ "$branch" = main ]; then
  echo "FAIL private-feature-import: use a named feature branch based on public main" >&2
  exit 1
fi
if [ -n "$("$git_transport" --repository "$public_root" status --porcelain=v1 --untracked-files=all)" ]; then
  echo "FAIL private-feature-import: public destination must start clean" >&2
  exit 1
fi
if ! "$git_transport" --repository "$public_root" \
  show-ref --verify --quiet refs/remotes/origin/main; then
  echo "FAIL private-feature-import: public origin/main is missing" >&2
  exit 1
fi
public_head="$("$git_transport" --repository "$public_root" rev-parse HEAD)"
public_main="$("$git_transport" --repository "$public_root" rev-parse refs/remotes/origin/main)"
live_ref="$("$git_transport" --no-repository \
  ls-remote "$canonical_remote_url" refs/heads/main)" || {
  echo "FAIL private-feature-import: canonical public main is unreadable" >&2
  exit 1
}
if [ "$(printf '%s\n' "$live_ref" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 1 ] \
  || ! printf '%s\n' "$live_ref" | grep -Eq '^[0-9a-f]{40}[[:space:]]+refs/heads/main$'; then
  echo "FAIL private-feature-import: canonical public main did not resolve exactly once" >&2
  exit 1
fi
live_main="$(printf '%s\n' "$live_ref" | awk '$2 == "refs/heads/main" { print $1 }')"
if [ "$public_main" != "$live_main" ]; then
  echo "FAIL private-feature-import: local origin/main does not match live canonical main" >&2
  exit 1
fi
if [ "$public_head" != "$live_main" ]; then
  echo "FAIL private-feature-import: feature branch must start exactly at public origin/main with no extra ancestry" >&2
  exit 1
fi
alternates="$("$git_transport" --repository "$public_root" rev-parse --git-path objects/info/alternates)"
if [ -s "$alternates" ]; then
  echo "FAIL private-feature-import: public clone must not use object alternates" >&2
  exit 1
fi
for private_commit in "$base_commit" "$feature_commit"; do
  if "$git_transport" --repository "$public_root" \
    cat-file -e "$private_commit^{commit}" 2>/dev/null; then
    echo "FAIL private-feature-import: a private commit object is already reachable locally" >&2
    exit 1
  fi
done

import_temp="$(mktemp -d)"
patch_file="$import_temp/feature.patch"
temporary_index="$import_temp/public.index"
cleanup() {
  if [ -n "${import_temp:-}" ] && [ -d "$import_temp" ]; then
    rm -rf -- "$import_temp"
  fi
}
trap cleanup EXIT

"$git_transport" --repository "$private_root" diff --binary --full-index \
  --no-ext-diff --no-textconv \
  "$base_commit" "$feature_commit" -- >"$patch_file"
if [ ! -s "$patch_file" ]; then
  echo "FAIL private-feature-import: private feature has no tree delta" >&2
  exit 1
fi

index_git=("$git_transport" --trusted-index-file "$temporary_index" --repository "$public_root")
"${index_git[@]}" read-tree HEAD
"${index_git[@]}" update-index --refresh
"${index_git[@]}" apply --check --index --whitespace=error-all "$patch_file"
"${index_git[@]}" apply --index --whitespace=error-all "$patch_file"

if "${index_git[@]}" ls-files --stage \
  | grep -Eq '^(120000|160000) '; then
  echo "FAIL private-feature-import: imported delta contains a symlink or gitlink" >&2
  exit 1
fi
for private_commit in "$base_commit" "$feature_commit"; do
  if "$git_transport" --repository "$public_root" \
    cat-file -e "$private_commit^{commit}" 2>/dev/null; then
    echo "FAIL private-feature-import: private history entered the public object database" >&2
    exit 1
  fi
done

"${index_git[@]}" status --porcelain=v1 --untracked-files=all >/dev/null
audit_env=(
  env -i
  "PATH=$PATH"
  GIT_CONFIG_NOSYSTEM=1
  GIT_CONFIG_GLOBAL=/dev/null
  GIT_NO_REPLACE_OBJECTS=1
  "GIT_INDEX_FILE=$temporary_index"
  "PERFECTORY_TRUSTED_GIT_INDEX_FILE=$temporary_index"
)
for system_name in SYSTEMROOT WINDIR COMSPEC PATHEXT TMP TEMP USERPROFILE; do
  if [ -n "${!system_name:-}" ]; then
    audit_env+=("$system_name=${!system_name}")
  fi
done
"${audit_env[@]}" bash -ceu '
  cd "$1"
  bash scripts/guard/monorepo-guard.sh
  bash scripts/ci/gitleaks-scan.sh tree .
' _ "$public_root"
imported_tree="$("${index_git[@]}" write-tree)"

echo "OK private-feature-import tree=$imported_tree"
echo "Changes are left unstaged in the real public-clone index for review; this script never commits or pushes."
