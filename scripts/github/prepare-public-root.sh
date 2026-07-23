#!/usr/bin/env bash
# Audits one clean source commit in an isolated worktree, then builds a
# history-free bare repository for the same-process publisher.
# Usage: prepare-public-root.sh <source-repository> <source-commit> <new-bare-repository.git>
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
git_transport="$root/scripts/github/safe-git-transport.sh"
control_legal_validator="$root/scripts/github/validate-legal-publication.sh"
control_repository_identity_validator="$root/scripts/github/validate-public-repository-identity.sh"

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <source-repository> <source-commit> <new-bare-repository.git>" >&2
  exit 2
fi
for command_name in git grep mktemp python3 realpath; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-root-prepare: missing command '$command_name'" >&2
    exit 1
  }
done
bash "$control_repository_identity_validator"
bash "$control_legal_validator"

source_root="$("$git_transport" --repository "$1" rev-parse --show-toplevel 2>/dev/null)" || {
  echo "FAIL public-root-prepare: source is not a Git worktree" >&2
  exit 1
}
source_root="$(realpath "$source_root")"
candidate_legal_validator="$source_root/scripts/github/validate-legal-publication.sh"
candidate_repository_identity_validator="$source_root/scripts/github/validate-public-repository-identity.sh"
source_commit="$("$git_transport" --repository "$source_root" rev-parse --verify "$2^{commit}" 2>/dev/null)" || {
  echo "FAIL public-root-prepare: source ref is not a commit" >&2
  exit 1
}
source_head="$("$git_transport" --repository "$source_root" rev-parse HEAD)"
source_index_flags="$("$git_transport" --repository "$source_root" ls-files -v)"
if printf '%s\n' "$source_index_flags" | grep -Eq '^[hSs] '; then
  echo "FAIL public-root-prepare: source index contains skip-worktree or assume-unchanged entries" >&2
  printf '%s\n' "$source_index_flags" | grep -E '^[hSs] ' >&2
  exit 1
fi
destination="$(realpath -m "$3")"

if [ "$source_commit" != "$source_head" ]; then
  echo "FAIL public-root-prepare: audit only the checked-out HEAD commit" >&2
  exit 1
fi
source_status="$("$git_transport" --repository "$source_root" status --porcelain=v1 --untracked-files=all)"
if [ -n "$source_status" ]; then
  echo "FAIL public-root-prepare: source worktree must be clean before the exact commit is audited" >&2
  printf '%s\n' "$source_status" >&2
  exit 1
fi
if "$git_transport" --repository "$source_root" ls-tree -r "$source_commit" \
  | grep -Eq '^(120000 blob|160000 commit) '; then
  echo "FAIL public-root-prepare: public snapshot must not contain symlinks or gitlinks" >&2
  exit 1
fi
source_tree_hash="$("$git_transport" --repository "$source_root" \
  rev-parse "$source_commit^{tree}")"
bash "$candidate_repository_identity_validator"
bash "$candidate_legal_validator"

audit_root="$(mktemp -d)"
public_clone="$audit_root/public-root-clone"
cleanup() {
  local exit_code=$?
  if [ -n "${audit_root:-}" ] && [ -d "$audit_root" ]; then
    rm -rf -- "$audit_root"
  fi
  trap - EXIT
  exit "$exit_code"
}
trap cleanup EXIT

"$root/scripts/github/build-public-root.sh" \
  "$source_root" "$source_commit" "$destination"
"$git_transport" --no-repository clone --quiet --no-local \
  "$destination" "$public_clone"
clone_tree="$("$git_transport" --repository "$public_clone" rev-parse 'HEAD^{tree}')"
clone_count="$("$git_transport" --repository "$public_clone" rev-list --count --all)"
clone_parent_count="$("$git_transport" --repository "$public_clone" cat-file -p HEAD | grep -c '^parent ' || true)"
if [ "$clone_tree" != "$source_tree_hash" ] \
  || [ "$clone_count" -ne 1 ] \
  || [ "$clone_parent_count" -ne 0 ]; then
  echo "FAIL public-root-prepare: built root no longer matches the source tree" >&2
  exit 1
fi

audit_env=(
  env -i
  "PATH=$PATH"
  LANG=C.UTF-8
  LC_ALL=C.UTF-8
  CI=true
  GIT_CONFIG_NOSYSTEM=1
  GIT_CONFIG_GLOBAL=/dev/null
  GIT_NO_REPLACE_OBJECTS=1
)
for system_name in SYSTEMROOT WINDIR COMSPEC PATHEXT TMP TEMP USERPROFILE \
  SystemDrive ProgramData; do
  if [ -n "${!system_name:-}" ]; then
    audit_env+=("$system_name=${!system_name}")
  fi
done
"${audit_env[@]}" bash -ceu '
  cd "$1"
  bash scripts/github/validate-public-repository-identity.sh
  # This clone is the committed candidate, so its strict result is
  # authoritative over defense-in-depth checks of the source worktree.
  bash scripts/github/validate-legal-publication.sh
  # Run the repository-wide guard once against the real, one-commit clone. The
  # Docker verifier repeats it using a history-free index+blob Git directory.
  bash scripts/guard/monorepo-guard.sh
  CI=true bash scripts/ci/reuse-lint.sh
  CI=true bash scripts/ci/lychee-docs.sh
  bash scripts/ci/gitleaks-scan.sh tree .
  PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh products/gongzzang
  PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh platforms/foundation-platform
  PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh platforms/identity-platform
  PERFECTORY_CLEAN_VERIFY=1 bash scripts/verify/cargo-verify.sh platforms/intelligence-platform
  bash scripts/verify/frontend-test.sh
  bash scripts/ci/gitleaks-scan.sh all .
' _ "$public_clone"
post_verify_status="$("$git_transport" --repository "$public_clone" status --porcelain=v1 --untracked-files=all)"
if [ -n "$post_verify_status" ]; then
  echo "FAIL public-root-prepare: verification mutated the public-root checkout" >&2
  printf '%s\n' "$post_verify_status" >&2
  exit 1
fi

echo "OK public-root-prepare source=$source_commit tree=$source_tree_hash destination=$destination"
