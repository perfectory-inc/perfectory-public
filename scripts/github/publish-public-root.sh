#!/usr/bin/env bash
# Audits and deterministically rebuilds one history-free root in a private
# temporary directory, then publishes or safely resumes that exact root.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <source-repository> <source-commit>" >&2
  exit 2
fi

for command_name in git gh python3 realpath mktemp; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-root-publisher: missing command '$command_name'" >&2
    exit 1
  }
done

canonical_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -m "$1"
  else
    realpath "$1"
  fi
}

root="$(canonical_path "$(cd "$(dirname "$0")/../.." && pwd)")"
target="perfectory-inc/perfectory-public"
remote_url="https://github.com/${target}.git"
prepare="$root/scripts/github/prepare-public-root.sh"
configurator="$root/scripts/github/configure-public-repository.sh"
publication_authority="$root/scripts/github/check-publication-authority.sh"
git_transport="$root/scripts/github/safe-git-transport.sh"
identity_policy="$root/tools/github/public-root-identity.json"

control_root="$("$git_transport" --repository "$root" rev-parse --show-toplevel 2>/dev/null)" || {
  echo "FAIL public-root-publisher: publisher is not running from a Git worktree" >&2
  exit 1
}
source_root="$("$git_transport" --repository "$1" rev-parse --show-toplevel 2>/dev/null)" || {
  echo "FAIL public-root-publisher: source is not a Git worktree" >&2
  exit 1
}
control_root="$(canonical_path "$control_root")"
source_root="$(canonical_path "$source_root")"
if [ "$source_root" != "$control_root" ] || [ "$control_root" != "$root" ]; then
  echo "FAIL public-root-publisher: source, control worktree, and publisher root must be identical" >&2
  exit 1
fi

publication_root="$(mktemp -d)"
snapshot="$publication_root/public-root.git"
verification_clone="$publication_root/verification-clone"
publication_started=0
main_locked=0
cleanup() {
  local exit_code=$?
  if [ "$publication_started" -eq 1 ] \
    && [ "$main_locked" -eq 0 ] \
    && [ "$exit_code" -ne 0 ]; then
    echo "WARN public-root-publisher: publication started; retrying immediate main lock" >&2
    "$configurator" lock || \
      echo "CRITICAL public-root-publisher: main was published but could not be locked" >&2
  elif [ "$publication_started" -eq 1 ] && [ "$exit_code" -ne 0 ]; then
    echo "WARN public-root-publisher: publication failed after the main lock was verified" >&2
  fi
  if [ -n "${publication_root:-}" ] && [ -d "$publication_root" ]; then
    rm -rf -- "$publication_root"
  fi
  trap - EXIT
  exit "$exit_code"
}
trap cleanup EXIT

# Preparation and every expensive verification gate run inside this process.
# A caller cannot substitute a previously prepared bare repository or forge a
# local config marker to authorize publication.
"$prepare" "$source_root" "$2" "$snapshot"

source_commit="$("$git_transport" --repository "$source_root" rev-parse --verify "$2^{commit}" 2>/dev/null)" || {
  echo "FAIL public-root-publisher: source commit disappeared after preparation" >&2
  exit 1
}
source_head="$("$git_transport" --repository "$source_root" rev-parse HEAD)"
source_tree="$("$git_transport" --repository "$source_root" rev-parse "$source_commit^{tree}")"
source_status="$("$git_transport" --repository "$source_root" status --porcelain=v1 --untracked-files=all)"

if [ "$source_commit" != "$source_head" ] || [ -n "$source_status" ]; then
  echo "FAIL public-root-publisher: prepared source must remain at the same clean HEAD" >&2
  [ -z "$source_status" ] || printf '%s\n' "$source_status" >&2
  exit 1
fi
if [ ! -d "$snapshot" ] \
  || [ "$("$git_transport" --repository "$snapshot" rev-parse --is-bare-repository 2>/dev/null)" != true ]; then
  echo "FAIL public-root-publisher: preparation did not create a bare snapshot" >&2
  exit 1
fi

snapshot_commit="$("$git_transport" --repository "$snapshot" rev-parse refs/heads/main)"
snapshot_tree="$("$git_transport" --repository "$snapshot" rev-parse 'refs/heads/main^{tree}')"
commit_count="$("$git_transport" --repository "$snapshot" rev-list --count --all)"
parent_count="$("$git_transport" --repository "$snapshot" cat-file -p "$snapshot_commit" | grep -c '^parent ' || true)"
refs="$("$git_transport" --repository "$snapshot" for-each-ref --format='%(refname)')"
head_ref="$("$git_transport" --repository "$snapshot" symbolic-ref HEAD 2>/dev/null || true)"
remotes="$("$git_transport" --repository "$snapshot" remote)"

author_name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["author_name"])' "$identity_policy")"
author_email="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["author_email"])' "$identity_policy")"
commit_message="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["commit_message"])' "$identity_policy")"
commit_epoch="$(python3 -c 'import datetime,json,sys; value=json.load(open(sys.argv[1], encoding="utf-8"))["commit_date_utc"]; print(int(datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()))' "$identity_policy")"
metadata="$("$git_transport" --repository "$snapshot" show -s --format='%an%n%ae%n%cn%n%ce%n%s' "$snapshot_commit")"
expected_metadata="$(printf '%s\n%s\n%s\n%s\n%s' \
  "$author_name" "$author_email" "$author_name" "$author_email" "$commit_message")"
author_record="$("$git_transport" --repository "$snapshot" cat-file -p "$snapshot_commit" | sed -n 's/^author /author /p')"
committer_record="$("$git_transport" --repository "$snapshot" cat-file -p "$snapshot_commit" | sed -n 's/^committer /committer /p')"
expected_author_record="author $author_name <$author_email> $commit_epoch +0000"
expected_committer_record="committer $author_name <$author_email> $commit_epoch +0000"
expected_snapshot_commit="$({
  export GIT_AUTHOR_NAME="$author_name"
  export GIT_AUTHOR_EMAIL="$author_email"
  export GIT_COMMITTER_NAME="$author_name"
  export GIT_COMMITTER_EMAIL="$author_email"
  export GIT_AUTHOR_DATE="@$commit_epoch +0000"
  export GIT_COMMITTER_DATE="@$commit_epoch +0000"
  printf '%s\n' "$commit_message" \
    | "$git_transport" --trusted-commit-identity \
        --repository "$snapshot" commit-tree "$source_tree"
})"

if [ "$snapshot_commit" != "$expected_snapshot_commit" ] \
  || [ "$snapshot_tree" != "$source_tree" ] \
  || [ "$commit_count" -ne 1 ] \
  || [ "$parent_count" -ne 0 ] \
  || [ "$refs" != "refs/heads/main" ] \
  || [ "$head_ref" != "refs/heads/main" ] \
  || [ -n "$remotes" ] \
  || [ "$metadata" != "$expected_metadata" ] \
  || [ "$author_record" != "$expected_author_record" ] \
  || [ "$committer_record" != "$expected_committer_record" ]; then
  echo "FAIL public-root-publisher: prepared snapshot/source invariant failed" >&2
  exit 1
fi
if [ "$source_commit" != "$snapshot_commit" ] \
  && "$git_transport" --repository "$snapshot" cat-file -e "$source_commit^{commit}" 2>/dev/null; then
  echo "FAIL public-root-publisher: private source commit leaked into the public snapshot" >&2
  exit 1
fi
fsck_output="$("$git_transport" --repository "$snapshot" fsck --full --strict --no-reflogs --unreachable 2>&1)" || {
  echo "FAIL public-root-publisher: prepared snapshot object verification failed" >&2
  printf '%s\n' "$fsck_output" >&2
  exit 1
}
if printf '%s\n' "$fsck_output" | grep -Eq '(^| )((unreachable|dangling) )'; then
  echo "FAIL public-root-publisher: prepared snapshot contains unreachable objects" >&2
  exit 1
fi

export PERFECTORY_EXPECTED_PUBLIC_ROOT="$snapshot_commit"
if ! remote_state="$("$git_transport" --no-repository ls-remote --symref "$remote_url")"; then
  echo "FAIL public-root-publisher: could not read the canonical public remote" >&2
  exit 1
fi
expected_remote_state="$(printf 'ref: refs/heads/main\tHEAD\n%s\tHEAD\n%s\trefs/heads/main' \
  "$snapshot_commit" "$snapshot_commit")"
if [ -z "$remote_state" ]; then
  publication_mode=fresh
elif [ "$remote_state" = "$expected_remote_state" ]; then
  publication_mode=resume
else
  echo "FAIL public-root-publisher: remote is neither empty nor the exact expected root" >&2
  printf '%s\n' "$remote_state" >&2
  exit 1
fi

if [ "$publication_mode" = fresh ]; then
  "$configurator" prepublish
  "$publication_authority"
  publication_started=1
  "$git_transport" --repository "$snapshot" push "$remote_url" \
    "$snapshot_commit:refs/heads/main"
  "$configurator" lock
  main_locked=1
fi

# Both a fresh publish and an exact-SHA resume must cross the same independent
# clone boundary before the idempotent activation step.
"$git_transport" --no-repository \
  clone --quiet "$remote_url" "$verification_clone"
clone_commit="$("$git_transport" --repository "$verification_clone" rev-parse refs/heads/main)"
clone_tree="$("$git_transport" --repository "$verification_clone" rev-parse 'refs/heads/main^{tree}')"
clone_count="$("$git_transport" --repository "$verification_clone" rev-list --count --all)"
clone_parent_count="$("$git_transport" --repository "$verification_clone" cat-file -p "$clone_commit" | grep -c '^parent ' || true)"
clone_tags="$("$git_transport" --repository "$verification_clone" tag --list)"
clone_metadata="$("$git_transport" --repository "$verification_clone" show -s --format='%an%n%ae%n%cn%n%ce%n%s' "$clone_commit")"
clone_author_record="$("$git_transport" --repository "$verification_clone" cat-file -p "$clone_commit" | sed -n 's/^author /author /p')"
clone_committer_record="$("$git_transport" --repository "$verification_clone" cat-file -p "$clone_commit" | sed -n 's/^committer /committer /p')"

if [ "$clone_commit" != "$snapshot_commit" ] \
  || [ "$clone_tree" != "$snapshot_tree" ] \
  || [ "$clone_count" -ne 1 ] \
  || [ "$clone_parent_count" -ne 0 ] \
  || [ -n "$clone_tags" ] \
  || [ "$clone_metadata" != "$expected_metadata" ] \
  || [ "$clone_author_record" != "$expected_author_record" ] \
  || [ "$clone_committer_record" != "$expected_committer_record" ]; then
  echo "FAIL public-root-publisher: independent clone invariant failed" >&2
  exit 1
fi

"$configurator" activate
publication_started=0
echo "OK public-root-publisher mode=$publication_mode commit=$snapshot_commit tree=$snapshot_tree target=$target"
