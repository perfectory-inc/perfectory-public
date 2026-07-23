#!/usr/bin/env bash
# Builds a history-free bare repository from one committed source tree.
# The destination is created once, outside the source worktree, and receives
# only a new parentless commit plus the tree/blob objects reachable from it.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <source-repository> <source-commit> <new-bare-repository.git>" >&2
  exit 2
fi

for command_name in git realpath dirname python3; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL public-root-builder: missing command '$command_name'" >&2
    exit 1
  }
done

source_input="$1"
source_ref="$2"
destination_input="$3"
script_root="$(cd "$(dirname "$0")/../.." && pwd)"
identity_policy="$script_root/tools/github/public-root-identity.json"
git_transport="$script_root/scripts/github/safe-git-transport.sh"
[ -f "$identity_policy" ] || {
  echo "FAIL public-root-builder: missing identity policy" >&2
  exit 1
}
author_name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["author_name"])' "$identity_policy")"
author_email="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["author_email"])' "$identity_policy")"
commit_message="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["commit_message"])' "$identity_policy")"
commit_date="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["commit_date_utc"])' "$identity_policy")"
commit_epoch="$(python3 -c 'import datetime,json,sys; value=json.load(open(sys.argv[1], encoding="utf-8"))["commit_date_utc"]; print(int(datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()))' "$identity_policy")"

if [ "$author_email" != "public-root@perfectory.invalid" ] \
  || [ "$author_name" != "Perfectory" ] \
  || [ "$commit_message" != "chore: publish audited source snapshot" ] \
  || ! printf '%s\n' "$commit_date" | grep -Eq '^20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' \
  || ! printf '%s\n' "$commit_epoch" | grep -Eq '^[0-9]+$'; then
  echo "FAIL public-root-builder: public root identity policy is not approved" >&2
  exit 1
fi

source_root="$("$git_transport" --repository "$source_input" rev-parse --show-toplevel 2>/dev/null)" || {
  echo "FAIL public-root-builder: source is not a Git worktree" >&2
  exit 1
}
source_root="$(realpath "$source_root")"
source_commit="$("$git_transport" --repository "$source_root" rev-parse --verify "$source_ref^{commit}" 2>/dev/null)" || {
  echo "FAIL public-root-builder: source ref is not a commit" >&2
  exit 1
}
source_tree="$("$git_transport" --repository "$source_root" rev-parse "$source_commit^{tree}")"

destination="$(realpath -m "$destination_input")"
if [ -e "$destination" ]; then
  echo "FAIL public-root-builder: destination already exists: $destination" >&2
  exit 1
fi
if [ ! -d "$(dirname "$destination")" ]; then
  echo "FAIL public-root-builder: destination parent does not exist" >&2
  exit 1
fi
case "$destination" in
  "$source_root"|"$source_root"/*)
    echo "FAIL public-root-builder: destination must be outside the source worktree" >&2
    exit 1
    ;;
esac
case "$destination" in
  *.git) ;;
  *)
    echo "FAIL public-root-builder: destination must end in .git" >&2
    exit 1
    ;;
esac

root_commit="$({
  export GIT_AUTHOR_NAME="$author_name"
  export GIT_AUTHOR_EMAIL="$author_email"
  export GIT_COMMITTER_NAME="$author_name"
  export GIT_COMMITTER_EMAIL="$author_email"
  export GIT_AUTHOR_DATE="@$commit_epoch +0000"
  export GIT_COMMITTER_DATE="@$commit_epoch +0000"
  printf '%s\n' "$commit_message" \
    | "$git_transport" --trusted-commit-identity \
        --repository "$source_root" commit-tree "$source_tree"
})"

"$git_transport" --no-repository init -q --bare --initial-branch=main "$destination"
"$git_transport" --repository "$source_root" push --quiet "$destination" \
  "$root_commit:refs/heads/main"
"$git_transport" --repository "$destination" symbolic-ref HEAD refs/heads/main

snapshot_commit="$("$git_transport" --repository "$destination" rev-parse refs/heads/main)"
snapshot_tree="$("$git_transport" --repository "$destination" rev-parse 'refs/heads/main^{tree}')"
commit_count="$("$git_transport" --repository "$destination" rev-list --count refs/heads/main)"
parent_count="$("$git_transport" --repository "$destination" cat-file -p "$snapshot_commit" | grep -c '^parent ' || true)"
refs="$("$git_transport" --repository "$destination" for-each-ref --format='%(refname)')"
metadata="$("$git_transport" --repository "$destination" show -s --format='%an%n%ae%n%cn%n%ce%n%s' "$snapshot_commit")"
expected_metadata="$(printf '%s\n%s\n%s\n%s\n%s' \
  "$author_name" "$author_email" "$author_name" "$author_email" "$commit_message")"
author_record="$("$git_transport" --repository "$destination" cat-file -p "$snapshot_commit" | sed -n 's/^author /author /p')"
committer_record="$("$git_transport" --repository "$destination" cat-file -p "$snapshot_commit" | sed -n 's/^committer /committer /p')"
expected_author_record="author $author_name <$author_email> $commit_epoch +0000"
expected_committer_record="committer $author_name <$author_email> $commit_epoch +0000"

if [ "$snapshot_commit" != "$root_commit" ] \
  || [ "$snapshot_tree" != "$source_tree" ] \
  || [ "$commit_count" -ne 1 ] \
  || [ "$parent_count" -ne 0 ] \
  || [ "$refs" != "refs/heads/main" ] \
  || [ "$metadata" != "$expected_metadata" ] \
  || [ "$author_record" != "$expected_author_record" ] \
  || [ "$committer_record" != "$expected_committer_record" ]; then
  echo "FAIL public-root-builder: history-free snapshot invariant failed; destination retained for inspection" >&2
  printf '  commit=%s expected=%s\n  tree=%s expected=%s\n  count=%s parents=%s\n  refs=%s\n' \
    "$snapshot_commit" "$root_commit" "$snapshot_tree" "$source_tree" \
    "$commit_count" "$parent_count" "$refs" >&2
  printf '  metadata=%q\n  expected_metadata=%q\n' \
    "$metadata" "$expected_metadata" >&2
  printf '  author=%q\n  expected_author=%q\n  committer=%q\n  expected_committer=%q\n' \
    "$author_record" "$expected_author_record" "$committer_record" "$expected_committer_record" >&2
  exit 1
fi
if [ "$source_commit" != "$root_commit" ] \
  && "$git_transport" --repository "$destination" cat-file -e "$source_commit^{commit}" 2>/dev/null; then
  echo "FAIL public-root-builder: source commit leaked into snapshot; destination retained for inspection" >&2
  exit 1
fi
fsck_output="$("$git_transport" --repository "$destination" fsck --full --strict --no-reflogs --unreachable 2>&1)" || {
  echo "FAIL public-root-builder: snapshot object verification failed; destination retained for inspection" >&2
  printf '%s\n' "$fsck_output" >&2
  exit 1
}
if printf '%s\n' "$fsck_output" | grep -Eq '(^| )((unreachable|dangling) )'; then
  echo "FAIL public-root-builder: snapshot contains unreachable objects; destination retained for inspection" >&2
  printf '%s\n' "$fsck_output" >&2
  exit 1
fi

printf 'OK public-root-builder commit=%s tree=%s destination=%s\n' \
  "$root_commit" "$source_tree" "$destination"
