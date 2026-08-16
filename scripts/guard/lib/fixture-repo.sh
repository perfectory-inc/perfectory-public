#!/usr/bin/env bash
# The one definition of "this script builds throwaway Git repositories".
#
# THE INCIDENT THIS FILE EXISTS FOR
#
# A `git push` ran the pre-push hook. Git starts a hook with GIT_DIR pointing at
# the real repository and GIT_WORK_TREE unset:
#
#     GIT_DIR=C:/.../perfectory/.git/worktrees/hook-isolation-fix
#     GIT_WORK_TREE=<unset>
#
# Under that environment `git -C "$tmp" init` does NOT create a repository in
# $tmp. GIT_DIR still names the real repository, and because GIT_WORK_TREE is
# unset git treats the current directory -- $tmp -- as the top of the working
# tree. So a self-test's `git -C "$tmp" add -A` staged "everything under $tmp is
# added, everything else is deleted" against the REAL index, and `git commit -m
# fixture` landed on the REAL branch. Five fixture commits were created that way
# and one of them deleted 2,169 files. The pushed head therefore contained no
# .github/workflows, so no CI workflow matched and the pull request reported zero
# runs -- which is why the outage first looked like a GitHub permissions problem.
#
# `mktemp -d` isolated the filesystem. Nothing isolated the Git environment.
#
# WHY A SHARED FILE RATHER THAN A FIX IN THE SCRIPT THAT BLEW UP
#
# Seven scripts under scripts/guard/ already carried the correct `unset` line,
# copy-pasted. Ten did not, including the two that caused the incident. A rule
# that has to be re-typed into every new self-test is a rule that will be missed
# again, so it lives here once and is inherited by sourcing.
#
# HOW TO USE IT
#
#   . "$(dirname "${BASH_SOURCE[0]}")/lib/fixture-repo.sh"
#
# Sourcing alone removes the ambient repository binding, which is enough to make
# a plain `git -C "$dir" ...` safe. Scripts that build fixtures should go one step
# further and use `fixture_root`/`fixture_git`, which make targeting the real
# repository unrepresentable rather than merely unlikely: `fixture_git` pins both
# ends of the binding to the fixture and refuses any target that is not inside a
# directory `fixture_root` created.

# Every environment variable through which an ancestor process can bind git to a
# repository that is not the one named on the command line. Hooks set GIT_DIR;
# filter/merge drivers and `git rebase` add the others.
FIXTURE_REPO_AMBIENT_VARIABLES=(
  GIT_DIR
  GIT_WORK_TREE
  GIT_INDEX_FILE
  GIT_COMMON_DIR
  GIT_OBJECT_DIRECTORY
  GIT_ALTERNATE_OBJECT_DIRECTORIES
  GIT_NAMESPACE
  GIT_PREFIX
  GIT_CEILING_DIRECTORIES
  GIT_DISCOVERY_ACROSS_FILESYSTEM
)

# Drops the inherited binding. Idempotent, and safe to call under `set -u`.
fixture_repo_release_ambient_repository() {
  unset "${FIXTURE_REPO_AMBIENT_VARIABLES[@]}" 2>/dev/null || true
}

# The repository this file is checked into. Resolved before the ambient binding
# is dropped and never through `git`, so it stays correct no matter what the
# caller's environment claims.
FIXTURE_REPO_REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"

fixture_repo_fail() {
  echo "FAIL fixture-repo: $*" >&2
  exit 1
}

# True when $2 is $1 itself or a path underneath it. Compares resolved paths, so
# a symlinked or relative spelling cannot slip past the boundary.
fixture_repo_path_is_within() {
  local parent="$1" candidate="$2"
  case "$candidate" in
    "$parent" | "$parent"/*) return 0 ;;
    *) return 1 ;;
  esac
}

# Directories handed out by `fixture_root`; the only legal targets for
# `fixture_git`.
FIXTURE_REPO_ROOTS=()

# The directory the most recent `fixture_root` created.
FIXTURE_ROOT=""

# Creates a throwaway directory to build fixtures in and registers it.
#
#     fixture_root
#     test_root="$FIXTURE_ROOT"
#
# It reports through a variable rather than stdout on purpose: `x="$(fixture_root)"`
# would run the registration inside a command-substitution subshell and throw it
# away, leaving `fixture_git` with nothing to recognise.
#
# Refuses a path that is empty, relative, or inside this repository -- the three
# ways an "isolated" temporary directory has been observed to end up pointing at
# the checkout. Failing here is the point: a self-test that cannot get an
# isolated directory must stop, not continue with a path that resolves to the
# working tree.
fixture_root() {
  local created
  created="$(mktemp -d)" || fixture_repo_fail "mktemp -d failed; refusing to continue without an isolated directory"
  [ -n "$created" ] || fixture_repo_fail "mktemp -d returned an empty path"
  [ -d "$created" ] || fixture_repo_fail "mktemp -d returned '$created', which is not a directory"

  local resolved
  resolved="$(cd "$created" && pwd -P)" || fixture_repo_fail "cannot resolve '$created'"
  case "$resolved" in
    /* | [A-Za-z]:/*) ;;
    *) fixture_repo_fail "temporary directory '$resolved' is not an absolute path" ;;
  esac
  if fixture_repo_path_is_within "$FIXTURE_REPO_REPOSITORY_ROOT" "$resolved"; then
    fixture_repo_fail "temporary directory '$resolved' is inside the repository ($FIXTURE_REPO_REPOSITORY_ROOT); set TMPDIR outside the checkout"
  fi

  FIXTURE_REPO_ROOTS+=("$resolved")
  FIXTURE_ROOT="$resolved"
}

# Runs git against a fixture directory and nothing else.
#
#     fixture_git "$dir" init -q
#     fixture_git "$dir" add -A
#
# The target must be inside a directory this file handed out, and the repository
# binding is pinned to that target: GIT_DIR and GIT_WORK_TREE are both derived
# from $1, so there is no environment an ancestor can set -- and no argument a
# caller can pass -- that redirects the command at the real checkout. An empty
# first argument is a hard error rather than the documented `git -C ""` no-op
# that silently means "use the current directory".
fixture_git() {
  local target="${1-}"
  [ "$#" -ge 1 ] || fixture_repo_fail "fixture_git needs a target directory"
  shift

  [ -n "$target" ] || fixture_repo_fail "fixture_git was given an empty target directory"
  [ -d "$target" ] || fixture_repo_fail "fixture_git target '$target' does not exist"

  local resolved
  resolved="$(cd "$target" && pwd -P)" || fixture_repo_fail "cannot resolve fixture target '$target'"

  if [ "${#FIXTURE_REPO_ROOTS[@]}" -eq 0 ]; then
    fixture_repo_fail "fixture_git called before any fixture_root; there is no isolated directory to work in"
  fi

  local root within=0
  for root in "${FIXTURE_REPO_ROOTS[@]}"; do
    if fixture_repo_path_is_within "$root" "$resolved"; then
      within=1
      break
    fi
  done
  [ "$within" -eq 1 ] ||
    fixture_repo_fail "fixture_git target '$resolved' is outside every fixture directory; refusing to run git there"

  # Belt and braces: even a registered root must never be the checkout.
  if fixture_repo_path_is_within "$FIXTURE_REPO_REPOSITORY_ROOT" "$resolved"; then
    fixture_repo_fail "fixture_git target '$resolved' is inside the repository; refusing"
  fi

  GIT_DIR="$resolved/.git" \
  GIT_WORK_TREE="$resolved" \
  GIT_CEILING_DIRECTORIES="$root" \
    git -C "$resolved" "$@"
}

# Sourcing is the adoption step: the binding goes away for the rest of the
# process, so even a bare `git -C "$tmp"` in the sourcing script is safe.
fixture_repo_release_ambient_repository
