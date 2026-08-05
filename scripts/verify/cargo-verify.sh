#!/usr/bin/env bash
# Runs one area's verification inside the CI-equivalent Linux container, via the
# single verification SSOT: `cargo xtask verify <area>` (ADR-0004). The harness
# owns only Docker orchestration; fmt/clippy/test (+ native deps, + gongzzang's
# two-stage contract) live in tools/xtask so local and CI cannot drift.
# Usage: scripts/verify/cargo-verify.sh <area-dir>
#   e.g. scripts/verify/cargo-verify.sh platforms/identity-platform
# Caches: normal development runs use named volumes (cargo registry shared,
# target per area, plus a dedicated xtask target). Publication audits set
# PERFECTORY_CLEAN_VERIFY=1 to use only fresh anonymous build-state volumes.
set -euo pipefail
AREA="${1:?area dir required, e.g. products/gongzzang}"
shift || true
SLUG="$(echo "$AREA" | tr '/' '-')"
# pwd -W: Windows-style path under Git Bash (docker needs it); plain pwd elsewhere.
REPO="$(cd "$(dirname "$0")/../.." && { pwd -W 2>/dev/null || pwd; })"
# The same root as a path this shell can open. `$REPO` is Windows-style for Docker's benefit, which
# `cp` and friends under Git Bash cannot follow.
REPO_POSIX="$(cd "$(dirname "$0")/../.." && pwd)"
source "$(dirname "$0")/../../tools/container-images.env"
# A linked worktree stores only a `.git` pointer whose target is outside the
# bind-mounted tree. Build a disposable Git directory containing the current
# index and only the blobs referenced by that index. Repository guards can then
# inspect real sizes/content without exposing private commits or prior history
# to build scripts running in the container.
SOURCE_GIT_INDEX="$(git -C "$REPO" rev-parse --path-format=absolute --git-path index)"
SOURCE_GIT_OBJECTS="$(git -C "$REPO" rev-parse --path-format=absolute --git-path objects)"
SAFE_GIT_ROOT="$(mktemp -d)"
cleanup() {
  case "${SAFE_GIT_ROOT:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*)
      [ ! -e "$SAFE_GIT_ROOT" ] || rm -rf -- "$SAFE_GIT_ROOT"
      ;;
    *) echo "cargo-verify: refusing unsafe temporary cleanup" >&2 ;;
  esac
}
trap cleanup EXIT
SAFE_GIT_REPOSITORY="$SAFE_GIT_ROOT/repository"
git init --quiet "$SAFE_GIT_REPOSITORY"
SAFE_GIT_DIR="$SAFE_GIT_REPOSITORY/.git"
cp "$SOURCE_GIT_INDEX" "$SAFE_GIT_DIR/index"
# Present the isolated metadata through a normal worktree-local .git pointer.
# An ambient GIT_DIR/GIT_WORK_TREE would leak into nested synthetic-repository
# tests and redirect their writes into this disposable metadata mount.
SAFE_GIT_CONFIG="$(cd "$SAFE_GIT_DIR" \
  && printf '%s/config\n' "$(pwd -W 2>/dev/null || pwd -P)")"
MSYS_NO_PATHCONV=1 git config --file "$SAFE_GIT_CONFIG" core.worktree /work
SAFE_GIT_POINTER="$SAFE_GIT_ROOT/worktree.git"
printf 'gitdir: /perfectory-git\n' >"$SAFE_GIT_POINTER"
# A read-only isolated `.git` must still allow the harness to hash an unstaged
# worktree file while checking immutability.  Keep all newly-created blobs and
# trees in a disposable object directory, while using the source object store
# read-only as an alternate for existing indexed objects.
VERIFY_OBJECT_DIR="$SAFE_GIT_ROOT/objects"
mkdir -p "$VERIFY_OBJECT_DIR"
# `git write-tree` may refresh/cache the repository index.  The verification
# container deliberately mounts its source `.git` read-only, and nested guard
# self-tests invoke this harness again from inside that container.  Build the
# snapshot tree through a disposable index so neither the real nor the
# read-only isolated index ever needs an `index.lock`.
SNAPSHOT_INDEX="$SAFE_GIT_ROOT/source-index"
cp "$SOURCE_GIT_INDEX" "$SNAPSHOT_INDEX"
SOURCE_TREE="$(
  GIT_INDEX_FILE="$SNAPSHOT_INDEX" \
  GIT_OBJECT_DIRECTORY="$VERIFY_OBJECT_DIR" \
  GIT_ALTERNATE_OBJECT_DIRECTORIES="$SOURCE_GIT_OBJECTS" \
    git -C "$REPO" write-tree
)"
printf '%s\n' "$SOURCE_TREE" \
  | GIT_OBJECT_DIRECTORY="$VERIFY_OBJECT_DIR" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$SOURCE_GIT_OBJECTS" \
      git -C "$REPO" pack-objects --stdout --revs \
  | git --git-dir="$SAFE_GIT_DIR" index-pack --stdin >/dev/null
# The disposable repository intentionally has no history, but Git status and
# repository guards still need a baseline commit. Without one every indexed
# path appears as an added file inside the container, which makes the
# gongzzang/public-tree guards reject an otherwise clean verification snapshot.
VERIFY_TREE="$SOURCE_TREE"
VERIFY_COMMIT="$(
  GIT_AUTHOR_NAME=perfectory-verify \
  GIT_AUTHOR_EMAIL=verify@localhost \
  GIT_COMMITTER_NAME=perfectory-verify \
  GIT_COMMITTER_EMAIL=verify@localhost \
    git --git-dir="$SAFE_GIT_DIR" commit-tree "$VERIFY_TREE" -m "verification snapshot"
)"
git --git-dir="$SAFE_GIT_DIR" update-ref refs/heads/verify "$VERIFY_COMMIT"
git --git-dir="$SAFE_GIT_DIR" symbolic-ref HEAD refs/heads/verify
GIT_DIR_MOUNT="$(cd "$SAFE_GIT_DIR" && { pwd -W 2>/dev/null || pwd; })"
# A linked worktree has a `.git` pointer file, while a normal clone has a
# `.git` directory. Docker cannot mount a file over the latter directory, so
# overlay the disposable Git directory directly when the source already has a
# directory-shaped metadata path.
if [ -d "$REPO/.git" ]; then
  GIT_METADATA_MOUNT="$GIT_DIR_MOUNT"
else
  GIT_METADATA_MOUNT="$(cd "$(dirname "$SAFE_GIT_POINTER")" \
    && printf '%s/%s\n' "$(pwd -W 2>/dev/null || pwd)" "$(basename "$SAFE_GIT_POINTER")")"
fi
# xtask runs from /work (repo root) so it reads the root .cargo/config.toml alias
# and resolves area paths itself; it accepts the dir form ("$AREA") too.
# MSYS_NO_PATHCONV: stop Git Bash rewriting /work/... args into C:/Program Files/Git/...
clean_verify="${PERFECTORY_CLEAN_VERIFY:-0}"
if [ "$clean_verify" != 0 ] && [ "$clean_verify" != 1 ]; then
  echo "FAIL cargo-verify: PERFECTORY_CLEAN_VERIFY must be 0 or 1" >&2
  exit 2
fi

# Docker Desktop creates volume mountpoints before applying the `/work` bind. A
# history-free public clone has no ignored `target/` folders, so create only
# these disposable, ignored mountpoints on the host first.
for mountpoint in "$REPO/$AREA/target" "$REPO/tools/xtask/target"; do
  if [ -e "$mountpoint" ] && [ ! -d "$mountpoint" ]; then
    echo "FAIL cargo-verify: expected directory mountpoint '$mountpoint'" >&2
    exit 1
  fi
  mkdir -p -- "$mountpoint"
done

if [ "$clean_verify" -eq 1 ] \
  && [ "${PERFECTORY_CARGO_VERIFY_TEST_DOUBLE:-0}" != 1 ]; then
  # Build a tree from the tracked worktree through a disposable index.  A
  # normal `git status`/`git diff --binary` walks the Windows bind-mounted
  # worktree and can block on huge ignored build trees; `git add -u` visits only
  # paths already in the index and gives us the same exact content/mode
  # invariant without touching the read-only source metadata.
  snapshot_worktree_tree() {
    local check_index="$1"
    cp "$SOURCE_GIT_INDEX" "$check_index"
    GIT_INDEX_FILE="$check_index" \
    GIT_OBJECT_DIRECTORY="$VERIFY_OBJECT_DIR" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$SOURCE_GIT_OBJECTS" \
      git -C "$REPO" add -u -- . >/dev/null
    GIT_INDEX_FILE="$check_index" \
    GIT_OBJECT_DIRECTORY="$VERIFY_OBJECT_DIR" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$SOURCE_GIT_OBJECTS" \
      git -C "$REPO" write-tree
  }
  BEFORE_CHECK_INDEX="$SAFE_GIT_ROOT/check-before-index"
  source_index_hash_before="$(sha256sum "$SOURCE_GIT_INDEX" | awk '{print $1}')"
  source_worktree_tree_before="$(snapshot_worktree_tree "$BEFORE_CHECK_INDEX")"
fi

# Build the verification image before the run. It layers every area's apt dependencies and the fmt
# and clippy components onto the pinned toolchain, which is what removes the last step that needed
# the internet — see tools/verify-image/Dockerfile. Docker caches the layers, so this is a no-op
# after the first build and whenever the base pin and the dependency list are unchanged.
#
VERIFY_IMAGE="perfectory-verify:local"
# The context is the repository root because the image copies `rust-toolchain.toml`, which lives
# there and is the single toolchain pin — assembling a smaller context elsewhere would either
# duplicate the pin or hand the guard a computed path, and
# `scripts/guard/check-container-runtime-policy.sh` requires a literal repository-relative one so a
# context is auditable. The root `.dockerignore` narrows what that costs to the two files it needs.
#
# The command carries no redirection and ends at its context on purpose: that guard reads the whole
# logical line and treats every trailing token as another context, so a `>log` or a closing paren
# beside the `.` reads as two contexts and is refused. Build output therefore streams; it is a few
# lines once the layers are cached. `set -e` makes a failed build stop the run.
(
  cd "$REPO_POSIX"
  MSYS_NO_PATHCONV=1 docker build -f ./tools/verify-image/Dockerfile -t "$VERIFY_IMAGE" .
)

docker_args=(
  run --rm
  # Offline verification, enforced rather than assumed. Cargo resolves from the mounted registry and
  # every apt dependency is already in the image, so nothing here has a reason to reach the network —
  # and a test that quietly reaches one to decide it can pass now cannot (ADR-0010 남은 부채 4).
  # Loopback still exists, so tests that stand up their own local server are unaffected.
  --network none
  --security-opt no-new-privileges
  --pids-limit 2048
  --mount "type=bind,source=$GIT_DIR_MOUNT,target=/perfectory-git,readonly"
  --mount "type=bind,source=$GIT_METADATA_MOUNT,target=/work/.git,readonly"
  -w /work
  -e SQLX_OFFLINE=true
  -e GIT_OPTIONAL_LOCKS=0
  -e CARGO_TERM_COLOR=always
  -e "PERFECTORY_VERIFY_AREA=$AREA"
)

# Large workspaces can exceed a developer's Docker memory limit when Cargo
# starts one rustc per crate. Keep the default unchanged, but allow callers to
# cap parallelism without bypassing the verification SSOT.
if [ -n "${CARGO_BUILD_JOBS:-}" ]; then
  docker_args+=( -e "CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS" )
fi

if [ "$clean_verify" -eq 1 ]; then
  # Cargo's build state is fresh and deleted automatically with --rm. The
  # source bind stays writable only because Docker Desktop cannot layer target
  # volumes under a read-only parent; the post-run Git integrity check below
  # makes tracked source mutation fail closed.
  docker_args+=(
    --mount "type=bind,source=$REPO,target=/work"
    --mount "type=volume,target=/usr/local/cargo/registry,volume-nocopy"
    --mount "type=volume,target=/work/$AREA/target,volume-nocopy"
  )
  # tools/xtask became an area in its own right (ADR-0011). When it IS the area
  # the line above already mounts that path, and repeating it makes Docker reject
  # the run with a duplicate mount point.
  if [ "$AREA" != "tools/xtask" ]; then
    docker_args+=(--mount "type=volume,target=/work/tools/xtask/target,volume-nocopy")
  fi
else
  # Named caches are an intentional performance optimization for ordinary
  # local/CI verification; they are never used by the publication audit.
  # No rustup volume. It used to cache components added at run time, and mounting it over
  # /usr/local/rustup hid the toolchain the verification image carries — including the fmt and clippy
  # components baked in there. The image is the one source of the toolchain now.
  docker_args+=(
    -v "$REPO:/work"
    -v perfectory-cargo-registry:/usr/local/cargo/registry
    -v "perfectory-target-$SLUG:/work/$AREA/target"
  )
  if [ "$AREA" != "tools/xtask" ]; then
    docker_args+=(-v perfectory-target-xtask:/work/tools/xtask/target)
  fi
fi

MSYS_NO_PATHCONV=1 docker "${docker_args[@]}" \
  "$VERIFY_IMAGE" bash -ceu '
    cargo xtask verify "$PERFECTORY_VERIFY_AREA"
  '

if [ "$clean_verify" -eq 1 ] \
  && [ "${PERFECTORY_CARGO_VERIFY_TEST_DOUBLE:-0}" != 1 ]; then
  # Docker Desktop cannot reliably layer writable target volumes under a
  # read-only parent bind on a normal public clone. The source bind is
  # therefore writable for this disposable run; enforce the invariant at the
  # boundary so tracked source edits are still a hard failure.
  AFTER_CHECK_INDEX="$SAFE_GIT_ROOT/check-after-index"
  source_index_hash_after="$(sha256sum "$SOURCE_GIT_INDEX" | awk '{print $1}')"
  source_worktree_tree_after="$(snapshot_worktree_tree "$AFTER_CHECK_INDEX")"
  if [ "$source_index_hash_before" != "$source_index_hash_after" ] \
    || [ "$source_worktree_tree_before" != "$source_worktree_tree_after" ]; then
    echo "FAIL cargo-verify: verification mutated the source worktree" >&2
    GIT_INDEX_FILE="$AFTER_CHECK_INDEX" git -C "$REPO" diff --name-status >&2 || true
    exit 1
  fi
fi
