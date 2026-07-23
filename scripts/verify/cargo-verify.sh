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
source "$(dirname "$0")/../../tools/container-images.env"
# A linked worktree stores only a `.git` pointer whose target is outside the
# bind-mounted tree. Build a disposable Git directory containing the current
# index and only the blobs referenced by that index. Repository guards can then
# inspect real sizes/content without exposing private commits or prior history
# to build scripts running in the container.
SOURCE_GIT_INDEX="$(git -C "$REPO" rev-parse --path-format=absolute --git-path index)"
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
git -C "$REPO" ls-files --stage \
  | awk '$1 == "100644" || $1 == "100755" || $1 == "120000" { print $2 }' \
  | sort -u >"$SAFE_GIT_ROOT/blob-oids"
if [ -s "$SAFE_GIT_ROOT/blob-oids" ]; then
  git -C "$REPO" pack-objects --stdout <"$SAFE_GIT_ROOT/blob-oids" \
    | git --git-dir="$SAFE_GIT_DIR" index-pack --stdin >/dev/null
fi
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

if [ "$clean_verify" -eq 1 ]; then
  source_status_before="$(git -C "$REPO" status --porcelain=v1 --untracked-files=all)"
  source_worktree_diff_before="$(git -C "$REPO" diff --binary -- . | git hash-object --stdin)"
  source_index_diff_before="$(git -C "$REPO" diff --cached --binary -- . | git hash-object --stdin)"
fi

docker_args=(
  run --rm
  --security-opt no-new-privileges
  --pids-limit 2048
  --mount "type=bind,source=$GIT_DIR_MOUNT,target=/perfectory-git,readonly"
  --mount "type=bind,source=$GIT_METADATA_MOUNT,target=/work/.git,readonly"
  -w /work
  -e SQLX_OFFLINE=true
  -e CARGO_TERM_COLOR=always
)

if [ "$clean_verify" -eq 1 ]; then
  # Cargo's build state is fresh and deleted automatically with --rm. The
  # source bind stays writable only because Docker Desktop cannot layer target
  # volumes under a read-only parent; the post-run Git integrity check below
  # makes tracked source mutation fail closed.
  docker_args+=(
    --mount "type=bind,source=$REPO,target=/work"
    --mount "type=volume,target=/usr/local/cargo/registry,volume-nocopy"
    --mount "type=volume,target=/work/$AREA/target,volume-nocopy"
    --mount "type=volume,target=/work/tools/xtask/target,volume-nocopy"
  )
else
  # Named caches are an intentional performance optimization for ordinary
  # local/CI verification; they are never used by the publication audit.
  docker_args+=(
    -v "$REPO:/work"
    -v perfectory-cargo-registry:/usr/local/cargo/registry
    -v perfectory-rustup:/usr/local/rustup
    -v "perfectory-target-$SLUG:/work/$AREA/target"
    -v perfectory-target-xtask:/work/tools/xtask/target
  )
fi

MSYS_NO_PATHCONV=1 docker "${docker_args[@]}" \
  "$RUST_TOOLCHAIN_IMAGE" bash -ceu '
    rustup component add rustfmt clippy >/dev/null 2>&1 || true
    cargo xtask verify "$1"
  ' _ "$AREA"

if [ "$clean_verify" -eq 1 ]; then
  # Docker Desktop cannot reliably layer writable target volumes under a
  # read-only parent bind on a normal public clone. The source bind is
  # therefore writable for this disposable run; enforce the invariant at the
  # boundary so tracked source edits are still a hard failure.
  source_status_after="$(git -C "$REPO" status --porcelain=v1 --untracked-files=all)"
  source_worktree_diff_after="$(git -C "$REPO" diff --binary -- . | git hash-object --stdin)"
  source_index_diff_after="$(git -C "$REPO" diff --cached --binary -- . | git hash-object --stdin)"
  if [ "$source_status_before" != "$source_status_after" ] \
    || [ "$source_worktree_diff_before" != "$source_worktree_diff_after" ] \
    || [ "$source_index_diff_before" != "$source_index_diff_after" ]; then
    echo "FAIL cargo-verify: verification mutated the source worktree" >&2
    git -C "$REPO" status --short >&2 || true
    exit 1
  fi
fi
