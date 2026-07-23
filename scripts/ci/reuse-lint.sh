#!/usr/bin/env bash
# Verify repository-wide REUSE licensing with the digest-pinned official image.
# The source tree is read-only and the tool has no network or Linux capabilities.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/container-images.env

for command_name in docker git mktemp; do
  command -v "$command_name" >/dev/null 2>&1 || {
    if [ "$command_name" = docker ] && [ "${CI:-}" != true ]; then
      echo "reuse-lint: Docker unavailable -- skipping local convenience check (CI is authoritative)."
      exit 0
    fi
    echo "reuse-lint: required command is unavailable: $command_name" >&2
    exit 1
  }
done

if ! docker info >/dev/null 2>&1; then
  if [ "${CI:-}" = "true" ]; then
    echo "reuse-lint: Docker is required in CI" >&2
    exit 1
  fi
  echo "reuse-lint: Docker unavailable -- skipping local convenience check (CI is authoritative)."
  exit 0
fi

# A linked Windows worktree stores an absolute host path in its `.git` file.
# That path is invalid inside a Linux container, so REUSE cannot apply Git
# ignores and may recursively scan local node_modules. Build the verification
# input from Git's current public-candidate set instead: tracked files plus
# non-ignored untracked files, with missing working-tree files excluded. This
# also avoids mounting private Git history into the verification container.
task_tmp_base="${TMPDIR:-/tmp}"
task_tmp_base="$(cd "$task_tmp_base" && pwd -P)"
snapshot="$(mktemp -d "$task_tmp_base/perfectory-reuse.XXXXXX")"
# The official REUSE image runs as a non-root user. `mktemp -d` creates mode
# 0700, which makes the read-only bind unreadable inside that container on
# Linux CI. Keep the snapshot immutable but grant traversal/read permission.
chmod 755 "$snapshot"
cleanup() {
  case "${snapshot:-}" in
    "$task_tmp_base"/perfectory-reuse.*)
      [ ! -e "$snapshot" ] || rm -rf -- "$snapshot"
      ;;
    *)
      echo "reuse-lint: refusing to remove unexpected temporary path: ${snapshot:-<unset>}" >&2
      ;;
  esac
}
trap cleanup EXIT

bash scripts/ci/materialize-public-candidate-tree.sh . "$snapshot" >/dev/null

snapshot_mount="$(cd "$snapshot" && pwd -W 2>/dev/null || pwd -P)"
MSYS_NO_PATHCONV=1 docker run --rm \
  --network none \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  -v "$snapshot_mount":/data:ro \
  -w /data \
  "$REUSE_IMAGE" lint
