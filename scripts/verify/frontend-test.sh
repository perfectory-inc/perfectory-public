#!/usr/bin/env bash
# Runs the required Gongzzang frontend unit suite against a disposable copy of
# the current public-candidate tree. Dependency scripts cannot write to the
# repository, read Git history, or reach host credentials/Docker control.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/container-images.env
if [ "$#" -gt 1 ]; then
  echo "FAIL frontend-test: usage: $0 [candidate-source-repository]" >&2
  exit 2
fi
candidate_source="${1:-.}"

for command_name in docker git mktemp realpath; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL frontend-test: missing command '$command_name'" >&2
    exit 1
  }
done

task_tmp_base="${TMPDIR:-/tmp}"
task_tmp_base="$(cd "$task_tmp_base" && pwd -P)"
snapshot="$(mktemp -d "$task_tmp_base/perfectory-frontend.XXXXXX")"
cleanup() {
  case "${snapshot:-}" in
    "$task_tmp_base"/perfectory-frontend.*)
      [ ! -e "$snapshot" ] || rm -rf -- "$snapshot"
      ;;
    *) echo "frontend-test: refusing unsafe temporary cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

bash scripts/ci/materialize-public-candidate-tree.sh "$candidate_source" "$snapshot" >/dev/null
snapshot_mount="$(cd "$snapshot" && { pwd -W 2>/dev/null || pwd -P; })"
MSYS_NO_PATHCONV=1 docker run --rm \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --pids-limit 512 \
  --tmpfs /tmp:rw,nosuid,nodev,size=2g \
  --mount "type=bind,source=$snapshot_mount,target=/source,readonly" \
  --mount "type=volume,target=/work,volume-nocopy" \
  -w / \
  -e CI=true \
  -e HOME=/tmp/home \
  -e XDG_CACHE_HOME=/tmp/cache \
  -e COREPACK_HOME=/tmp/corepack \
  -e PNPM_HOME=/tmp/pnpm \
  -e PNPM_STORE_DIR=/tmp/pnpm-store \
  "$NODE_VERIFY_IMAGE" bash -ceu '
    cp -a /source/. /work/
    mkdir -p "$HOME" "$XDG_CACHE_HOME" "$PNPM_HOME" "$PNPM_STORE_DIR"
    export PATH="$PNPM_HOME:$PATH"
    corepack enable --install-directory "$PNPM_HOME"
    corepack prepare pnpm@9.12.0 --activate
    cd /work/products/gongzzang
    pnpm install --frozen-lockfile --store-dir "$PNPM_STORE_DIR"
    pnpm -C apps/web test
  '
