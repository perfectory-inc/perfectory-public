#!/usr/bin/env bash
# Proves publication verification uses disposable build state while normal local
# verification retains the named caches that keep development runs practical.
set -euo pipefail
cd "$(dirname "$0")/../.."

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/docker" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$DOCKER_ARGUMENT_CAPTURE"
if [ "${DOCKER_VERIFY_BLOB_STORE:-0}" = 1 ]; then
  safe_git_dir=""
  git_metadata=""
  for argument in "$@"; do
    case "$argument" in
      type=bind,source=*,target=/perfectory-git,readonly)
        safe_git_dir="${argument#type=bind,source=}"
        safe_git_dir="${safe_git_dir%,target=/perfectory-git,readonly}"
        ;;
      type=bind,source=*,target=/work/.git,readonly)
        git_metadata="${argument#type=bind,source=}"
        git_metadata="${git_metadata%,target=/work/.git,readonly}"
        ;;
    esac
  done
  [ -n "$safe_git_dir" ] && [ -f "$safe_git_dir/index" ] || exit 81
  [ -n "$git_metadata" ] || exit 85
  if [ -f "$git_metadata" ]; then
    [ "$(cat "$git_metadata")" = "gitdir: /perfectory-git" ] || exit 86
  elif [ -d "$git_metadata" ]; then
    [ -f "$git_metadata/index" ] || exit 86
    [ "$(git --git-dir="$git_metadata" config --get core.worktree)" = /work ] || exit 89
  else
    exit 85
  fi
  [ "$(git --git-dir="$safe_git_dir" config --get core.bare)" = false ] || exit 87
  [ "$(git --git-dir="$safe_git_dir" config --get core.worktree)" = /work ] || exit 88
  first_blob="$(
    GIT_DIR="$safe_git_dir" git ls-files --stage \
      | awk '($1 == "100644" || $1 == "100755") && first == "" { first = $2 }
             END { print first }'
  )"
  [ -n "$first_blob" ] || exit 82
  GIT_DIR="$safe_git_dir" git cat-file -e "$first_blob^{blob}" || exit 83
  GIT_DIR="$safe_git_dir" GIT_WORK_TREE="$CARGO_VERIFY_REPO" \
    bash "$CARGO_VERIFY_BLOB_CHECKER" >/dev/null || exit 84
fi
SH
chmod +x "$fake_bin/docker"

require_argument() {
  local capture="$1"
  local expected="$2"
  if ! grep -Fqx -- "$expected" "$capture"; then
    echo "FAIL cargo-verify-isolation-self-test: missing Docker argument: $expected" >&2
    exit 1
  fi
}

reject_argument() {
  local capture="$1"
  local rejected="$2"
  if grep -Fqx -- "$rejected" "$capture"; then
    echo "FAIL cargo-verify-isolation-self-test: forbidden Docker argument: $rejected" >&2
    exit 1
  fi
}

clean_capture="$test_root/clean.args"
PATH="$fake_bin:$PATH" \
  DOCKER_ARGUMENT_CAPTURE="$clean_capture" \
  DOCKER_VERIFY_BLOB_STORE=1 \
  CARGO_VERIFY_REPO="$(pwd -W 2>/dev/null || pwd)" \
  CARGO_VERIFY_BLOB_CHECKER="$(pwd)/scripts/guard/check-tracked-blob-sizes.sh" \
  PERFECTORY_CLEAN_VERIFY=1 \
  bash scripts/verify/cargo-verify.sh products/gongzzang

require_argument "$clean_capture" "--security-opt"
require_argument "$clean_capture" "no-new-privileges"
require_argument "$clean_capture" "--pids-limit"
require_argument "$clean_capture" "2048"
reject_argument "$clean_capture" "PERFECTORY_GIT_DIR=/perfectory-git"
require_argument "$clean_capture" "type=volume,target=/usr/local/cargo/registry,volume-nocopy"
require_argument "$clean_capture" "type=volume,target=/work/products/gongzzang/target,volume-nocopy"
require_argument "$clean_capture" "type=volume,target=/work/tools/xtask/target,volume-nocopy"
if ! grep -Eq '^type=bind,source=.+,target=/work$' "$clean_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: clean source bind is missing" >&2
  exit 1
fi
if ! grep -Eq '^type=bind,source=.+,target=/perfectory-git,readonly$' "$clean_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: current-blob Git directory is not read-only" >&2
  exit 1
fi
if ! grep -Eq '^type=bind,source=.+,target=/work/\.git,readonly$' "$clean_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: isolated Git pointer is not mounted over the linked-worktree metadata" >&2
  exit 1
fi
if grep -Fq 'PERFECTORY_GIT_INDEX_FILE' "$clean_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: object-free Git metadata path returned" >&2
  exit 1
fi
if grep -Eq 'perfectory-(cargo-registry|rustup|target-)|target=/usr/local/rustup' "$clean_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: clean verification reused persistent state" >&2
  exit 1
fi
if [ "$(grep -Ec '^type=bind,source=.+,target=/work$' "$clean_capture")" -ne 1 ] \
  || [ "$(grep -Ec '^type=volume,target=(/usr/local/cargo/registry|/work/products/gongzzang/target|/work/tools/xtask/target),volume-nocopy$' "$clean_capture")" -ne 3 ]; then
  echo "FAIL cargo-verify-isolation-self-test: clean mounts are duplicated or ambiguous" >&2
  exit 1
fi
reject_argument "$clean_capture" "perfectory-cargo-registry:/usr/local/cargo/registry"
reject_argument "$clean_capture" "perfectory-rustup:/usr/local/rustup"

normal_capture="$test_root/normal.args"
PATH="$fake_bin:$PATH" \
  DOCKER_ARGUMENT_CAPTURE="$normal_capture" \
  DOCKER_VERIFY_BLOB_STORE=1 \
  CARGO_VERIFY_REPO="$(pwd -W 2>/dev/null || pwd)" \
  CARGO_VERIFY_BLOB_CHECKER="$(pwd)/scripts/guard/check-tracked-blob-sizes.sh" \
  bash scripts/verify/cargo-verify.sh products/gongzzang

require_argument "$normal_capture" "perfectory-cargo-registry:/usr/local/cargo/registry"
require_argument "$normal_capture" "perfectory-rustup:/usr/local/rustup"
require_argument "$normal_capture" "perfectory-target-products-gongzzang:/work/products/gongzzang/target"
require_argument "$normal_capture" "perfectory-target-xtask:/work/tools/xtask/target"
reject_argument "$normal_capture" "PERFECTORY_GIT_DIR=/perfectory-git"
if ! grep -Eq '^type=bind,source=.+,target=/work/\.git,readonly$' "$normal_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: normal verification lacks the isolated Git pointer" >&2
  exit 1
fi
if grep -Eq '^type=bind,source=.+,target=/work,readonly$' "$normal_capture"; then
  echo "FAIL cargo-verify-isolation-self-test: normal verification unexpectedly made the source read-only" >&2
  exit 1
fi

invalid_capture="$test_root/invalid.args"
if PATH="$fake_bin:$PATH" \
  DOCKER_ARGUMENT_CAPTURE="$invalid_capture" \
  PERFECTORY_CLEAN_VERIFY=unexpected \
  bash scripts/verify/cargo-verify.sh products/gongzzang >/dev/null 2>&1; then
  echo "FAIL cargo-verify-isolation-self-test: invalid clean-mode value was accepted" >&2
  exit 1
fi
if [ -e "$invalid_capture" ]; then
  echo "FAIL cargo-verify-isolation-self-test: invalid clean mode reached Docker" >&2
  exit 1
fi

if [ "${PERFECTORY_DOCKER_MOUNT_SMOKE:-0}" = 1 ]; then
  source tools/container-images.env
  repo="$(pwd -W 2>/dev/null || pwd)"
  probe=".perfectory-clean-verify-source-write-probe"
  if [ -e "$probe" ]; then
    echo "FAIL cargo-verify-isolation-self-test: source probe path already exists" >&2
    exit 1
  fi
  container_script='if touch /work/.perfectory-clean-verify-source-write-probe 2>/dev/null; then
    echo "FAIL clean source bind is writable" >&2
    exit 1
  fi
  touch /usr/local/cargo/registry/.probe
  touch /work/products/gongzzang/target/.probe
  touch /work/tools/xtask/target/.probe'
  MSYS_NO_PATHCONV=1 docker run --rm \
    --security-opt no-new-privileges \
    --pids-limit 2048 \
    --mount "type=bind,source=$repo,target=/work,readonly" \
    --mount "type=volume,target=/usr/local/cargo/registry,volume-nocopy" \
    --mount "type=volume,target=/work/products/gongzzang/target,volume-nocopy" \
    --mount "type=volume,target=/work/tools/xtask/target,volume-nocopy" \
    -w /work \
    "$RUST_TOOLCHAIN_IMAGE" bash -ceu "$container_script"
  if [ -e "$probe" ]; then
    echo "FAIL cargo-verify-isolation-self-test: mount smoke mutated source" >&2
    exit 1
  fi
fi

echo "OK cargo-verify-isolation-self-test"
