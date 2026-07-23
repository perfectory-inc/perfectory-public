#!/usr/bin/env bash
# Proves frontend dependency scripts receive only a disposable candidate-tree
# copy, never the repository worktree or host credential channels.
set -euo pipefail
cd "$(dirname "$0")/../.."

test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) [ ! -e "$test_root" ] || rm -rf -- "$test_root" ;;
    *) echo "frontend-test-isolation-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

mkdir -p "$test_root/bin"
source_repo="$test_root/source"
mkdir -p "$source_repo/products/gongzzang"
git -C "$source_repo" init --quiet
git -C "$source_repo" config user.name Synthetic
git -C "$source_repo" config user.email synthetic@example.invalid
printf '{"name":"synthetic","private":true}\n' \
  >"$source_repo/products/gongzzang/package.json"
git -C "$source_repo" add .
git -C "$source_repo" commit --quiet -m synthetic
capture="$test_root/docker.args"
cat >"$test_root/bin/docker" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$DOCKER_ARGUMENT_CAPTURE"
source_mount="$(printf '%s\n' "$@" | sed -n 's/^type=bind,source=\(.*\),target=\/source,readonly$/\1/p')"
[ -n "$source_mount" ] && [ -f "$source_mount/products/gongzzang/package.json" ]
SH
chmod +x "$test_root/bin/docker"

PATH="$test_root/bin:$PATH" DOCKER_ARGUMENT_CAPTURE="$capture" \
  bash scripts/verify/frontend-test.sh "$source_repo"

for exact in \
  --read-only \
  --security-opt no-new-privileges \
  --pids-limit 512 \
  type=volume,target=/work,volume-nocopy; do
  if ! grep -Fqx -- "$exact" "$capture"; then
    echo "FAIL frontend-test-isolation-self-test: missing Docker argument: $exact" >&2
    exit 1
  fi
done
if ! grep -Eq '^type=bind,source=.+,target=/source,readonly$' "$capture"; then
  echo "FAIL frontend-test-isolation-self-test: candidate source is not read-only" >&2
  exit 1
fi
if grep -Eq 'target=/work,readonly|:/work($|:)' "$capture"; then
  echo "FAIL frontend-test-isolation-self-test: repository was mounted as the work directory" >&2
  exit 1
fi

echo "OK frontend-test-isolation-self-test"
