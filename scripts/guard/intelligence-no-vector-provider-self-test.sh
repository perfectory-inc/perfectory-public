#!/usr/bin/env bash
# intelligence-no-vector-provider.sh가 실제 위반을 거부하는지 증명한다.
# 통과만 본 검사는 검사가 아니다.
set -euo pipefail

GUARD="$(dirname "$0")/intelligence-no-vector-provider.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/clean"
cat > "$WORK/clean/Cargo.toml" <<'TOML'
[package]
name = "clean"

[dependencies]
sqlx = "0.8"
# qdrant 는 주석일 뿐이며 결정이 아니다
TOML

mkdir -p "$WORK/dirty"
cat > "$WORK/dirty/Cargo.toml" <<'TOML'
[package]
name = "dirty"

[dependencies]
qdrant-client = "1"
TOML

mkdir -p "$WORK/dev-dirty"
cat > "$WORK/dev-dirty/Cargo.toml" <<'TOML'
[package]
name = "dev-dirty"

[dev-dependencies]
tantivy = "0.22"
TOML

if ! bash "$GUARD" "$WORK/clean" >/dev/null; then
  echo "SELF-TEST FAIL: guard rejected a clean tree" >&2
  exit 1
fi

for case in dirty dev-dirty; do
  set +e
  bash "$GUARD" "$WORK/$case" >/dev/null 2>&1
  status=$?
  set -e
  if [ "$status" -ne 2 ]; then
    echo "SELF-TEST FAIL: guard returned $status for $case, expected 2" >&2
    exit 1
  fi
done

echo "SELF-TEST OK: guard accepts a mention in prose and rejects pinned dependencies"
