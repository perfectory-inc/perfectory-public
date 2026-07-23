#!/usr/bin/env bash
# Proves the tree-only importer starts from the live canonical main and cannot
# be fooled by a poisoned local origin/main or an extra destination ancestor.
set -euo pipefail
cd "$(dirname "$0")/../.."

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

private="$test_root/private"
public_seed="$test_root/public-seed"
live="$test_root/live.git"
harness="$test_root/harness"
importer="$harness/scripts/github/import-private-feature-diff.sh"

git init -q --initial-branch=main "$private"
git -C "$private" config user.name "Synthetic Test Author"
git -C "$private" config user.email author@example.invalid
printf 'base\n' >"$private/feature.txt"
git -C "$private" add feature.txt
git -C "$private" commit -q -m base
private_base="$(git -C "$private" rev-parse HEAD)"
printf 'feature\n' >"$private/feature.txt"
git -C "$private" commit -q -am feature
private_feature="$(git -C "$private" rev-parse HEAD)"

git init -q --initial-branch=main "$public_seed"
git -C "$public_seed" config user.name "Synthetic Test Author"
git -C "$public_seed" config user.email author@example.invalid
mkdir -p "$public_seed/scripts/guard" "$public_seed/scripts/ci"
printf 'base\n' >"$public_seed/feature.txt"
printf '#!/usr/bin/env bash\nexit 0\n' >"$public_seed/scripts/guard/monorepo-guard.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"$public_seed/scripts/ci/gitleaks-scan.sh"
git -C "$public_seed" add .
git -C "$public_seed" commit -q -m public-root
live_main="$(git -C "$public_seed" rev-parse HEAD)"
git clone -q --bare "$public_seed" "$live"

mkdir -p "$harness/scripts/github"
cp scripts/github/import-private-feature-diff.sh "$importer"
cp scripts/github/safe-git-transport.sh "$harness/scripts/github/safe-git-transport.sh"
python3 - "$importer" "$live" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
old = 'canonical_remote_url="https://github.com/perfectory-inc/perfectory-public.git"'
new = f"canonical_remote_url={json.dumps(sys.argv[2])}"
if source.count(old) != 1:
    raise SystemExit("importer canonical URL assignment drifted")
path.write_text(source.replace(old, new), encoding="utf-8", newline="\n")
PY
chmod +x "$importer" "$harness/scripts/github/safe-git-transport.sh"

new_destination() {
  local destination="$1"
  git clone -q "$live" "$destination"
  git -C "$destination" remote set-url origin \
    https://github.com/perfectory-inc/perfectory-public.git
  git -C "$destination" switch -q -c feature/import
  git -C "$destination" update-ref refs/remotes/origin/main "$live_main"
  git -C "$destination" config user.name "Synthetic Test Author"
  git -C "$destination" config user.email author@example.invalid
}

valid="$test_root/valid"
new_destination "$valid"
"$importer" "$private" "$private_base" "$private_feature" "$valid" >/dev/null
if ! grep -qx feature "$valid/feature.txt" \
  || git -C "$valid" diff --quiet -- feature.txt; then
  echo "FAIL import-private-feature-diff-self-test: valid tree-only import failed" >&2
  exit 1
fi

poisoned="$test_root/poisoned"
new_destination "$poisoned"
printf 'poisoned local ancestry\n' >"$poisoned/sentinel.txt"
git -C "$poisoned" add sentinel.txt
git -C "$poisoned" commit -q -m poisoned-local-origin
git -C "$poisoned" update-ref refs/remotes/origin/main HEAD
if "$importer" "$private" "$private_base" "$private_feature" "$poisoned" \
  >"$test_root/poisoned.out" 2>&1 \
  || ! grep -q 'local origin/main does not match live canonical main' \
    "$test_root/poisoned.out"; then
  echo "FAIL import-private-feature-diff-self-test: accepted poisoned local origin/main" >&2
  exit 1
fi

extra="$test_root/extra"
new_destination "$extra"
printf 'private-history-sentinel\n' >"$extra/sentinel.txt"
git -C "$extra" add sentinel.txt
git -C "$extra" commit -q -m accidental-private-ancestor
rm "$extra/sentinel.txt"
git -C "$extra" commit -q -am remove-sentinel
if "$importer" "$private" "$private_base" "$private_feature" "$extra" \
  >"$test_root/extra.out" 2>&1 \
  || ! grep -q 'with no extra ancestry' "$test_root/extra.out"; then
  echo "FAIL import-private-feature-diff-self-test: accepted extra destination ancestry" >&2
  exit 1
fi

echo "OK import-private-feature-diff-self-test"
