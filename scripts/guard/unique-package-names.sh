#!/usr/bin/env bash
# Prevents: cross-workspace crate-name collisions (observed: outbox-publisher,
# normalization-domain duplicated) that block any future workspace unification/publishing.
set -euo pipefail
cd "$(dirname "$0")/../.."
# [package]-section-aware: a [[bin]]/[lib] target legitimately reusing its own
# package's name must not count as a duplicate.
dupes=$(git ls-files -z \
  ':(glob)products/**/Cargo.toml' \
  ':(glob)platforms/**/Cargo.toml' \
  | xargs -0r awk -F'"' '{sub(/\r$/,"")} /^\[/{insec=($0=="[package]")} insec && /^name[ \t]*=/{print $2}' \
  | sort | uniq -d)
if [ -n "$dupes" ]; then
  echo "FAIL unique-package-names: duplicated across workspaces:" >&2
  echo "$dupes" >&2
  exit 1
fi
echo "OK unique-package-names"
