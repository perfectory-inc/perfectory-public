#!/usr/bin/env bash
# Prevents: per-area compiler drift (observed 1.91/1.95/1.96 three-way split) and
# silent shadowing of the root pin — rustup prefers the CLOSEST rust-toolchain file,
# so an area-local file would override the root SSOT without any error.
# Pin lives in the root rust-toolchain.toml (ADR-0001 §3).
set -euo pipefail
cd "$(dirname "$0")/../.."
PIN="1.96.0"
fail=0
ch=$(grep -oP 'channel *= *"\K[^"]+' rust-toolchain.toml)
if [ "$ch" != "$PIN" ]; then
  echo "FAIL toolchain: root rust-toolchain.toml pins $ch, ADR-0001 pin is $PIN" >&2; fail=1
fi
# Area-local toolchain files (incl. extensionless legacy form, which outranks .toml)
stray=$(find products platforms -maxdepth 2 \( -name rust-toolchain.toml -o -name rust-toolchain \) -not -path "*/node_modules/*" 2>/dev/null || true)
if [ -n "$stray" ]; then
  echo "FAIL toolchain: area-local toolchain files shadow the root SSOT:" >&2
  echo "$stray" >&2; fail=1
fi
# Docker base images must match the pin (area build contexts cannot COPY the root file).
# Scan the tracked SSOT instead of recursively walking dependency/build directories.
mismatch=$(git grep -n "FROM rust:" -- \
  ':(glob)products/**/Dockerfile*' \
  ':(glob)platforms/**/Dockerfile*' \
  2>/dev/null | grep -v "rust:$PIN-" || true)
if [ -n "$mismatch" ]; then
  echo "FAIL toolchain: Dockerfile rust base images must pin rust:$PIN-*:" >&2
  echo "$mismatch" >&2; fail=1
fi
[ "$fail" -eq 0 ] && echo "OK toolchain-consistency"
exit "$fail"
