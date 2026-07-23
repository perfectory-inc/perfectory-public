#!/usr/bin/env bash
# Prevents: verification drift. fmt/clippy MUST go through `cargo xtask verify`
# (ADR-0004, the single verification definition). A raw `cargo clippy`/`cargo fmt`
# in a workflow is how the flags drifted across areas and broke local/CI parity.
set -euo pipefail
cd "$(dirname "$0")/../.."
# Match `cargo clippy` / `cargo fmt` that is NOT `cargo xtask ...`.
bad=$(grep -rnE 'cargo[[:space:]]+(clippy|fmt)\b' .github/workflows/ 2>/dev/null | grep -v 'cargo xtask' || true)
if [ -n "$bad" ]; then
  echo "FAIL no-adhoc-cargo-lint: fmt/clippy must go through 'cargo xtask verify' (ADR-0004):" >&2
  echo "$bad" >&2
  exit 1
fi
echo "OK no-adhoc-cargo-lint"
