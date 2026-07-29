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

# `cargo test` is the same rule (ADR-0011). The lane table is the SSOT for WHICH
# targets run; a raw `cargo test` picks targets on its own, so the set the
# completeness guard proves and the set CI executes drift apart silently.
# identity-ci did exactly that: the lane declares two targets and the workflow ran
# one named test inside one of them, which is why `live_provisioning` had never
# executed. `scripts/verify/foundation-kafka-live.sh` kept its own copy of the
# same three target names.
#
# `scripts/` is scanned too — the kafka copy lives outside `.github/`, so a
# workflow-only sweep would have declared the repository clean while a private
# duplicate ran beside the lane. `cargo xtask` is the sanctioned entry point, so
# `cargo-verify.sh` and `integration.sh` do not match.
#
# The pattern requires `cargo` to be the command: word-boundary `cargo`, an
# optional `+toolchain`, then `test` as the subcommand. A loose match on "cargo"
# and "test" anywhere in the line sweeps up filenames like
# `cargo-verify-isolation-self-test.sh` and the guard lists that name them, which
# would push us straight back to maintaining an exclusion list.
#
# Comment lines are dropped. Both this repository's workflows and its guards
# explain the commands they replaced, and a rule that cannot tell an instruction
# from a description of one would forbid documenting the very drift it prevents.
# `grep -rn` emits `file:line:content`, so the filter anchors on the content field.
bad_test=$(grep -rnE '(^|[^[:alnum:]_./-])cargo([[:space:]]+\+[^[:space:]]+)?[[:space:]]+test\b' \
  .github/workflows/ scripts/ 2>/dev/null \
  | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' \
  | grep -v 'cargo xtask' \
  | grep -v '^scripts/guard/no-adhoc-cargo-lint' || true)
if [ -n "$bad_test" ]; then
  echo "FAIL no-adhoc-cargo-lint: 'cargo test' must go through an xtask lane (ADR-0011):" >&2
  echo "$bad_test" >&2
  exit 1
fi

echo "OK no-adhoc-cargo-lint"
