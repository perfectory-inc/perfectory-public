#!/usr/bin/env bash
# Synthetic fixtures prove that an installed package-manager wrapper cannot
# make an unavailable hook subtool fail an advisory local hook.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/lefthook-advisory-policy.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL lefthook-advisory-policy-self-test: missing $checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

valid="$test_root/valid.yml"
mkdir -p "$test_root/products/example"
cat >"$valid" <<'YAML'
pre-commit:
  parallel: true
  commands:
    frontend-format:
      root: "products/example/"
      run: pnpm biome check --write {staged_files}
      skip:
        - merge
        - run: "! (cd products/example && pnpm biome --version) >/dev/null 2>&1"
    rust-format:
      root: "products/example/"
      run: cargo fmt -- {staged_files}
      skip:
        - rebase
        - run: "! (cd products/example && cargo fmt --version) >/dev/null 2>&1"
    docs-lint:
      root: "products/example/"
      run: pnpm markdownlint-cli2 {staged_files}
      skip:
        - run: "! (cd products/example && pnpm markdownlint-cli2 --version) >/dev/null 2>&1"
YAML
bash "$checker" "$valid" >/dev/null

expect_rejected() {
  local label="$1"
  local fixture="$2"
  if bash "$checker" "$fixture" >/dev/null 2>&1; then
    echo "FAIL lefthook-advisory-policy-self-test: accepted $label" >&2
    exit 1
  fi
}

wrapper_only="$test_root/wrapper-only.yml"
sed 's|! (cd products/example && pnpm biome --version)|! command -v pnpm|' \
  "$valid" >"$wrapper_only"
expect_rejected wrapper-only-probe "$wrapper_only"

wrong_subtool="$test_root/wrong-subtool.yml"
sed 's|pnpm biome --version|pnpm prettier --version|' "$valid" >"$wrong_subtool"
expect_rejected mismatched-subtool-probe "$wrong_subtool"

missing_probe="$test_root/missing-probe.yml"
sed '/cargo fmt --version/d' "$valid" >"$missing_probe"
expect_rejected missing-subtool-probe "$missing_probe"

wrong_root="$test_root/wrong-root.yml"
sed '0,/cd products\/example/{s|cd products/example|cd products/wrong|}' \
  "$valid" >"$wrong_root"
expect_rejected mismatched-command-root "$wrong_root"

indirect="$test_root/indirect.yml"
sed 's/run: pnpm biome/run: env SYNTHETIC=1 pnpm biome/' "$valid" >"$indirect"
expect_rejected indirect-launcher-bypass "$indirect"

missing_subtool="$test_root/missing-subtool.yml"
sed 's/run: pnpm biome check --write {staged_files}/run: pnpm/' \
  "$valid" >"$missing_subtool"
expect_rejected missing-pnpm-subtool "$missing_subtool"

pnpm_exec="$test_root/pnpm-exec.yml"
sed -e 's/run: pnpm biome/run: pnpm exec biome/' \
  -e 's/pnpm biome --version/pnpm exec --version/' \
  "$valid" >"$pnpm_exec"
expect_rejected ambiguous-pnpm-exec-probe "$pnpm_exec"

cargo_toolchain="$test_root/cargo-toolchain.yml"
sed 's/run: cargo fmt/run: cargo +1.96.0 fmt/' "$valid" >"$cargo_toolchain"
expect_rejected ambiguous-cargo-toolchain-probe "$cargo_toolchain"

duplicate_skip="$test_root/duplicate-skip.yml"
cat >"$duplicate_skip" <<'YAML'
pre-commit:
  commands:
    duplicate-skip:
      root: "products/example/"
      run: pnpm biome check {staged_files}
      skip:
        - run: "! (cd products/example && pnpm biome --version) >/dev/null 2>&1"
      skip:
        - merge
YAML
expect_rejected duplicate-skip-key "$duplicate_skip"

host_cargo_run="$test_root/host-cargo-run.yml"
cp "$valid" "$host_cargo_run"
cat >>"$host_cargo_run" <<'YAML'
    host-build:
      root: "products/example/"
      run: cargo run -p repo-guard -- migration-version-prefixes
      skip:
        - run: "! (cd products/example && cargo --version) >/dev/null 2>&1"
YAML
expect_rejected host-cargo-build "$host_cargo_run"

block_scalar="$test_root/block-scalar.yml"
sed 's/^      run: pnpm biome check --write {staged_files}$/      run: >-\
        pnpm biome check --write {staged_files}/' "$valid" >"$block_scalar"
expect_rejected block-run-scalar "$block_scalar"

jobs_shape="$test_root/jobs-shape.yml"
sed '/^pre-commit:$/a\  jobs:\
    - run: pnpm biome check .' "$valid" >"$jobs_shape"
expect_rejected lefthook-jobs-shape "$jobs_shape"

compound="$test_root/compound.yml"
sed 's/run: pnpm biome check --write {staged_files}/run: pnpm biome check {staged_files} \&\& cargo fmt/' \
  "$valid" >"$compound"
expect_rejected compound-package-tools "$compound"

echo "OK lefthook-advisory-policy-self-test"
