#!/usr/bin/env bash
# Planted failures for lefthook-time-budget: a heavy step, a heavy step hidden inside an
# invoked script, and a hook-only check with no CI twin must each be rejected.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/lefthook-time-budget.sh"
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT

# Tool names are spelled through variables: the repository's own text guards
# (no-adhoc-cargo-lint, container-runtime-policy) read a literal heavy command in this
# file as a real one.
cargo_cmd=cargo
container_cmd=docker

mkdir -p "$test_root/ci" "$test_root/scripts"
cat >"$test_root/ci/workflow.yml" <<YAML
steps:
  - run: bash scripts/fast-check.sh
  - run: gitleaks git .
  # CI names every heavy plant below, so each one is rejected by the budget rule alone and
  # not by the CI-twin rule.
  - run: $cargo_cmd test && $cargo_cmd build && $container_cmd build . && pnpm turbo && bash monorepo-guard.sh
  - run: bash scripts/hidden-heavy.sh && bash scripts/absent.sh
YAML
printf '#!/usr/bin/env bash\n# %s test and %s are named in prose only.\ngrep -q x README\n' \
  "$cargo_cmd" "$container_cmd" >"$test_root/scripts/fast-check.sh"
printf '#!/usr/bin/env bash\n%s compose run --rm verify\n' "$container_cmd" \
  >"$test_root/scripts/hidden-heavy.sh"

valid="$test_root/valid.yml"
cat >"$valid" <<'YAML'
pre-push:
  parallel: true
  commands:
    secrets:
      run: gitleaks git --pre-commit --staged
    fast:
      run: bash scripts/fast-check.sh
YAML
bash "$checker" "$valid" "$test_root/ci" >/dev/null

expect_rejected() {
  local label="$1" fixture="$2"
  if bash "$checker" "$fixture" "$test_root/ci" >/dev/null 2>&1; then
    echo "FAIL lefthook-time-budget-self-test: accepted $label" >&2
    exit 1
  fi
}

plant() {
  local label="$1" step="$2" fixture="$test_root/$1.yml"
  cp "$valid" "$fixture"
  printf '    planted:\n      run: %s\n' "$step" >>"$fixture"
  expect_rejected "$label" "$fixture"
}

plant cargo-test "$cargo_cmd test --workspace"
plant cargo-build "$cargo_cmd build -p repo-guard"
plant container "$container_cmd run --rm lycheeverse/lychee ."
plant guard-sweep 'bash monorepo-guard.sh'
plant turbo 'pnpm turbo test'
plant hidden-heavy 'bash scripts/hidden-heavy.sh'
plant missing-script 'bash scripts/absent.sh'

cp "$valid" "$test_root/hook-only.yml"
printf '#!/usr/bin/env bash\ngrep -q y README\n' >"$test_root/scripts/hook-only.sh"
printf '    planted:\n      run: bash scripts/hook-only.sh\n' >>"$test_root/hook-only.yml"
expect_rejected hook-only-check "$test_root/hook-only.yml"

echo "OK lefthook-time-budget-self-test"
