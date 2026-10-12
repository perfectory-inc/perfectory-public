#!/usr/bin/env bash
# Proves the pinned-downloads guard refuses each shape it exists to stop (root ADR-0197). The
# fetcher's own retry/cache/checksum tests run in the repository workflow (test_fetch_pinned.py).
set -euo pipefail
# The fixture is a real repository (the guard reads `git ls-files`); the library removes the
# repository binding a hook hands its children and confines git to the fixture.
. "$(dirname "$0")/lib/fixture-repo.sh"
cd "$(dirname "$0")/../.."
repo_root="$(pwd)"

fixture_root
fixture="$FIXTURE_ROOT"
trap 'rm -rf -- "$fixture"' EXIT

restore_step="$(python3 - <<'PY'
import importlib.util
spec = importlib.util.spec_from_file_location("guard", "scripts/guard/check-pinned-downloads.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
print(guard.RESTORE_STEP, end="")
PY
)"

# A fixture tree with one direct caller of the fetcher and one workflow that is correct.
make_fixture() {
  rm -rf -- "$fixture/repo"
  mkdir -p "$fixture/repo/tools" "$fixture/repo/scripts/ci" "$fixture/repo/.github/workflows"
  cp tools/pinned-artifacts.json "$fixture/repo/tools/"
  printf '#!/usr/bin/env bash\npython3 scripts/ci/fetch_pinned.py tool demo --output out\n' \
    >"$fixture/repo/scripts/ci/install-demo.sh"
  {
    printf 'name: demo\non:\n  pull_request:\njobs:\n  demo:\n    runs-on: ubuntu-24.04\n    steps:\n'
    printf '      - uses: actions/checkout@0000000000000000000000000000000000000000\n'
    printf '        with:\n          persist-credentials: false\n'
    printf '%s\n' "$restore_step"
    printf '      - run: bash scripts/ci/install-demo.sh\n'
    printf '      - run: cargo xtask verify demo\n'
  } >"$fixture/repo/.github/workflows/demo.yml"
}

run_guard() {
  fixture_git "$fixture/repo" init -q
  fixture_git "$fixture/repo" add -A
  python3 "$repo_root/scripts/guard/check-pinned-downloads.py" --root "$fixture/repo" \
    >"$fixture/out" 2>&1
}

expect_refused() {
  local label="$1" pattern="$2"
  if run_guard; then
    echo "FAIL pinned-downloads-self-test: accepted $label" >&2
    exit 1
  fi
  grep -q -- "$pattern" "$fixture/out" || {
    echo "FAIL pinned-downloads-self-test: $label was refused for another reason:" >&2
    cat "$fixture/out" >&2
    exit 1
  }
  echo "EXPECTED-FAIL $label"
}

make_fixture
run_guard || { echo "FAIL pinned-downloads-self-test: the correct fixture was refused" >&2; cat "$fixture/out" >&2; exit 1; }

make_fixture
sed -i '/Restore pinned release binaries/,/restore-keys:/d' "$fixture/repo/.github/workflows/demo.yml"
expect_refused "a job that downloads without restoring the cache" "without first restoring"

make_fixture
sed -i 's/pinned-release-binaries-v1-\${{ runner.os }}-$/pinned-release-binaries-v2-/' \
  "$fixture/repo/.github/workflows/demo.yml"
expect_refused "a restore step with a drifted key" "differs from the one restore step"

make_fixture
printf 'curl -fsSL -o x https://github.com/acme/tool/releases/download/v1/tool.tar.gz\n' \
  >>"$fixture/repo/scripts/ci/install-demo.sh"
expect_refused "a raw release download" "release asset URL outside"

make_fixture
printf '  late:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: bash scripts/ci/install-demo.sh\n' \
  >>"$fixture/repo/.github/workflows/demo.yml"
expect_refused "a second job that calls a fetching script" "job late reaches the pinned fetcher"

echo "OK pinned-downloads-self-test"
