#!/usr/bin/env bash
# Synthetic fixtures prove the completeness guard accepts a fully-declared lane
# table, and rejects a live test that belongs to no lane — whether it is gated by
# `#[ignore]` or by a cargo feature. Both gating styles exist in this repository
# (foundation/identity use `#[ignore]`, gongzzang uses `#![cfg(feature = ...)]`),
# so a guard that understood only one would report completeness it had not checked.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/live-lane-completeness.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL live-lane-completeness-self-test: missing $checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

# One lane table declaring exactly one target.
xtask="$test_root/xtask.rs"
cat >"$xtask" <<'RUST'
const AREAS: &[Area] = &[Area {
    live_lanes: &[LiveLane {
        name: "postgres",
        required_env: &["DATABASE_URL"],
        gating: LaneGating::Ignored,
        targets: &[LaneTarget {
            package: "example-persistence",
            test: "declared_live_reads",
        }],
    }],
}];
RUST

# The same table declaring the OTHER gating style for the same target. Used to
# prove the guard compares the declaration against the source rather than
# accepting whatever the table says.
xtask_feature="$test_root/xtask-feature.rs"
cat >"$xtask_feature" <<'RUST'
const AREAS: &[Area] = &[Area {
    live_lanes: &[LiveLane {
        name: "postgres",
        required_env: &["DATABASE_URL"],
        gating: LaneGating::Feature("integration"),
        targets: &[LaneTarget {
            package: "example-persistence",
            test: "declared_live_reads",
        }],
    }],
}];
RUST

make_crate() {
  local root="$1" pkg="$2"
  mkdir -p "$root/crates/$pkg/tests"
  cat >"$root/crates/$pkg/Cargo.toml" <<TOML
[package]
name = "$pkg"
version = "0.1.0"
TOML
}

expect_accepted() {
  local label="$1" root="$2"
  if ! bash "$checker" "$xtask" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

expect_rejected() {
  local label="$1" root="$2"
  if bash "$checker" "$xtask" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

# --- accepted: the declared target, and a test needing no backend at all -------
ok="$test_root/ok"
make_crate "$ok" example-persistence
cat >"$ok/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
cat >"$ok/crates/example-persistence/tests/pure_unit.rs" <<'RUST'
#[test]
fn parses_without_any_backend() {}
RUST
expect_accepted "declared #[ignore] target plus a backend-free test" "$ok"

# --- rejected: an #[ignore] target missing from the table ---------------------
missing_ignore="$test_root/missing-ignore"
make_crate "$missing_ignore" example-persistence
cat >"$missing_ignore/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
cat >"$missing_ignore/crates/example-persistence/tests/undeclared_live_writes.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn writes_rows() {}
RUST
expect_rejected "#[ignore] target absent from every lane" "$missing_ignore"

# --- rejected: a feature-gated target missing from the table ------------------
# This is the shape gongzzang uses; an #[ignore]-only guard would pass it.
missing_feature="$test_root/missing-feature"
make_crate "$missing_feature" example-persistence
cat >"$missing_feature/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
cat >"$missing_feature/crates/example-persistence/tests/undeclared_feature_gated.rs" <<'RUST'
#![cfg(feature = "integration")]

#[tokio::test]
async fn writes_rows() {}
RUST
expect_rejected "feature-gated target absent from every lane" "$missing_feature"

# --- rejected: the target IS declared, but under the wrong gating -------------
# Membership is not correctness. `Feature("integration")` selects with
# `--features integration` and no `--ignored`; against an `#[ignore]` source that
# selects nothing, and cargo still exits 0. This is the shape that hid twenty
# gongzzang tests, and the run-time count check cannot see it for the five lanes
# that run in no CI job.
expect_rejected_with() {
  local label="$1" table="$2" root="$3"
  if bash "$checker" "$table" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been rejected" >&2
    exit 1
  fi
}
expect_accepted_with() {
  local label="$1" table="$2" root="$3"
  if ! bash "$checker" "$table" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

# Source is `#[ignore]`, table claims Feature(...).
expect_rejected_with "Feature declared over an #[ignore] source" "$xtask_feature" "$ok"

# Source is feature-gated, table claims Ignored — the gongzzang bug exactly.
wrong_ignored="$test_root/wrong-ignored"
make_crate "$wrong_ignored" example-persistence
cat >"$wrong_ignored/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#![cfg(feature = "integration")]

#[tokio::test]
async fn reads_rows() {}
RUST
expect_rejected_with "Ignored declared over a feature-gated source" "$xtask" "$wrong_ignored"

# The matching declaration for that same source must be accepted, or the rule
# would just forbid one style rather than compare the two.
expect_accepted_with "Feature declared over a feature-gated source" "$xtask_feature" "$wrong_ignored"

# --- rejected: a lane naming a target that does not exist --------------------
# A typo in the table is caught by cargo eventually, but only when the lane runs
# — and five lanes never do.
phantom="$test_root/phantom"
make_crate "$phantom" example-persistence
cat >"$phantom/crates/example-persistence/tests/some_other_name.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
expect_rejected_with "lane names a target with no source file" "$xtask" "$phantom"

# --- Python: the same rule, one language over (ADR-0011) ----------------------
# `PythonTests` declares a command, not a target, so nothing could be compared
# against it. Four files (26 tests) sat unrun behind that silence — one of them
# asserting `assertIn(X)` and `assertNotIn(S)` with `S ⊂ X`, so it could never
# have passed had it ever run. `covers` turns the command into a claim, and these
# fixtures prove the claim is actually checked.

# A table whose one suite claims exactly one discovery root.
xtask_python="$test_root/xtask-python.rs"
cat >"$xtask_python" <<'RUST'
const AREAS: &[Area] = &[Area {
    python_tests: &[PythonTests {
        dir: ".",
        python_path: None,
        args: &["-m", "unittest", "discover", "-s", "infra/checks"],
        covers: &["infra/checks"],
    }],
    live_lanes: &[],
}];
RUST

# The same table claiming two roots that both contain the file. Ownership must be
# unambiguous: two runners over one file means neither is responsible for it.
xtask_python_overlap="$test_root/xtask-python-overlap.rs"
cat >"$xtask_python_overlap" <<'RUST'
const AREAS: &[Area] = &[Area {
    python_tests: &[
        PythonTests {
            dir: ".",
            python_path: None,
            args: &["-m", "unittest", "discover", "-s", "infra/checks"],
            covers: &["infra/checks"],
        },
        PythonTests {
            dir: ".",
            python_path: None,
            args: &["-m", "pytest", "infra/checks/deep"],
            covers: &["infra/checks", "infra/checks/deep"],
        },
    ],
    live_lanes: &[],
}];
RUST

# accepted: the discovered file sits under exactly one declared root.
py_ok="$test_root/py-ok"
mkdir -p "$py_ok/infra/checks"
cat >"$py_ok/infra/checks/test_contract.py" <<'PY'
import unittest


class ContractTest(unittest.TestCase):
    def test_holds(self) -> None:
        self.assertTrue(True)
PY
expect_accepted_with "python file under exactly one covers root" "$xtask_python" "$py_ok"

# rejected: a discovered file that no covers root claims. This is the dbt shape.
py_orphan="$test_root/py-orphan"
mkdir -p "$py_orphan/infra/checks" "$py_orphan/infra/unclaimed"
cat >"$py_orphan/infra/checks/test_contract.py" <<'PY'
import unittest


class ContractTest(unittest.TestCase):
    def test_holds(self) -> None:
        self.assertTrue(True)
PY
cat >"$py_orphan/infra/unclaimed/test_forgotten.py" <<'PY'
import unittest


class ForgottenTest(unittest.TestCase):
    def test_never_ran(self) -> None:
        self.assertTrue(True)
PY
expect_rejected_with "python file claimed by no covers root" "$xtask_python" "$py_orphan"

# rejected: two covers roots claim the same file.
py_overlap="$test_root/py-overlap"
mkdir -p "$py_overlap/infra/checks/deep"
cat >"$py_overlap/infra/checks/deep/test_contract.py" <<'PY'
import unittest


class ContractTest(unittest.TestCase):
    def test_holds(self) -> None:
        self.assertTrue(True)
PY
expect_rejected_with "python file claimed by two covers roots" "$xtask_python_overlap" "$py_overlap"

# accepted: the `*_test.py` spelling is discovered too, not just `test_*.py`.
# Recognising one spelling would leave the other invisible, which is the same
# defect wearing a different filename.
py_suffix="$test_root/py-suffix"
mkdir -p "$py_suffix/infra/checks"
cat >"$py_suffix/infra/checks/contract_test.py" <<'PY'
import unittest


class ContractTest(unittest.TestCase):
    def test_holds(self) -> None:
        self.assertTrue(True)
PY
expect_accepted_with "*_test.py spelling under a covers root" "$xtask_python" "$py_suffix"

py_suffix_orphan="$test_root/py-suffix-orphan"
mkdir -p "$py_suffix_orphan/infra/unclaimed"
cat >"$py_suffix_orphan/infra/unclaimed/contract_test.py" <<'PY'
import unittest


class ContractTest(unittest.TestCase):
    def test_holds(self) -> None:
        self.assertTrue(True)
PY
expect_rejected_with "*_test.py spelling claimed by no covers root" "$xtask_python" "$py_suffix_orphan"

# --- workspaces: the layer above the lanes (ADR-0011) -------------------------
# A lane can only be checked inside an area that `verify` actually enters. This
# repository had five Cargo workspaces and four declared areas; the undeclared one
# was tools/xtask — the harness whose own ten tests therefore ran nowhere, among
# them the proof that a zero-test lane is a failure. `slug:` precedes an Area's
# `dir:`, which is how the parser tells it from a PythonTests `dir:`.
xtask_workspace="$test_root/xtask-workspace.rs"
cat >"$xtask_workspace" <<'RUST'
const AREAS: &[Area] = &[Area {
    slug: "example",
    dir: "example-area",
    python_tests: &[PythonTests {
        dir: "nested/runner",
        python_path: None,
        args: &["-m", "unittest", "discover", "-s", "checks"],
        covers: &["example-area/checks"],
    }],
    live_lanes: &[],
}];
RUST

make_workspace() {
  local root="$1" rel="$2"
  mkdir -p "$root/$rel"
  cat >"$root/$rel/Cargo.toml" <<'TOML'
[workspace]
members = []
TOML
}

# accepted: the only workspace present is the declared one.
ws_ok="$test_root/ws-ok"
make_workspace "$ws_ok" example-area
expect_accepted_with "the only workspace is a declared area" "$xtask_workspace" "$ws_ok"

# rejected: a second workspace no Area declares. This is the tools/xtask shape.
ws_orphan="$test_root/ws-orphan"
make_workspace "$ws_orphan" example-area
make_workspace "$ws_orphan" undeclared-tooling
expect_rejected_with "workspace declared by no Area" "$xtask_workspace" "$ws_orphan"

# accepted: a plain `[package]` manifest is not a workspace and needs no Area.
# Confusing the two would demand an Area per crate, which is not the rule.
ws_package="$test_root/ws-package"
make_workspace "$ws_package" example-area
make_crate "$ws_package" example-persistence
expect_accepted_with "a [package] crate is not a workspace" "$xtask_workspace" "$ws_package"

# --- TypeScript: scripts, not files (ADR-0011) --------------------------------
# `pnpm turbo test` sweeps packages that happen to declare a `test` script, so a
# test-shaped script under any other name is selected by nothing. `probe:naver`
# was exactly that. The unit here is the script, and the question is whether some
# workflow invokes it — or whether it is declared local-only with the resource
# that keeps it out, the same sentence `LiveLane.required_env` says for Rust.
ts_workflows="$test_root/workflows"
mkdir -p "$ts_workflows"
cat >"$ts_workflows/frontend.yml" <<'YAML'
jobs:
  web:
    steps:
      - run: pnpm turbo test
      - run: pnpm --filter=@example/web run test:integration
YAML

xtask_local_only="$test_root/xtask-local-only.rs"
cat >"$xtask_local_only" <<'RUST'
const AREAS: &[Area] = &[Area {
    slug: "example",
    dir: "example-area",
    python_tests: &[],
    live_lanes: &[],
}];

const LOCAL_ONLY_SCRIPTS: &[LocalOnlyScript] = &[LocalOnlyScript {
    manifest: "apps/web/package.json",
    script: "probe:hardware",
    requires: "hardware GL",
}];
RUST

make_package_json() {
  local root="$1" rel="$2" scripts="$3"
  mkdir -p "$root/$rel"
  cat >"$root/$rel/package.json" <<JSON
{ "name": "@example/web", "scripts": { $scripts } }
JSON
}

expect_ts_accepted() {
  local label="$1" table="$2" root="$3"
  if ! LIVE_LANE_WORKFLOWS_DIR="$ts_workflows" bash "$checker" "$table" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been accepted" >&2
    exit 1
  fi
}
expect_ts_rejected() {
  local label="$1" table="$2" root="$3"
  if LIVE_LANE_WORKFLOWS_DIR="$ts_workflows" bash "$checker" "$table" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

# accepted: both scripts are invoked by the fixture workflow.
ts_ok="$test_root/ts-ok"
make_workspace "$ts_ok" example-area
make_package_json "$ts_ok" apps/web '"test": "vitest run", "test:integration": "vitest run --config x"'
expect_ts_accepted "npm scripts invoked by a workflow" "$xtask_local_only" "$ts_ok"

# rejected: a test-shaped script no workflow names and no declaration excuses.
ts_orphan="$test_root/ts-orphan"
make_workspace "$ts_orphan" example-area
make_package_json "$ts_orphan" apps/web '"test": "vitest run", "test:forgotten": "vitest run --config y"'
expect_ts_rejected "test-shaped script invoked by nothing" "$xtask_local_only" "$ts_orphan"

# accepted: the same shape, but declared local-only with its reason.
ts_declared="$test_root/ts-declared"
make_workspace "$ts_declared" example-area
make_package_json "$ts_declared" apps/web '"test": "vitest run", "probe:hardware": "playwright test"'
expect_ts_accepted "local-only script declared with its requirement" "$xtask_local_only" "$ts_declared"

# --- what the adversarial pass found (ADR-0011) -------------------------------
# Two of three attacks on the first implementation succeeded. Both are fixed;
# these fixtures are what stops them coming back.

# Attack 1: name the file outside the convention. Discovery was by name, so a real
# `unittest.TestCase` under any other name was invisible to this guard AND to
# `unittest discover -p 'test_*.py'` — it ran nowhere and the guard said OK.
# Discovery is now by content, and the name is checked rather than trusted.
py_misnamed="$test_root/py-misnamed"
mkdir -p "$py_misnamed/infra/checks"
cat >"$py_misnamed/infra/checks/checks_hidden.py" <<'PY'
import unittest


class HiddenTest(unittest.TestCase):
    def test_never_discovered(self) -> None:
        self.assertTrue(True)
PY
expect_rejected_with "test file named outside the discovery convention" "$xtask_python" "$py_misnamed"

# A file with no tests in it is not a test file, whatever it is called. Treating
# every `.py` as a candidate would demand coverage for helpers and fixtures.
py_helper="$test_root/py-helper"
mkdir -p "$py_helper/infra/checks"
cat >"$py_helper/infra/checks/helpers.py" <<'PY'
def build_fixture(value: int) -> int:
    return value * 2
PY
expect_accepted_with "a helper module with no tests needs no coverage" "$xtask_python" "$py_helper"

# Attack 2: put the file outside every scan root. `scripts/catalog/
# test_audit_documentation.py` was exactly that — run by docs.yml, invisible here,
# so deleting that one workflow line would have been silent. `scripts` is scanned
# now, and a file a workflow names directly counts as covered.
py_workflow="$test_root/py-workflow"
mkdir -p "$py_workflow/toolbox"
cat >"$py_workflow/toolbox/test_named_by_workflow.py" <<'PY'
import unittest


class NamedByWorkflowTest(unittest.TestCase):
    def test_runs(self) -> None:
        self.assertTrue(True)
PY
py_workflow_dir="$test_root/py-workflow-wf"
mkdir -p "$py_workflow_dir"
cat >"$py_workflow_dir/docs.yml" <<YAML
jobs:
  docs:
    steps:
      - run: python3 -m unittest $py_workflow/toolbox/test_named_by_workflow.py -v
YAML
if ! LIVE_LANE_WORKFLOWS_DIR="$py_workflow_dir" bash "$checker" "$xtask_python" "$py_workflow" >/dev/null 2>&1; then
  echo "FAIL live-lane-completeness-self-test: a file a workflow names should have been accepted" >&2
  exit 1
fi
if LIVE_LANE_WORKFLOWS_DIR="$ts_workflows" bash "$checker" "$xtask_python" "$py_workflow" >/dev/null 2>&1; then
  echo "FAIL live-lane-completeness-self-test: a file no workflow names should have been rejected" >&2
  exit 1
fi

echo "OK live-lane-completeness-self-test"
