#!/usr/bin/env bash
# Negative fixtures prove that normal named-step YAML cannot bypass the Action,
# checkout, required-context, or terminal-job policy parser.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/check-workflow-policy.sh"
if [ ! -x "$checker" ]; then
  echo "FAIL workflow-policy-self-test: missing executable $checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

mkdir -p "$test_root/valid"
cat >"$test_root/ruleset.json" <<'JSON'
{
  "rules": [
    {
      "type": "required_status_checks",
      "parameters": {
        "required_status_checks": [
          {"context": "required/example", "integration_id": 15368}
        ]
      }
    }
  ]
}
JSON
cat >"$test_root/actions.json" <<'JSON'
{
  "github_owned_allowed": true,
  "verified_allowed": false,
  "patterns_allowed": [
    "example/tool@1111111111111111111111111111111111111111"
  ]
}
JSON
cat >"$test_root/valid/example.yml" <<'YAML'
name: synthetic
on:
  pull_request:
    branches: [main]
  merge_group:
  push:
    branches: [main]
permissions:
  contents: read
jobs:
  build:
    name: Build
    runs-on: ubuntu-24.04
    steps:
      - name: Checkout
        uses: actions/checkout@2222222222222222222222222222222222222222
        with:
          persist-credentials: false
      - name: Tool
        uses: example/tool@1111111111111111111111111111111111111111
  test:
    name: Test
    runs-on: windows-2022
    steps:
      - run: echo synthetic
  required:
    name: required/example
    if: ${{ always() }}
    needs: [build, test]
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@2222222222222222222222222222222222222222
        with:
          persist-credentials: false
      - name: Require every gate
        env:
          REQUIRED_RESULT_BUILD: ${{ needs.build.result }}
          REQUIRED_RESULT_TEST: ${{ needs.test.result }}
        working-directory: .
        run: bash scripts/ci/require-successful-needs.sh
YAML

"$checker" "$test_root/valid" "$test_root/ruleset.json" "$test_root/actions.json" >/dev/null

expect_rejected() {
  local label="$1"
  local fixture="$2"
  if "$checker" "$fixture" "$test_root/ruleset.json" "$test_root/actions.json" \
    --semantic-self-test >/dev/null 2>&1; then
    echo "FAIL workflow-policy-self-test: accepted $label bypass" >&2
    exit 1
  fi
}

mkdir "$test_root/untrusted-runner"
sed 's/runs-on: windows-2022/runs-on: macos-15/' \
  "$test_root/valid/example.yml" >"$test_root/untrusted-runner/example.yml"
expect_rejected untrusted-runner "$test_root/untrusted-runner"

mkdir "$test_root/actionlint-duplicate"
sed '0,/    runs-on: ubuntu-24.04/{s/    runs-on: ubuntu-24.04/    runs-on: ubuntu-24.04\n    runs-on: self-hosted/}' \
  "$test_root/valid/example.yml" >"$test_root/actionlint-duplicate/example.yml"
if scripts/ci/actionlint.sh "$test_root/actionlint-duplicate" >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: actionlint accepted a duplicate runner key" >&2
  exit 1
fi

mkdir "$test_root/unsafe-checkout"
sed '/persist-credentials: false/d' "$test_root/valid/example.yml" \
  >"$test_root/unsafe-checkout/example.yml"
expect_rejected named-step-checkout "$test_root/unsafe-checkout"

mkdir "$test_root/mutable-action"
sed 's#example/tool@1111111111111111111111111111111111111111#example/tool@v1#' \
  "$test_root/valid/example.yml" >"$test_root/mutable-action/example.yml"
expect_rejected named-step-mutable-action "$test_root/mutable-action"

mkdir "$test_root/step-name-spoof"
sed 's/^    name: required\/example$/    name: Aggregate/; s/^      - name: Require every gate$/      - name: required\/example/' \
  "$test_root/valid/example.yml" >"$test_root/step-name-spoof/example.yml"
expect_rejected required-step-name-spoof "$test_root/step-name-spoof"

# A scoped workflow (root ADR-0201): the terminal step carries the scope answer
# exactly when a `scope` job exists, and never in place of a result mapping.
mkdir "$test_root/scoped"
sed 's/^  build:$/  scope:\n    name: Change scope\n    runs-on: ubuntu-24.04\n    steps:\n      - run: echo synthetic\n  build:/; s/needs: \[build, test\]/needs: [scope, build, test]/; s/^          REQUIRED_RESULT_BUILD:/          CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.run }}\n          REQUIRED_RESULT_SCOPE: ${{ needs.scope.result }}\n          REQUIRED_RESULT_BUILD:/' \
  "$test_root/valid/example.yml" >"$test_root/scoped/example.yml"
"$checker" "$test_root/scoped" "$test_root/ruleset.json" "$test_root/actions.json" \
  --semantic-self-test >/dev/null || {
  echo "FAIL workflow-policy-self-test: rejected a valid scoped workflow" >&2
  exit 1
}
mkdir "$test_root/scoped-without-answer"
sed '/CHANGE_SCOPE_RUN:/d' "$test_root/scoped/example.yml" >"$test_root/scoped-without-answer/example.yml"
expect_rejected scoped-terminal-without-scope-answer "$test_root/scoped-without-answer"
mkdir "$test_root/answer-without-scope"
sed 's/^          REQUIRED_RESULT_BUILD:/          CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.run }}\n          REQUIRED_RESULT_BUILD:/' \
  "$test_root/valid/example.yml" >"$test_root/answer-without-scope/example.yml"
expect_rejected scope-answer-without-scope-job "$test_root/answer-without-scope"
mkdir "$test_root/scope-answer-forged"
sed 's/CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.run }}/CHANGE_SCOPE_RUN: false/' \
  "$test_root/scoped/example.yml" >"$test_root/scope-answer-forged/example.yml"
expect_rejected forged-scope-answer "$test_root/scope-answer-forged"

mkdir "$test_root/incomplete-terminal"
sed 's/needs: \[build, test\]/needs: [build]/' "$test_root/valid/example.yml" \
  >"$test_root/incomplete-terminal/example.yml"
expect_rejected incomplete-terminal-needs "$test_root/incomplete-terminal"

mkdir "$test_root/unchecked-result"
sed '/REQUIRED_RESULT_TEST:.*needs\.test\.result/d' \
  "$test_root/valid/example.yml" >"$test_root/unchecked-result/example.yml"
expect_rejected unchecked-terminal-result "$test_root/unchecked-result"

mkdir "$test_root/job-write-permission"
sed '0,/    runs-on: ubuntu-24.04/{s/    runs-on: ubuntu-24.04/    runs-on: ubuntu-24.04\n    permissions:\n      contents: write/}' \
  "$test_root/valid/example.yml" >"$test_root/job-write-permission/example.yml"
expect_rejected job-write-permission "$test_root/job-write-permission"

mkdir "$test_root/multiline-self-hosted"
sed '0,/    runs-on: ubuntu-24.04/{s/    runs-on: ubuntu-24.04/    runs-on:\n      - self-hosted/}' \
  "$test_root/valid/example.yml" >"$test_root/multiline-self-hosted/example.yml"
expect_rejected multiline-self-hosted "$test_root/multiline-self-hosted"

mkdir "$test_root/bracket-secret"
sed '0,/echo synthetic/{s/echo synthetic/echo "${{ secrets['\''TOKEN'\''] }}"/}' \
  "$test_root/valid/example.yml" >"$test_root/bracket-secret/example.yml"
expect_rejected bracket-secret-reference "$test_root/bracket-secret"

mkdir "$test_root/repository-variable"
sed '0,/echo synthetic/{s/echo synthetic/echo "${{ vars.PRIVATE_BINDING }}"/}' \
  "$test_root/valid/example.yml" >"$test_root/repository-variable/example.yml"
expect_rejected repository-variable-reference "$test_root/repository-variable"

mkdir "$test_root/yaml-extension"
sed 's#example/tool@1111111111111111111111111111111111111111#example/tool@v1#' \
  "$test_root/valid/example.yml" >"$test_root/yaml-extension/example.yaml"
expect_rejected yaml-extension "$test_root/yaml-extension"

mkdir "$test_root/quoted-job"
sed 's/^  build:$/  "build":/' "$test_root/valid/example.yml" \
  >"$test_root/quoted-job/example.yml"
expect_rejected quoted-job-id "$test_root/quoted-job"

mkdir "$test_root/spaced-job-id"
sed '/^  build:$/i\  hidden :\n    name: Hidden\n    runs-on: self-hosted\n    steps:\n      - run: echo hidden' \
  "$test_root/valid/example.yml" >"$test_root/spaced-job-id/example.yml"
expect_rejected spaced-job-id "$test_root/spaced-job-id"

mkdir "$test_root/explicit-job-key"
sed '/^  build:$/i\  ? hidden\n  :\n    name: Hidden\n    runs-on: self-hosted\n    steps:\n      - run: echo hidden' \
  "$test_root/valid/example.yml" >"$test_root/explicit-job-key/example.yml"
expect_rejected explicit-job-key "$test_root/explicit-job-key"

mkdir "$test_root/flow-job-map"
sed '/^  build:$/i\  hidden: { runs-on: ubuntu-24.04, steps: [{ run: "exit 1" }] }' \
  "$test_root/valid/example.yml" >"$test_root/flow-job-map/example.yml"
expect_rejected flow-job-map "$test_root/flow-job-map"

mkdir "$test_root/inline-step"
sed 's#^      - name: Tool$#      - { uses: example/tool@1111111111111111111111111111111111111111 }#; /uses: example\/tool@1111111111111111111111111111111111111111/d' \
  "$test_root/valid/example.yml" >"$test_root/inline-step/example.yml"
expect_rejected inline-step-map "$test_root/inline-step"

mkdir "$test_root/flow-permissions"
sed '/^permissions:$/,/^jobs:$/c\permissions: {id-token: write}\njobs:' \
  "$test_root/valid/example.yml" >"$test_root/flow-permissions/example.yml"
expect_rejected flow-permissions "$test_root/flow-permissions"

mkdir "$test_root/quoted-write-all"
sed 's/^permissions:$/permissions: "write-all"/' \
  "$test_root/valid/example.yml" >"$test_root/quoted-write-all/example.yml"
expect_rejected quoted-write-all "$test_root/quoted-write-all"

mkdir "$test_root/quoted-content-write"
sed 's/^  contents: read$/  contents: "write"/' \
  "$test_root/valid/example.yml" >"$test_root/quoted-content-write/example.yml"
expect_rejected quoted-content-write "$test_root/quoted-content-write"

mkdir "$test_root/spaced-permissions-colon"
sed 's/^permissions:$/permissions :/' \
  "$test_root/valid/example.yml" >"$test_root/spaced-permissions-colon/example.yml"
expect_rejected spaced-permissions-colon "$test_root/spaced-permissions-colon"

mkdir "$test_root/yaml-anchor"
sed 's/^  contents: read$/  contents: \&read_permission read/; s/^    name: Build$/    name: *read_permission/' \
  "$test_root/valid/example.yml" >"$test_root/yaml-anchor/example.yml"
expect_rejected yaml-anchor-alias "$test_root/yaml-anchor"

mkdir "$test_root/terminal-extra-step"
sed '/      - name: Require every gate/i\      - run: echo bypass' \
  "$test_root/valid/example.yml" >"$test_root/terminal-extra-step/example.yml"
expect_rejected terminal-extra-step "$test_root/terminal-extra-step"

mkdir "$test_root/terminal-extra-command"
sed 's#run: bash scripts/ci/require-successful-needs.sh#run: bash scripts/ci/require-successful-needs.sh || true#' \
  "$test_root/valid/example.yml" >"$test_root/terminal-extra-command/example.yml"
expect_rejected terminal-extra-command "$test_root/terminal-extra-command"

mkdir "$test_root/terminal-missing-working-directory"
sed '/working-directory: \./d' "$test_root/valid/example.yml" \
  >"$test_root/terminal-missing-working-directory/example.yml"
expect_rejected terminal-missing-working-directory "$test_root/terminal-missing-working-directory"

mkdir "$test_root/terminal-non-root-working-directory"
sed 's/working-directory: \./working-directory: products\/gongzzang/' \
  "$test_root/valid/example.yml" >"$test_root/terminal-non-root-working-directory/example.yml"
expect_rejected terminal-non-root-working-directory "$test_root/terminal-non-root-working-directory"

mkdir "$test_root/terminal-if"
sed '/      - name: Require every gate/a\        if: ${{ false }}' \
  "$test_root/valid/example.yml" >"$test_root/terminal-if/example.yml"
expect_rejected terminal-step-condition "$test_root/terminal-if"

mkdir "$test_root/upstream-continue"
sed '0,/    runs-on: ubuntu-24.04/{s/    runs-on: ubuntu-24.04/    runs-on: ubuntu-24.04\n    continue-on-error: true/}' \
  "$test_root/valid/example.yml" >"$test_root/upstream-continue/example.yml"
expect_rejected upstream-continue-on-error "$test_root/upstream-continue"

mkdir "$test_root/inline-continue"
sed '0,/      - run: echo synthetic/{s/      - run: echo synthetic/      - continue-on-error: true\n        run: echo synthetic/}' \
  "$test_root/valid/example.yml" >"$test_root/inline-continue/example.yml"
expect_rejected inline-continue-on-error "$test_root/inline-continue"

mkdir "$test_root/inline-step-if"
sed '0,/      - run: echo synthetic/{s/      - run: echo synthetic/      - if: false\n        run: echo synthetic/}' \
  "$test_root/valid/example.yml" >"$test_root/inline-step-if/example.yml"
expect_rejected inline-step-condition "$test_root/inline-step-if"

mkdir "$test_root/spaced-continue-colon"
sed '0,/      - run: echo synthetic/{s/      - run: echo synthetic/      - continue-on-error : true\n        run: echo synthetic/}' \
  "$test_root/valid/example.yml" >"$test_root/spaced-continue-colon/example.yml"
expect_rejected spaced-continue-colon "$test_root/spaced-continue-colon"

mkdir "$test_root/spaced-if-colon"
sed '0,/      - run: echo synthetic/{s/      - run: echo synthetic/      - if : false\n        run: echo synthetic/}' \
  "$test_root/valid/example.yml" >"$test_root/spaced-if-colon/example.yml"
expect_rejected spaced-if-colon "$test_root/spaced-if-colon"

mkdir "$test_root/checkout-env-spoof"
sed 's/^        with:$/        env:/' "$test_root/valid/example.yml" \
  >"$test_root/checkout-env-spoof/example.yml"
expect_rejected checkout-env-spoof "$test_root/checkout-env-spoof"

mkdir "$test_root/no-pull-request"
sed '/^  pull_request:$/,+1d' "$test_root/valid/example.yml" \
  >"$test_root/no-pull-request/example.yml"
expect_rejected missing-pull-request "$test_root/no-pull-request"

mkdir "$test_root/pull-path-filter"
sed '/^    branches: \[main\]$/a\    paths: ["src/**"]' \
  "$test_root/valid/example.yml" >"$test_root/pull-path-filter/example.yml"
expect_rejected pull-request-path-filter "$test_root/pull-path-filter"

# A required context that does not report on merge_group leaves a merge queue
# waiting forever, and a filtered one does the same for the groups it skips.
mkdir "$test_root/no-merge-group"
sed '/^  merge_group:$/d' "$test_root/valid/example.yml" \
  >"$test_root/no-merge-group/example.yml"
expect_rejected missing-merge-group "$test_root/no-merge-group"

mkdir "$test_root/merge-group-filter"
sed '/^  merge_group:$/a\    types: [checks_requested]' \
  "$test_root/valid/example.yml" >"$test_root/merge-group-filter/example.yml"
expect_rejected merge-group-filter "$test_root/merge-group-filter"

# The Foundation scope-gate condition is admitted only on the shared Rust setup
# and its cache; on any other step it would be a silent skip.
gate_if="        if: \${{ env.FOUNDATION_CI_GATE_SELECTED == 'true' }}"
gate_step() {
  awk -v name="$1" -v gate="$gate_if" \
    '$0 == "      - name: Tool" { print "      - name: " name; print gate; next } { print }' \
    "$test_root/valid/example.yml"
}
mkdir "$test_root/gated-rust-setup"
gate_step "Set up Rust" >"$test_root/gated-rust-setup/example.yml"
grep -q 'FOUNDATION_CI_GATE_SELECTED' "$test_root/gated-rust-setup/example.yml"
if ! "$checker" "$test_root/gated-rust-setup" "$test_root/ruleset.json" "$test_root/actions.json" \
  --semantic-self-test >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: rejected the gated shared Rust setup" >&2
  exit 1
fi
mkdir "$test_root/gated-other-step"
gate_step "Tool" >"$test_root/gated-other-step/example.yml"
grep -q 'FOUNDATION_CI_GATE_SELECTED' "$test_root/gated-other-step/example.yml"
expect_rejected gate-condition-on-a-gate-step "$test_root/gated-other-step"

# The workspace build is saved only from main (root ADR-0201): the main-only
# condition is admitted on that step, a wider one or the same one on a gate is not.
named_if_step() {
  awk -v name="$1" -v condition="        if: $2" \
    '$0 == "      - name: Tool" { print "      - name: " name; print condition; next } { print }' \
    "$test_root/valid/example.yml"
}
main_only="\${{ github.ref == 'refs/heads/main' }}"
mkdir "$test_root/main-only-save" "$test_root/pull-request-save" "$test_root/main-only-gate"
named_if_step "Save workspace build" "$main_only" >"$test_root/main-only-save/example.yml"
named_if_step "Save workspace build" "always()" >"$test_root/pull-request-save/example.yml"
named_if_step "Tool" "$main_only" >"$test_root/main-only-gate/example.yml"
if ! "$checker" "$test_root/main-only-save" "$test_root/ruleset.json" "$test_root/actions.json" \
  --semantic-self-test >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: rejected the main-only workspace build save" >&2
  exit 1
fi
expect_rejected workspace-build-saved-from-every-ref "$test_root/pull-request-save"
expect_rejected main-only-condition-on-a-gate-step "$test_root/main-only-gate"

mkdir "$test_root/pull-types-filter"
sed '/^    branches: \[main\]$/a\    types: [opened]' \
  "$test_root/valid/example.yml" >"$test_root/pull-types-filter/example.yml"
expect_rejected pull-request-types-filter "$test_root/pull-types-filter"

mkdir "$test_root/local-action"
sed 's#example/tool@1111111111111111111111111111111111111111#./.github/actions/local#' \
  "$test_root/valid/example.yml" >"$test_root/local-action/example.yml"
expect_rejected local-action "$test_root/local-action"

mkdir "$test_root/spaced-uses-colon"
sed '0,/        uses: example\/tool/{s/        uses:/        uses :/}' \
  "$test_root/valid/example.yml" >"$test_root/spaced-uses-colon/example.yml"
expect_rejected spaced-uses-colon "$test_root/spaced-uses-colon"

mkdir "$test_root/job-container"
sed '0,/    runs-on: ubuntu-24.04/{s#    runs-on: ubuntu-24.04#    runs-on: ubuntu-24.04\n    container: ubuntu:latest#}' \
  "$test_root/valid/example.yml" >"$test_root/job-container/example.yml"
expect_rejected mutable-job-container "$test_root/job-container"

mkdir "$test_root/spaced-container-colon"
sed '0,/    runs-on: ubuntu-24.04/{s#    runs-on: ubuntu-24.04#    runs-on: ubuntu-24.04\n    container : ubuntu:latest#}' \
  "$test_root/valid/example.yml" >"$test_root/spaced-container-colon/example.yml"
expect_rejected spaced-container-colon "$test_root/spaced-container-colon"

mkdir "$test_root/tagged-permissions"
sed 's/^  contents: read$/  contents: !!str read/' \
  "$test_root/valid/example.yml" >"$test_root/tagged-permissions/example.yml"
expect_rejected tagged-permission-scalar "$test_root/tagged-permissions"

mkdir "$test_root/unsafe-defaults"
sed '/^jobs:$/i\defaults:\n  run:\n    shell: bash' \
  "$test_root/valid/example.yml" >"$test_root/unsafe-defaults/example.yml"
expect_rejected unsafe-defaults "$test_root/unsafe-defaults"

mkdir "$test_root/mutable-service"
sed '0,/    runs-on: ubuntu-24.04/{s#    runs-on: ubuntu-24.04#    runs-on: ubuntu-24.04\n    services:\n      postgres:\n        image: postgres:17#}' \
  "$test_root/valid/example.yml" >"$test_root/mutable-service/example.yml"
expect_rejected mutable-service-image "$test_root/mutable-service"

mkdir "$test_root/single-required-conditional"
cat >"$test_root/single-required-conditional/example.yml" <<'YAML'
name: synthetic-single
on:
  pull_request:
    branches: [main]
permissions:
  contents: read
jobs:
  only:
    name: required/example
    if: false
    runs-on: ubuntu-24.04
    steps:
      - name: Tool
        uses: example/tool@1111111111111111111111111111111111111111
YAML
expect_rejected single-required-job-condition "$test_root/single-required-conditional"

# A schedule-only report workflow (root ADR-0203) is admitted without a PR trigger or
# required context, but stays under every other rule, and any extra trigger or a
# required context puts it back under the required shape.
scheduled_fixture() {
  mkdir "$test_root/$1"
  cp "$test_root/valid/example.yml" "$test_root/$1/example.yml"
  cat >"$test_root/$1/report.yml" <<'YAML'
name: synthetic-report
on:
  schedule:
    - cron: "0 0 * * 1"
  workflow_dispatch:
permissions:
  contents: read
jobs:
  report:
    name: Report
    runs-on: ubuntu-24.04
    steps:
      - name: Tool
        uses: example/tool@1111111111111111111111111111111111111111
YAML
}
scheduled_fixture scheduled-report
if ! "$checker" "$test_root/scheduled-report" "$test_root/ruleset.json" "$test_root/actions.json" \
  --semantic-self-test >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: rejected a schedule-only report workflow" >&2
  exit 1
fi
scheduled_fixture scheduled-required
sed -i 's/^    name: Report$/    name: required\/example/' "$test_root/scheduled-required/report.yml"
expect_rejected scheduled-required-context "$test_root/scheduled-required"
# The duplicate context alone would also fail; require the schedule rule to be the reason.
scheduled_required_error="$("$checker" "$test_root/scheduled-required" "$test_root/ruleset.json" \
  "$test_root/actions.json" --semantic-self-test 2>&1 >/dev/null || true)"
if [[ "$scheduled_required_error" != *'schedule-only workflow must not define a required'* ]]; then
  echo "FAIL workflow-policy-self-test: a schedule-only required context was not named" >&2
  exit 1
fi
scheduled_fixture scheduled-push
sed -i '/^  workflow_dispatch:$/a\  push:\n    branches: [main]' "$test_root/scheduled-push/report.yml"
expect_rejected scheduled-with-push-trigger "$test_root/scheduled-push"
scheduled_fixture scheduled-runner
sed -i 's/runs-on: ubuntu-24.04/runs-on: self-hosted/' "$test_root/scheduled-runner/report.yml"
expect_rejected scheduled-untrusted-runner "$test_root/scheduled-runner"
scheduled_fixture scheduled-write
sed -i 's/^  contents: read$/  contents: write/' "$test_root/scheduled-write/report.yml"
expect_rejected scheduled-write-permission "$test_root/scheduled-write"

# The shared terminal helper itself is fail closed: at least one result is
# required and every mapped dependency must have succeeded.
REQUIRED_RESULT_BUILD=success REQUIRED_RESULT_TEST=success \
  scripts/ci/require-successful-needs.sh >/dev/null
if scripts/ci/require-successful-needs.sh >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: terminal helper accepted no dependencies" >&2
  exit 1
fi
if REQUIRED_RESULT_BUILD=success REQUIRED_RESULT_TEST=failure \
  scripts/ci/require-successful-needs.sh >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: terminal helper accepted a failed dependency" >&2
  exit 1
fi
if REQUIRED_RESULT_BUILD=success REQUIRED_RESULT_TEST=skipped \
  scripts/ci/require-successful-needs.sh >/dev/null 2>&1; then
  echo "FAIL workflow-policy-self-test: terminal helper accepted a skipped dependency" >&2
  exit 1
fi

# A scoped workflow (root ADR-0201) may skip its gated jobs only when its scope
# job succeeded and answered an explicit `false`.
scoped_ok() {
  env "$@" scripts/ci/require-successful-needs.sh >/dev/null 2>&1
}
scoped_ok CHANGE_SCOPE_RUN=false REQUIRED_RESULT_SCOPE=success REQUIRED_RESULT_BUILD=skipped || {
  echo "FAIL workflow-policy-self-test: terminal helper refused an untouched scoped area" >&2
  exit 1
}
for refused in   "CHANGE_SCOPE_RUN=true REQUIRED_RESULT_SCOPE=success REQUIRED_RESULT_BUILD=skipped"   "CHANGE_SCOPE_RUN=false REQUIRED_RESULT_SCOPE=failure REQUIRED_RESULT_BUILD=skipped"   "CHANGE_SCOPE_RUN=false REQUIRED_RESULT_SCOPE=skipped REQUIRED_RESULT_BUILD=skipped"   "CHANGE_SCOPE_RUN= REQUIRED_RESULT_SCOPE=success REQUIRED_RESULT_BUILD=skipped"   "CHANGE_SCOPE_RUN=false REQUIRED_RESULT_SCOPE=success REQUIRED_RESULT_BUILD=failure"   "CHANGE_SCOPE_RUN=false REQUIRED_RESULT_BUILD=skipped"; do
  # shellcheck disable=SC2086 # each case is a list of NAME=value words
  if scoped_ok $refused; then
    echo "FAIL workflow-policy-self-test: terminal helper accepted: $refused" >&2
    exit 1
  fi
done

echo "OK workflow-policy-self-test"
