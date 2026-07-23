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
    runs-on: ubuntu-24.04
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
sed '0,/    runs-on: ubuntu-24.04/{s#    runs-on: ubuntu-24.04#    runs-on: ubuntu-24.04\n    services:\n      postgres:\n        image: postgres:16#}' \
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

echo "OK workflow-policy-self-test"
