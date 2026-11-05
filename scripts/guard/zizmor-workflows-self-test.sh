#!/usr/bin/env bash
# Proves zizmor-workflows refuses each incident it took over from check-workflow-policy.sh — each
# planted alone in a synthetic workflow — and accepts the clean one.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
fixture="$(mktemp -d)"
trap 'rm -rf -- "$fixture"' EXIT
checkout="actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"

plant() {
  mkdir -p "$fixture/$1"
  cat >"$fixture/$1/t.yml"
}
# A refusal must name the audit that owns the incident; "zizmor failed" for any other reason
# (no inputs, a crash) is not proof.
expect() {
  local verdict="$1" case_name="$2" audit="${3:-}" output
  if output="$(bash scripts/guard/zizmor-workflows.sh "$root" "$fixture/$case_name/t.yml" 2>&1)"; then
    [ "$verdict" = accept ] || { echo "FAIL zizmor-workflows-self-test: accepted $case_name" >&2; exit 1; }
  else
    [ "$verdict" = refuse ] || {
      echo "FAIL zizmor-workflows-self-test: refused $case_name" >&2
      printf '%s\n' "$output" >&2
      exit 1
    }
    grep -q "\[$audit\]" <<<"$output" || {
      echo "FAIL zizmor-workflows-self-test: $case_name failed without the $audit audit" >&2
      printf '%s\n' "$output" >&2
      exit 1
    }
  fi
}
header='name: t
on: pull_request
permissions: {}
concurrency:
  group: t
  cancel-in-progress: true
jobs:
  a:
    name: a
    runs-on: ubuntu-24.04'

plant clean <<EOF2
$header
    steps:
      - uses: $checkout
        with:
          persist-credentials: false
EOF2
plant checkout-persists-credentials <<EOF2
$header
    steps:
      - uses: $checkout
EOF2
plant checkout-credentials-under-env <<EOF2
$header
    steps:
      - uses: $checkout
        env:
          persist-credentials: false
EOF2
plant first-party-action-by-tag <<EOF2
$header
    steps:
      - uses: actions/checkout@v4
        with:
          persist-credentials: false
EOF2
plant third-party-action-by-tag <<EOF2
$header
    steps:
      - uses: example/tool@v1
EOF2
plant docker-action-by-tag <<EOF2
$header
    steps:
      - uses: docker://alpine:3.20
EOF2
plant service-image-by-tag <<EOF2
$header
    services:
      postgres:
        image: postgres:17
    steps:
      - run: echo synthetic
EOF2

expect accept clean
expect refuse checkout-persists-credentials artipacked
expect refuse checkout-credentials-under-env artipacked
expect refuse first-party-action-by-tag unpinned-uses
expect refuse third-party-action-by-tag unpinned-uses
expect refuse docker-action-by-tag unpinned-images
expect refuse service-image-by-tag unpinned-images
echo "OK zizmor-workflows-self-test"
