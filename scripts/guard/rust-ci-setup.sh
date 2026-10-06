#!/usr/bin/env bash
# Prevents: a CI job that runs cargo without the one shared Rust setup (root ADR-0155).
#
# On 2026-10-06 two pull requests failed on "curl failed [16] Error in the HTTP2
# framing layer" while crates.io was fine: the jobs that installed Rust through
# dtolnay/rust-toolchain had CARGO_HTTP_MULTIPLEXING=false as a side effect of
# that action, the ones that called rustup directly did not, and nothing said
# the five setups were one fact. The Foundation jobs also had no cache at all
# and spent 25 of their 30 minutes compiling the same dependencies every run.
#
# Every job whose steps run cargo, rustup or cargo-deny therefore has, in order:
#   1. a step named "Set up Rust" that runs exactly `bash scripts/ci/rust-setup.sh`,
#   2. a step named "Rust cache" using Swatinem/rust-cache that saves only from main,
# and no other toolchain installer. The setup precedes every step that runs cargo.
# xtask refuses to run in GitHub Actions without the marker the setup exports,
# which also covers a job that reaches cargo only through a script.
set -euo pipefail

workflow_dir="${1:-.github/workflows}"
cd "$(dirname "$0")/../.."

shopt -s nullglob
workflows=("$workflow_dir"/*.yml "$workflow_dir"/*.yaml)
if [ "${#workflows[@]}" -eq 0 ]; then
  echo "FAIL rust-ci-setup: no workflow files found in $workflow_dir" >&2
  exit 1
fi

rust_jobs="$(mktemp)"
trap 'rm -f -- "$rust_jobs"' EXIT
rc=0
for workflow in "${workflows[@]}"; do
  awk -v file="$workflow" -v counter="$rust_jobs" '
    function fail(message) {
      print "FAIL rust-ci-setup: " file ": job " job ": " message > "/dev/stderr"
      failed=1
    }
    function runs_rust(line) {
      return line ~ /(^|[^[:alnum:]_.\/-])(cargo|rustup)([[:space:]]|$|-deny)/ \
        || line ~ /dtolnay\/rust-toolchain/
    }
    function flush_step() {
      if (step == 0) return
      if (step_name == "Set up Rust") {
        setup_count++
        setup_step = step
        if (step_run != "bash scripts/ci/rust-setup.sh") fail("\"Set up Rust\" must run exactly bash scripts/ci/rust-setup.sh")
        if (step_shell != "bash") fail("\"Set up Rust\" must use shell: bash")
      } else if (step_run ~ /rust-setup[.]sh/) {
        fail("scripts/ci/rust-setup.sh may run only in the step named \"Set up Rust\"")
      }
      if (step_uses ~ /^Swatinem\/rust-cache@/) {
        cache_count++
        cache_step = step
        if (step_name != "Rust cache") fail("the rust-cache step must be named \"Rust cache\"")
        if (save_if != "${{ github.ref == '\''refs/heads/main'\'' }}" \
          && save_if != "${{ github.ref == '\''refs/heads/main'\'' || github.head_ref == '\''w1kch9812-cmd/ci-speed'\'' }}") {
          fail("Rust cache must use save-if: ${{ github.ref == '\''refs/heads/main'\'' }} (main writes, pull requests and merge groups only read)")
        }
        if (bad_with != "") fail("Rust cache admits only workspaces, shared-key and save-if, found " bad_with)
        if (!has_workspaces) fail("Rust cache must name its workspaces")
      } else if (step_rust && first_rust_step == 0) {
        first_rust_step = step
      }
      step=0; step_name=""; step_run=""; step_shell=""; step_uses=""; step_rust=0
      save_if=""; bad_with=""; has_workspaces=0; in_with=0
    }
    function flush_job() {
      flush_step()
      if (job != "" && job_rust) {
        print job >> counter
        if (setup_count != 1) fail("runs cargo but has " setup_count " \"Set up Rust\" steps, expected 1")
        if (cache_count != 1) fail("runs cargo but has " cache_count " Rust cache steps, expected 1")
        if (setup_count == 1 && cache_count == 1 && cache_step < setup_step) {
          fail("Rust cache must follow \"Set up Rust\"; its key hashes the environment the setup exports")
        }
        if (setup_count == 1 && first_rust_step && first_rust_step < setup_step) {
          fail("runs cargo before \"Set up Rust\"")
        }
      } else if (job != "" && (setup_count || cache_count)) {
        fail("has a Rust setup or cache but runs no cargo")
      }
      job=""; job_rust=0; in_steps=0; setup_count=0; cache_count=0
      setup_step=0; cache_step=0; first_rust_step=0; step_index=0
    }
    /^jobs:[[:space:]]*$/ { in_jobs=1; next }
    !in_jobs { next }
    /^[^[:space:]]/ { flush_job(); in_jobs=0; next }
    /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
      flush_job()
      job=$0; sub(/^  /, "", job); sub(/:.*/, "", job)
      next
    }
    /^[[:space:]]*#/ { next }
    /^    steps:[[:space:]]*$/ { in_steps=1; next }
    in_steps && /^    [A-Za-z0-9_-]+:/ { flush_step(); in_steps=0 }
    !in_steps { next }
    /^      -([[:space:]]|$)/ {
      flush_step()
      step=++step_index
    }
    step == 0 { next }
    {
      line=$0
      sub(/^      - /, "        ", line)
      if (runs_rust(line)) { step_rust=1; job_rust=1 }
      if (line ~ /^        name:[[:space:]]*/) { value=line; sub(/^        name:[[:space:]]*/, "", value); step_name=value }
      if (line ~ /^        shell:[[:space:]]*/) { value=line; sub(/^        shell:[[:space:]]*/, "", value); step_shell=value }
      if (line ~ /^        run:[[:space:]]*/) { value=line; sub(/^        run:[[:space:]]*/, "", value); step_run=value }
      if (line ~ /^        uses:[[:space:]]*/) {
        value=line; sub(/^        uses:[[:space:]]*/, "", value); sub(/[[:space:]]+#.*$/, "", value); step_uses=value
        if (value ~ /^Swatinem\/rust-cache@/) job_rust=1
      }
      if (line ~ /^        with:[[:space:]]*$/) { in_with=1; next }
      if (line ~ /^        [A-Za-z0-9_-]+:/) in_with=0
      if (in_with && line ~ /^          [A-Za-z0-9_-]+:/) {
        key=line; sub(/^          /, "", key); sub(/:.*/, "", key)
        if (key == "save-if") { value=line; sub(/^          save-if:[[:space:]]*/, "", value); sub(/[[:space:]]+$/, "", value); save_if=value }
        else if (key == "workspaces") has_workspaces=1
        else if (key != "shared-key") bad_with=bad_with " " key
      }
      if (line ~ /rustup[[:space:]]+(toolchain|default|install|override)/ || line ~ /dtolnay\/rust-toolchain/) {
        fail("installs a toolchain outside scripts/ci/rust-setup.sh")
      }
    }
    END { flush_job(); exit failed ? 1 : 0 }
  ' "$workflow" || rc=1
done

if [ "$rc" -ne 0 ]; then
  exit 1
fi
# A parser that recognises no Rust job proves nothing about the ones it missed.
count="$(wc -l <"$rust_jobs" | tr -d ' ')"
if [ "$count" -eq 0 ]; then
  echo "FAIL rust-ci-setup: found no job that runs cargo in $workflow_dir" >&2
  exit 1
fi
echo "OK rust-ci-setup ($count Rust jobs)"
