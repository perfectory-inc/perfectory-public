#!/usr/bin/env bash
# Daily RUSTSEC check of every Cargo workspace on main (root ADR-0271). Each refused advisory is an
# `advisory` annotation, which the host's service checks carry to Slack (root ADR-0278).
# Prevents: a new advisory or yank reaching main without a commit and being found by an unrelated
# pull request hours later (2026-10-16/17, zip twice).
#
#   check <summary.md> [workspace-dir...]
#       cargo-deny check advisories in each workspace with its own deny.toml (default: every directory
#       that holds a tracked Cargo.lock); writes a markdown summary; exits 1 when any workspace refuses.
set -euo pipefail

fail() {
  echo "FAIL cargo-advisories: $*" >&2
  exit 1
}

check() {
  [ "$#" -ge 1 ] || fail "usage: check <summary.md> [workspace-dir...]"
  local summary="$1" dir log refused=0
  shift
  local dirs=("$@")
  if [ "${#dirs[@]}" -eq 0 ]; then
    mapfile -t dirs < <(git ls-files '*Cargo.lock' | xargs -r -n1 dirname | sort -u)
  fi
  [ "${#dirs[@]}" -gt 0 ] || fail "no Cargo workspace to check"
  command -v cargo-deny >/dev/null || fail "cargo-deny is not on PATH (cargo xtask ci install-cargo-deny)"
  log="$(mktemp)"
  : >"$summary"
  for dir in "${dirs[@]}"; do
    [ -f "$dir/Cargo.lock" ] || fail "$dir has no Cargo.lock"
    local config=()
    [ -f "$dir/deny.toml" ] && config=(--config "$dir/deny.toml")
    # --config goes before `check` since cargo-deny 0.21.0 (PR#899 moved it to the top level; 0.19 took it only after).
    if cargo-deny --locked --manifest-path "$dir/Cargo.toml" "${config[@]}" check --hide-inclusion-graph advisories >"$log" 2>&1; then
      echo "ok $dir"
      continue
    fi
    refused=1
    echo "REFUSED $dir" >&2
    cat "$log" >&2
    # In a GitHub run each advisory it names is also an `advisory` annotation, which the host's
    # service checks read from the daily run on main to page #alerts (root ADR-0278):
    # `<advisory id> cargo:<workspace>`, a yanked crate as `yanked:<crate>`.
    if [ "${GITHUB_ACTIONS:-}" = true ]; then
      {
        grep -oE 'ID: (RUSTSEC|GHSA)-[A-Za-z0-9-]+' "$log" | cut -d' ' -f2 || true
        grep -oE 'cargo update -p [A-Za-z0-9_-]+' "$log" | awk '{ print "yanked:" $4 }' || true
      } | sort -u | while read -r advisory; do
        printf '::error title=advisory::%s cargo:%s\n' "$advisory" "$dir"
      done
    fi
    {
      printf '### `%s`\n\n```\n' "$dir"
      # The finding lines; a run that stopped before any finding (fetch, metadata) keeps its tail.
      grep -E '^(error|warning)\[|ID: |Advisory: ' "$log" || tail -n 20 "$log"
      printf '```\n\n'
    } >>"$summary"
  done
  rm -f -- "$log"
  return "$refused"
}

case "${1:-}" in
  check) shift; check "$@" ;;
  *) fail "usage: $0 check <summary.md> [workspace-dir...]" ;;
esac
