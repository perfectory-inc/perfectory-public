#!/usr/bin/env bash
# Daily RUSTSEC check of every Cargo workspace on main, and the one issue that carries it (root ADR-0271).
# Prevents: a new advisory or yank reaching main without a commit and being found by an unrelated
# pull request hours later (2026-10-16/17, zip twice).
#
#   check <summary.md> [workspace-dir...]
#       cargo-deny check advisories in each workspace with its own deny.toml (default: every directory
#       that holds a tracked Cargo.lock); writes a markdown summary; exits 1 when any workspace refuses.
#   issue <check-result> <summary.md>
#       keeps the one open issue titled below in step with the check: failure opens it or, when the
#       findings changed, rewrites its body and comments; success closes it. Needs gh with GH_REPO.
set -euo pipefail

title="Cargo advisories on main"

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
  command -v cargo-deny >/dev/null || fail "cargo-deny is not on PATH (scripts/ci/install-cargo-deny.sh)"
  log="$(mktemp)"
  : >"$summary"
  for dir in "${dirs[@]}"; do
    [ -f "$dir/Cargo.lock" ] || fail "$dir has no Cargo.lock"
    local config=()
    [ -f "$dir/deny.toml" ] && config=(--config "$dir/deny.toml")
    if cargo-deny --locked --manifest-path "$dir/Cargo.toml" check "${config[@]}" --hide-inclusion-graph advisories >"$log" 2>&1; then
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

issue() {
  [ "$#" -eq 2 ] || fail "usage: issue <check-result> <summary.md>"
  local result="$1" summary="$2" number body current
  : "${GH_REPO:?GH_REPO names the repository}"
  local run="${RUN_URL:-the daily run}"
  number="$(gh issue list --state open --search "\"$title\" in:title" --json number,title \
    --jq "map(select(.title == \"$title\")) | .[0].number // empty")"
  case "$result" in
    success)
      if [ -n "$number" ]; then
        gh issue close "$number" --comment "Cleared: every workspace passes cargo-deny advisories ($run)."
      fi
      ;;
    failure)
      body="$(mktemp)"
      {
        printf 'The daily `cargo-deny check advisories` on main refused (root ADR-0271). Latest run: %s\n\n' "$run"
        if [ -s "$summary" ]; then
          cat "$summary"
        else
          printf 'The check stopped before it wrote a finding; read the run.\n'
        fi
      } >"$body"
      if [ -z "$number" ]; then
        gh issue create --title "$title" --body-file "$body"
      else
        current="$(gh issue view "$number" --json body --jq .body)"
        # The run link changes every day; only a change in the findings rewrites and notifies.
        if [ "$(printf '%s\n' "$current" | sed 1d)" != "$(sed 1d "$body")" ]; then
          gh issue edit "$number" --body-file "$body"
          gh issue comment "$number" --body "The findings changed ($run)."
        fi
      fi
      rm -f -- "$body"
      ;;
    *)
      echo "cargo-advisories: check result '$result' leaves the issue as it is"
      ;;
  esac
}

case "${1:-}" in
  check) shift; check "$@" ;;
  issue) shift; issue "$@" ;;
  *) fail "usage: $0 check <summary.md> [workspace-dir...] | issue <check-result> <summary.md>" ;;
esac
