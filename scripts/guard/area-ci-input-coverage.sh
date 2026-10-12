#!/usr/bin/env bash
# Prevents: a test that reads a file outside its own area, behind a CI change
# scope that does not take that file as an input (tools/ci/change-scope.json,
# root ADR-0201). The test then never reruns on the change it is guarding.
# `scripts/ci/change_scope.py check` freezes this for one known input
# (tools/xtask); this guard derives the rest from what the sources actually read.
#
# That gap is not hypothetical. `administrative_boundary_contract` reads two
# documents under `docs/architecture/`, which `foundation-ci.yml` did not watch,
# so the Korean-first documentation migration broke the test on a push that could
# not have rerun it.
#
# Two path roots are in use and the filesystem tells them apart, so this guard
# does not parse `ancestors().nth(N)` or `join("../..")` to decide which applies
# — that would be a second analyzer of Rust, which these guards deliberately
# avoid. A literal that resolves under the area root is area-local and the area's
# own glob already covers it. A literal that resolves only from the repository
# root must appear in the filter. A literal that resolves from neither is not a
# path (an assertion fragment) and is ignored. No path in this repository
# resolves from both roots; if one ever does, it is treated as area-local, which
# can only cause CI to run when it need not.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
selector="$(cd "$(dirname "$0")/../ci" && pwd -P)/change_scope.py"
workflows_dir="${AREA_CI_INPUT_WORKFLOWS_DIR:-$repo_root/.github/workflows}"

[ -d "$workflows_dir" ] || {
  echo "FAIL area-ci-input-coverage: missing $workflows_dir" >&2
  exit 1
}

for command in find grep python3 sed sort; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "FAIL area-ci-input-coverage: $command is required" >&2
    exit 1
  }
done

# Whether one input glob covers one repository-relative file.
covered_by() {
  local glob="$1"
  local target="$2"
  case "$glob" in
    *'/**')
      local base="${glob%/\*\*}"
      [ "$target" = "$base" ] || [ "${target#"$base"/}" != "$target" ]
      ;;
    *) [ "$target" = "$glob" ] ;;
  esac
}

rc=0
checked=0
for workflow in "$workflows_dir"/*.yml; do
  [ -e "$workflow" ] || continue
  # The workflow's inputs as the change-scope contract states them. A workflow
  # the contract leaves unscoped runs on every pull request; only a push-only
  # `paths:` filter it still carries can miss an input, so that list is read.
  filters="$(python3 "$selector" paths --root "$repo_root" --workflow "$(basename "$workflow")")"
  if [ -z "$filters" ] && grep -qE '^[[:space:]]*paths:' "$workflow"; then
    filters="$(grep -oE '^[[:space:]]+- "[^"]+"$' "$workflow" | sed 's/^[^"]*"//; s/"$//')" || true
  fi
  [ -n "$filters" ] || continue
  # Only a workflow that actually runs the area's tests can miss one. `products/gongzzang/**` is
  # also an input of an sqlx-prepare and a walking-skeleton workflow, and requiring those to take
  # every file the area's tests read would be a demand with no defect behind it. Same gate as the
  # xtask input check in change_scope.py: `cargo xtask verify|integration` runs the tests (ADR-0004).
  grep -qE 'cargo[[:space:]]+.*xtask[[:space:]]+(verify|integration)' "$workflow" || continue

  # The area is the one `<platforms|products|consoles>/<name>/**` input. A
  # workflow taking several areas has no single area to scan, and a workflow
  # taking none is not an area CI.
  areas="$(
    printf '%s\n' "$filters" | grep -E '^(platforms|products|consoles)/[A-Za-z0-9._-]+/\*\*$' \
      | sed 's|/\*\*$||' | sort -u
  )" || true
  [ "$(printf '%s\n' "$areas" | grep -c .)" -eq 1 ] || continue
  area="$areas"
  [ -d "$repo_root/$area" ] || continue

  while IFS= read -r literal; do
    [ -n "$literal" ] || continue
    # Area-local: the area's own glob already retriggers the workflow.
    [ -e "$repo_root/$area/$literal" ] && continue
    # If a relative literal did not resolve inside the area, it is assertion data
    # rather than a repository-root input. Never follow it outside the repository:
    # on Linux, strings such as `../../etc/passwd` otherwise make this guard inspect
    # the host filesystem and demand impossible CI path filters for `/etc`.
    case "/$literal/" in
      */../*) continue ;;
    esac
    # Not a path at either root: an assertion fragment, or a generated artifact.
    [ -e "$repo_root/$literal" ] || continue
    checked=$((checked + 1))
    is_covered=0
    while IFS= read -r glob; do
      [ -n "$glob" ] || continue
      if covered_by "$glob" "$literal"; then
        is_covered=1
        break
      fi
    done <<EOF
$filters
EOF
    if [ "$is_covered" -eq 0 ]; then
      echo "FAIL area-ci-input-coverage: $area reads $literal but $(basename "$workflow") does not take it as an input in tools/ci/change-scope.json; a change to that file would not rerun the test that reads it" >&2
      rc=1
    fi
  done <<EOF
$(
  # Quoted literals containing a slash. Requiring the slash drops bare names
  # used only as assertion text, before any filesystem check. One recursive
  # grep over the area, not one process per file: per-file spawning made this
  # guard exceed two minutes on Windows.
  grep -rhoE --include='*.rs' \
    --exclude-dir=target --exclude-dir=node_modules \
    '"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)+"' "$repo_root/$area" 2>/dev/null \
    | tr -d '"' | sort -u
)
EOF
done

[ "$rc" -eq 0 ] || exit 1

echo "OK area-ci-input-coverage (out-of-area inputs checked=$checked)"
