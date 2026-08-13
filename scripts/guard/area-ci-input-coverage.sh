#!/usr/bin/env bash
# Prevents: a test that reads a file outside its own area, behind a CI path
# filter that does not watch that file. The test then never reruns on the change
# it is guarding. `xtask-path-coverage.sh` freezes this for one known input
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
workflows_dir="${AREA_CI_INPUT_WORKFLOWS_DIR:-$repo_root/.github/workflows}"

[ -d "$workflows_dir" ] || {
  echo "FAIL area-ci-input-coverage: missing $workflows_dir" >&2
  exit 1
}

for command in find grep sed sort; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "FAIL area-ci-input-coverage: $command is required" >&2
    exit 1
  }
done

# Whether one `paths:` entry covers one repository-relative file.
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
  grep -qE '^[[:space:]]*paths:' "$workflow" || continue # unfiltered -> always runs -> covered
  # Only a workflow that actually runs the area's tests can miss one. `products/gongzzang/**` is
  # also watched by an sqlx-prepare and a walking-skeleton workflow, and requiring those to watch
  # every file the area's tests read would be a demand with no defect behind it. Same gate as
  # `xtask-path-coverage`: `cargo xtask verify|integration` is what runs the tests (ADR-0004).
  grep -qE 'cargo[[:space:]]+.*xtask[[:space:]]+(verify|integration)' "$workflow" || continue

  # The area is the one `<platforms|products>/<name>/**` entry the workflow
  # watches. A workflow watching several areas has no single area to scan, and a
  # workflow watching none is not an area CI.
  areas="$(
    grep -oE '"(platforms|products)/[A-Za-z0-9._-]+/\*\*"' "$workflow" \
      | tr -d '"' | sed 's|/\*\*$||' | sort -u
  )" || true
  [ "$(printf '%s\n' "$areas" | grep -c .)" -eq 1 ] || continue
  area="$areas"
  [ -d "$repo_root/$area" ] || continue

  filters="$(grep -oE '^[[:space:]]+- "[^"]+"$' "$workflow" | sed 's/^[^"]*"//; s/"$//')" || true

  while IFS= read -r literal; do
    [ -n "$literal" ] || continue
    # Area-local: the area's own glob already retriggers the workflow.
    [ -e "$repo_root/$area/$literal" ] && continue
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
      echo "FAIL area-ci-input-coverage: $area reads $literal but $(basename "$workflow") does not watch it; a change to that file would not rerun the test that reads it" >&2
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
