#!/usr/bin/env bash
# Bounded guard: domain and application crates never read the process
# environment. A deployment switch read inside a use case is answerable
# differently by two call sites and cannot be exercised without mutating global
# state, so those decisions are injected as types (see
# `RuntimeManifestPublicationCapability`) and resolved once at startup by an
# infrastructure crate or service, which this guard does not restrict.
#
# The rule is frozen while the count is zero. Every `*-domain` and
# `*-application` crate in the repository is currently clean, so there is no
# exception list to maintain and no legacy tail to argue about.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
scan_roots=()
for candidate in platforms products; do
  [ -d "$repo_root/$candidate" ] && scan_roots+=("$repo_root/$candidate")
done

[ "${#scan_roots[@]}" -gt 0 ] || {
  echo "FAIL no-env-access-in-domain-layers: no platforms/ or products/ under $repo_root" >&2
  exit 1
}

for command in find grep sed; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "FAIL no-env-access-in-domain-layers: $command is required" >&2
    exit 1
  }
done

# `env!`/`option_env!` are included deliberately. They read the environment at
# compile time, which bakes one machine's value into the artifact — a stronger
# version of the same defect, not an exemption from it.
pattern='(^|[^[:alnum:]_])(std::)?env::(var|var_os|vars|vars_os)[[:space:]]*[(<]|(^|[^[:alnum:]_])(option_)?env![[:space:]]*[("]'

failed=0
while IFS= read -r manifest; do
  crate_dir="${manifest%/Cargo.toml}"
  case "${crate_dir##*/}" in
    *-domain | *-application) ;;
    *) continue ;;
  esac
  [ -d "$crate_dir/src" ] || continue
  while IFS= read -r -d '' source_file; do
    # Drop only unambiguous full-line comments. An inline comment or a string
    # literal that names an environment read stays fail-closed: deciding which
    # occurrences are inert would mean parsing Rust here.
    if sed '/^[[:space:]]*\/\//d' "$source_file" | grep -Eq "$pattern"; then
      echo "FAIL no-env-access-in-domain-layers: ${source_file#"$repo_root/"} reads the process environment; inject the decision as a type instead" >&2
      failed=1
    fi
  done < <(find "$crate_dir/src" -type f -name '*.rs' -print0)
done < <(
  find "${scan_roots[@]}" -type d -name target -prune -o -type f -name Cargo.toml -print \
    | sort
)

[ "$failed" -eq 0 ] || exit 1

echo "OK no-env-access-in-domain-layers"
