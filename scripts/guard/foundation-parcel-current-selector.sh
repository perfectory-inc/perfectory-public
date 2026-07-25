#!/usr/bin/env bash
# Bounded guard: the Rust contract artifact owns the parcel current-row
# predicate; production Spark consumers may only call its helper.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
jobs_dir="$repo_root/platforms/foundation-platform/infra/lakehouse/spark/jobs"

[ -d "$jobs_dir" ] || {
  echo "FAIL foundation-parcel-current-selector: missing $jobs_dir" >&2
  exit 1
}

for command in find grep sed tr; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "FAIL foundation-parcel-current-selector: $command is required" >&2
    exit 1
  }
done

# Flatten each bounded production job before matching so a direct Spark
# expression split across lines cannot bypass the check. Keep every pattern
# local to one expression: bounded wildcards both made this scan slow and
# joined unrelated expressions into false positives.
sql_pattern="(^|[^[:alnum:]_])['\"\`]?valid_to_utc['\"\`]?[[:space:]]+IS[[:space:]]+(NOT[[:space:]]+)?NULL([^[:alnum:]_]|\$)"
spark_method_pattern='(F\.)?(col|column)\([[:space:]]*["'\'']valid_to_utc["'\''][[:space:]]*\)[[:space:]]*\.[[:space:]]*(isnull|isnotnull|eqnullsafe)[[:space:]]*\('
spark_index_method_pattern='\[[[:space:]]*["'\'']valid_to_utc["'\''][[:space:]]*\][[:space:]]*\.[[:space:]]*(isnull|isnotnull|eqnullsafe)[[:space:]]*\('
spark_attribute_method_pattern='(^|[^[:alnum:]_])[[:alpha:]_][[:alnum:]_]*\.valid_to_utc[[:space:]]*\.[[:space:]]*(isnull|isnotnull|eqnullsafe)[[:space:]]*\('
spark_function_pattern='(F\.)?(isnull|isnotnull)[[:space:]]*\([[:space:]]*(F\.)?(col|column)\([[:space:]]*["'\'']valid_to_utc["'\''][[:space:]]*\)[[:space:]]*\)'
artifact_index_pattern='\[[[:space:]]*["'\'']current_row_predicate["'\''][[:space:]]*\]'
artifact_get_pattern='\.get[[:space:]]*\([[:space:]]*["'\'']current_row_predicate["'\'']'
pattern="$sql_pattern|$spark_method_pattern|$spark_index_method_pattern|$spark_attribute_method_pattern|$spark_function_pattern|$artifact_index_pattern|$artifact_get_pattern"

while IFS= read -r -d '' candidate; do
  basename="${candidate##*/}"
  case "$basename" in
    spatial_tile_publication_wap.py) ;;
    *)
      if ! printf '%s\n' "$basename" | grep -Eiq 'parcel[[:alnum:]_]*boundar' \
        && ! grep -Eiq 'parcel[[:alnum:]_]*boundar' "$candidate"; then
        continue
      fi
      ;;
  esac
  # Ignore only unambiguous full-line Python/SQL comments. Inline comments and
  # string literals containing an exact selector remain fail-closed; parsing
  # both languages here would create a second analyzer.
  if sed '/^[[:space:]]*#/d; /^[[:space:]]*--/d' "$candidate" \
    | tr '\n' ' ' \
    | grep -Eiq "$pattern"; then
    echo "FAIL foundation-parcel-current-selector: handwritten selector in $candidate; use platform_contracts.current_row_predicate()" >&2
    exit 1
  fi
done < <(
  find "$jobs_dir" -type f \
    \( -name '*.py' -o -name '*.sql' \) \
    ! -name 'platform_contracts.py' -print0
)

echo "OK foundation-parcel-current-selector"
