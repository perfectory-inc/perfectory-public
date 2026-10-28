#!/usr/bin/env bash
# Stable terminal-job gate: every declared upstream result must be `success`.
# A scoped workflow (root ADR-0202) also passes CHANGE_SCOPE_RUN, its `scope`
# job's answer. Only an explicit `false` lets the gated jobs be `skipped`; the
# scope job itself must still succeed, so a broken selector cannot skip a gate.
set -euo pipefail

scope_run="${CHANGE_SCOPE_RUN-unset}"
case "$scope_run" in
  unset|true|false) ;;
  *)
    echo "FAIL required-job-result: CHANGE_SCOPE_RUN='$scope_run' is neither true nor false" >&2
    exit 1
    ;;
esac

# Root ADR-0324: the scope job may also name, as a JSON list of job ids, the jobs it found with no
# work; they never took a runner. Only those, and never the scope job, may be skipped.
skippable=" "
if [ -n "${CHANGE_SCOPE_SKIPPED+set}" ]; then
  if [ "$scope_run" = unset ] || ! [[ "$CHANGE_SCOPE_SKIPPED" =~ ^\[(\"[a-z0-9-]+\"(,\ ?\"[a-z0-9-]+\")*)?\]$ ]]; then
    echo "FAIL required-job-result: CHANGE_SCOPE_SKIPPED='$CHANGE_SCOPE_SKIPPED' is not a scope job's list of job ids" >&2
    exit 1
  fi
  ids="${CHANGE_SCOPE_SKIPPED//[^a-z0-9-]/ }"
  for job in $ids; do
    name="${job//-/_}"
    name="REQUIRED_RESULT_${name^^}"
    if [ "$name" = REQUIRED_RESULT_SCOPE ]; then
      echo "FAIL required-job-result: the scope job cannot skip itself" >&2
      exit 1
    fi
    skippable+="$name "
  done
fi

found=0
skipped=0
while IFS='=' read -r name value; do
  case "$name" in
    REQUIRED_RESULT_*)
      found=$((found + 1))
      if [ "$value" = success ]; then
        continue
      fi
      if [ "$value" = skipped ] && [ "$scope_run" = false ] && [ "$name" != REQUIRED_RESULT_SCOPE ]; then
        skipped=$((skipped + 1))
        continue
      fi
      if [ "$value" = skipped ] && [ "$scope_run" = true ] && [[ "$skippable" == *" $name "* ]]; then
        skipped=$((skipped + 1))
        continue
      fi
      echo "FAIL required-job-result: $name=$value" >&2
      exit 1
      ;;
  esac
done < <(env)

if [ "$found" -eq 0 ]; then
  echo "FAIL required-job-result: no REQUIRED_RESULT_* inputs" >&2
  exit 1
fi
if [ "$scope_run" != unset ] && [ "${REQUIRED_RESULT_SCOPE-}" != success ]; then
  echo "FAIL required-job-result: a scoped workflow must report its scope job" >&2
  exit 1
fi

echo "OK required-job-result count=$found skipped-by-scope=$skipped"
