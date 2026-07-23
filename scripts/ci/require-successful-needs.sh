#!/usr/bin/env bash
# Stable terminal-job gate: every declared upstream result must be `success`.
set -euo pipefail

found=0
while IFS='=' read -r name value; do
  case "$name" in
    REQUIRED_RESULT_*)
      found=$((found + 1))
      if [ "$value" != success ]; then
        echo "FAIL required-job-result: $name=$value" >&2
        exit 1
      fi
      ;;
  esac
done < <(env)

if [ "$found" -eq 0 ]; then
  echo "FAIL required-job-result: no REQUIRED_RESULT_* inputs" >&2
  exit 1
fi

echo "OK required-job-result count=$found"
