#!/bin/bash
# T22: does any line of the rig's log group hold a credential? Runs in the log-scan task (probes.tf), whose
# execution role injects every secret of the rig but the warehouse's R2 credential (that slot has one reader, the
# warehouse task) and whose task role may only read this log group. It prints counts and names, never a value
# and never a log line.
#
#   log-scan.sh <minutes back>
set -euo pipefail
minutes="${1:-180}"
start=$(( ($(date +%s) - minutes * 60) * 1000 ))
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
aws logs filter-log-events --region "$AWS_REGION" --log-group-name "$LOG_GROUP" --start-time "$start" \
  --query 'events[].message' --output text > "$work/lines"
printf '{"scan": "lines", "minutes": %s, "bytes": %s}\n' "$minutes" "$(wc -c < "$work/lines")"

found=0
# Every injected secret by name: SECRET_<NAME> (the task definition's), searched as a fixed string.
for name in $(compgen -v | grep '^SECRET_' || true); do
  value="${!name}"
  [ "${#value}" -ge 8 ] || { printf '{"scan": "%s", "skipped": "shorter than 8"}\n' "$name"; continue; }
  count="$(grep -c -F -- "$value" "$work/lines" || true)"
  printf '{"scan": "%s", "lines_with_value": %s}\n' "$name" "$count"
  [ "$count" = 0 ] || found=1
done
# Shapes of a credential whose value this task does not hold: a JWT, an AWS key id, a Cloudflare R2 secret
# access key (64 hex) or key id (32 hex) standing alone.
for shape in 'eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}' '(AKIA|ASIA)[A-Z0-9]{16}' '(^|[^0-9a-f])[0-9a-f]{64}([^0-9a-f]|$)' \
  '(^|[^0-9a-f])[0-9a-f]{32}([^0-9a-f]|$)'; do
  count="$(grep -c -E -- "$shape" "$work/lines" || true)"
  printf '{"scan": "shape", "pattern": "%s", "lines": %s}\n' "$shape" "$count"
done
exit "$found"
