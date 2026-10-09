#!/usr/bin/env bash
# Sourced by the registered jobs that keep a journal file and a run log under their state directory
# (root ADR-0174). The unit's journal (`journalctl -u <unit>`) is the log of record: the state
# directory belongs to the service account and an operator cannot read it, so a line written only
# there is a line nobody sees. The files stay: later steps and runbooks read them.
#
#   job_journal <file> <text>       appends "<UTC time> <text>" to the job's journal file and prints
#                                   <text> on stdout, so the unit's journal carries the same line.
#   job_run_log_tail <run log> [n]  prints the last n lines (default 40) of a run log on stderr: each
#                                   prefixed "  | " (no relayed line can read as the job's own
#                                   `foundation-job-outcome` line), cut to 400 characters, with
#                                   credentials in URLs, bearer tokens and *SECRET*/*PASSWORD*/*TOKEN*
#                                   assignments masked. A run log holds a line per file downloaded, so
#                                   only its end is relayed, and only when the run failed.
#
# Neither function fails: both run inside ERR traps, where a second failure would hide the first.

job_journal() {
  local file="$1"
  shift
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" >>"${file}" 2>/dev/null || true
  printf '%s\n' "$*" || true
}

job_run_log_tail() {
  local file="$1" lines="${2:-40}"
  if [[ ! -s "${file}" ]]; then
    printf 'run log %s is empty or missing\n' "${file}" >&2 || true
    return 0
  fi
  printf 'last %s lines of %s:\n' "${lines}" "${file}" >&2 || true
  tail -n "${lines}" -- "${file}" 2>/dev/null | python3 -I -c '
import re, sys
masks = (
    (re.compile(r"(://[^/\s:@]+:)[^@\s]+@"), r"\1***@"),
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1***"),
    (re.compile(r"(?i)\b([A-Z0-9_]*(?:SECRET|PASSWORD|TOKEN|ACCESS_KEY|API_KEY)[A-Z0-9_]*\s*[=:]\s*)\S+"), r"\1***"),
)
for raw in sys.stdin.buffer:
    line = raw.decode("utf-8", "replace").rstrip("\r\n").split("\r")[-1]
    for pattern, replacement in masks:
        line = pattern.sub(replacement, line)
    if len(line) > 400:
        line = line[:400] + "..."
    print("  | " + line)
' >&2 || true
  return 0
}
