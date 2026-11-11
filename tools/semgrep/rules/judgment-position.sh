#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/judgment-position.yml (`opengrep scan --test`). Not run.
set -euo pipefail

# The 2026-08-05 shape.
# ruleid: judgment-position-reads-status-after-a-pipeline
run-the-build --workspace | tail -40
echo "CHECK_EXIT=$?"
# The same read, one blank line later: a status does not expire.
# ruleid: judgment-position-reads-status-after-a-pipeline
cmd | tail -4

rc=$?
# The ADR's own forbidden example, and its exit and return forms.
# ruleid: judgment-position-pipeline-decides-an-or
cmd | tail -6 || rc=1
# ruleid: judgment-position-pipeline-decides-an-or
cmd | tail -6 || exit 1
# ruleid: judgment-position-pipeline-decides-an-or
cmd && other | sort || return 1

# The adopted form: judge the command, tidy the output separately.
# ok: judgment-position-reads-status-after-a-pipeline
if cmd >out.txt 2>&1; then tail -2 out.txt; else rc=1; tail -20 out.txt; fi
echo "EXIT=$?"
# `|| true` discards the status on purpose.
# ok: judgment-position-pipeline-decides-an-or
git ls-files | sort -u || true
# A pipeline whose status nobody reads.
# ok: judgment-position-reads-status-after-a-pipeline
cmd | sort >out.txt
echo done
# `|` in a case pattern is alternation.
case "$path" in
  # ok: judgment-position-pipeline-decides-an-or
  /tmp/*|/var/tmp/*) rm -rf -- "$path" || exit 1 ;;
esac
# So is `|` in a `[[ ]]` regex, and a quoted pipe is data.
# ok: judgment-position-pipeline-decides-an-or
if [[ "$line" =~ ^(alpha|beta)$ ]] || rc=1; then :; fi
# ok: judgment-position-pipeline-decides-an-or
awk "{print \$1|\"sort\"}" file || rc=1
# ok: judgment-position-pipeline-decides-an-or
grep -c 'a|b' file || rc=1
# A comment is not a pipeline.
# ok: judgment-position-reads-status-after-a-pipeline
# cmd | tail
echo "$?"
# An `||` chain with no pipe.
# ok: judgment-position-pipeline-decides-an-or
a || b || exit 1
# A reviewed exception is declared on the line.
# ok: judgment-position-pipeline-decides-an-or
cmd | tail -6 || rc=1 # verification-judgment: reviewed-pipeline
# ok: judgment-position-reads-status-after-a-pipeline
cmd | tail -6 # verification-judgment: reviewed-pipeline
echo "$?"
