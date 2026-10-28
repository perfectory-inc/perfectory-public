#!/usr/bin/env bash
# Bounded guard: no tracked source or document file passes 1500 lines without a recorded reason.
#
# What failing this prevents: on 2026-10-28 thirteen tracked files were over the 1500-line rule
# in foundation's and gongzzang's AGENTS.md, because only gongzzang's repo-guard enforced it, and
# only inside products/gongzzang. The rule and the ratchet: file-line-limit.py.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
checker="$(cd "$(dirname "$0")" && pwd)/file-line-limit.py"
contract="${2:-$root/tools/ci/file-line-limit.exceptions.json}"

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$checker" "$root" "$contract"
fi
exec python "$checker" "$root" "$contract"
