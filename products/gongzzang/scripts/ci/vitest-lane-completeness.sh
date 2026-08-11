#!/usr/bin/env bash
# Prove that every Vitest test file on disk is collected by some lane.
#
# The two lanes split by directory, and nothing outside those directories runs at
# all. A `*.test.ts` written next to a component — or a directory renamed on one
# side only — is collected by no project, and both `pnpm test` and
# `pnpm test:integration` stay green while it never executes. ADR-0011 남은 부채 2
# named this: the split may be complete, but nothing proves it.
#
# The authority is Vitest's own collector, not a second implementation of its
# include/exclude semantics. `vitest list --filesOnly --json` reports the files the
# configured projects actually resolve; re-deriving that from the config here would
# recreate the mirrored-pattern problem this check exists to catch. `--static-parse`
# (Vitest 4.1+) parses the files instead of importing them, so collection cannot
# touch Redis or any other backing service.
#
# Run from the gongzzang workspace root, after `pnpm install`.
set -euo pipefail
cd "$(dirname "$0")/../.."

web="apps/web"
[ -d "$web/node_modules" ] || [ -d node_modules ] || {
  echo "FAIL vitest-lane-completeness: dependencies are not installed; run pnpm install first" >&2
  exit 1
}

collected="$(mktemp)"
discovered="$(mktemp)"
cleanup() { rm -f -- "$collected" "$discovered"; }
trap cleanup EXIT

# stdout carries the JSON; keep it out of the judgment position (ADR-0012 rule 2).
listing="$(mktemp)"
if ! pnpm -C "$web" exec vitest list --filesOnly --json --static-parse >"$listing" 2>"$listing.err"; then
  echo "FAIL vitest-lane-completeness: 'vitest list' failed" >&2
  cat "$listing.err" >&2 || true
  rm -f -- "$listing" "$listing.err"
  exit 1
fi

# Each path on its own line, terminated. Without the trailing newline `wc -l` reports
# one fewer than there are entries, and the OK line then contradicts itself — a
# passing check that reads like a failure is the defect ADR-0012 is named after.
node -e '
const fs = require("fs");
const path = require("path");
const listing = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
const web = path.resolve(process.argv[2]);
const norm = (p) => path.relative(web, p).split(path.sep).join("/");
const lines = listing.map((entry) => norm(entry.file)).sort();
fs.writeFileSync(process.argv[3], lines.length ? `${lines.join("\n")}\n` : "");
' "$listing" "$web" "$collected"
rm -f -- "$listing" "$listing.err"

# Playwright owns `*.spec.ts`; Vitest owns `*.test.ts(x)`. Only the latter is this
# check's subject, and the extension split is what keeps the two runners apart.
#
# `LC_ALL=C` on both sorts: `comm` assumes its inputs are ordered the same way it
# compares them, and a locale-dependent collation on one side silently produces
# wrong differences rather than an error.
(cd "$web" && find . -path ./node_modules -prune -o \
  \( -name '*.test.ts' -o -name '*.test.tsx' \) -print) \
  | sed 's|^\./||' | LC_ALL=C sort >"$discovered"
LC_ALL=C sort -o "$collected" "$collected"

missing="$(comm -23 "$discovered" "$collected")"
if [ -n "$missing" ]; then
  echo "FAIL vitest-lane-completeness: these test files are collected by no Vitest project," >&2
  echo "  so neither lane runs them and both lanes stay green:" >&2
  printf '    %s\n' $missing >&2
  echo "  Move them under a lane's directory, or widen a project's include." >&2
  exit 1
fi

echo "OK vitest-lane-completeness (collected=$(wc -l <"$collected"), discovered=$(wc -l <"$discovered"))"
