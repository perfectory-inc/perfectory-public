#!/usr/bin/env bash
# Prevents: a second library entering the workspace beside the one already chosen
# for that role, which is how a codebase ends up with two spellings of `debounce`,
# two error-boundary implementations, and a bundle carrying both.
#
# `docs/technology-stack.md` 운영 원칙 1 already says one role gets one canonical
# technology. That rule was only enforced for pinned runtime versions; a plain
# `dependencies` entry could add a whole parallel surface without anything
# noticing. §1.1 of that document names the chosen library per role, so the
# alternatives are refused here rather than argued about in review.
#
# This bans the alternatives; it cannot mechanically require that a hand-written
# `debounce` be replaced by the library one. That half stays a review rule in
# `docs/technology-stack.md` §1.1. Banning the substitutes is the part a script
# can actually decide, and it is the part that goes wrong silently.
#
# Scope note: only dependency KEYS in package manifests are read. A name appearing
# in prose, in a comment, or as a version value is not a dependency, and a guard
# that reacted to those would make the documentation unwritable.
set -euo pipefail

# Manifests may be passed explicitly so the self-test can exercise the failing
# paths; the real repository is expected to sit on the passing one forever.
if [ "$#" -gt 0 ]; then
  manifests=("$@")
else
  cd "$(dirname "$0")/../.."
  mapfile -t manifests < <(git ls-files '*package.json' ':(exclude)**/node_modules/**')
fi

# Anchored to the start of a line and closed by a colon: that is a dependency key
# and nothing else in JSON looks like it.
#
# The list holds only packages whose role §1.1 has already assigned to something
# else. Overlay and Hangul get no entries because nothing rival is likely to be
# installed there — the way those two go wrong is hand-rolling, which a dependency
# key cannot see.
banned_key='^[[:space:]]*"(lodash([.-][a-z0-9.-]+)?|underscore|ramda|react-error-boundary|@types/(lodash|underscore|ramda)([.-][a-z0-9.-]+)?)"[[:space:]]*:'

fail=0
for manifest in "${manifests[@]}"; do
  [ -f "$manifest" ] || continue
  hits="$(grep -nE "$banned_key" "$manifest" || true)"
  if [ -n "$hits" ]; then
    echo "FAIL utility-library-policy: $manifest declares a utility library beside the canonical one." >&2
    echo "$hits" | sed 's/^/    /' >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "    The canonical JS/TS utility library is es-toolkit (docs/technology-stack.md §1)." >&2
  echo "    Use it, or change the canonical choice in that document and this guard together." >&2
  exit 1
fi

echo "OK utility-library-policy (${#manifests[@]} manifests)"
