#!/usr/bin/env bash
# Proves the error-boundary guard rejects both React lifecycle traces while
# accepting the adopted library and prose-like occurrences in source files.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd -P)"
checker="$root/scripts/guard/hand-rolled-error-boundary.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL hand-rolled-error-boundary-self-test: missing $checker" >&2
  exit 1
fi

tmp_root="${TMPDIR:-/tmp}"
tmp_root="${tmp_root%/}"
work="$(mktemp -d "$tmp_root/hand-rolled-error-boundary.XXXXXX")"
cleanup() {
  case "${work:-}" in
    "$tmp_root"/hand-rolled-error-boundary.*) [ ! -e "$work" ] || rm -rf -- "$work" ;;
    *) echo "hand-rolled-error-boundary-self-test: refusing unsafe cleanup" >&2 ;;
  esac
}
trap cleanup EXIT

fixture_repo() {
  local name="$1"
  local repo="$work/$name"
  mkdir -p "$repo/src"
  git -C "$repo" init -q
  cat >"$repo/src/example.tsx"
  git -C "$repo" add src/example.tsx
  printf '%s' "$repo"
}

expect_accepted() {
  local label="$1" repo="$2"
  if ! bash "$checker" "$repo" >/dev/null 2>&1; then
    echo "FAIL hand-rolled-error-boundary-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

expect_rejected() {
  local label="$1" repo="$2"
  if bash "$checker" "$repo" >/dev/null 2>&1; then
    echo "FAIL hand-rolled-error-boundary-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

expect_rejected "getDerivedStateFromError lifecycle" "$(fixture_repo derived <<'TSX'
import React from "react";

export class Boundary extends React.Component {
  static getDerivedStateFromError() {
    return { failed: true };
  }
}
TSX
)"

expect_rejected "componentDidCatch lifecycle" "$(fixture_repo caught <<'TSX'
import React from "react";

export class Boundary extends React.Component {
  componentDidCatch(error: Error) {
    console.error(error);
  }
}
TSX
)"

expect_rejected "lifecycle after a regular-expression literal" "$(fixture_repo regex-before <<'TSX'
const slashPattern = /\/*/;

export class Boundary {
  componentDidCatch(error: Error) {
    console.error(error);
  }
}
TSX
)"

expect_rejected "lifecycle after a regular expression in template interpolation" "$(fixture_repo template-regex <<'TSX'
export const result = `${/}/.test("}")}`;

export class Boundary {
  static getDerivedStateFromError() {
    return { failed: true };
  }
}
TSX
)"

expect_rejected "Unicode-escaped componentDidCatch lifecycle" "$(fixture_repo escaped-caught <<'TSX'
export class Boundary {
  componentDid\u0043atch(error: Error) {
    console.error(error);
  }
}
TSX
)"

expect_rejected "Unicode-escaped getDerivedStateFromError lifecycle" "$(fixture_repo escaped-derived <<'TSX'
export class Boundary {
  static getDerivedStateFrom\u{45}rror() {
    return { failed: true };
  }
}
TSX
)"

expect_accepted "the adopted library" "$(fixture_repo adopted <<'TSX'
import { ErrorBoundary } from "@suspensive/react";

export const App = () => (
  <ErrorBoundary fallback={null} onError={() => undefined}>
    <main />
  </ErrorBoundary>
);
TSX
)"

expect_accepted "lifecycle names only in comments and strings" "$(fixture_repo prose <<'TSX'
// getDerivedStateFromError() belongs to React's class API.
/*
 * componentDidCatch() is named here only to explain a migration.
 */
export const migrationNote = "getDerivedStateFromError() and componentDidCatch() were removed";
export const templateNote = `componentDidCatch() is documentation, too`;
export const lifecycleNamePattern = /(getDerivedStateFromError|componentDidCatch)/;
TSX
)"

echo "OK hand-rolled-error-boundary-self-test"
