#!/usr/bin/env bash
# Proves expand-contract-migrations refuses what it exists to refuse (ADR-0299 계열 E). The fixtures are
# synthetic (a made-up table): a bad ADD COLUMN NOT NULL and a bad lone DROP must be refused; the same
# shapes with a DEFAULT, an emptiness/approval guard, or an `expand-contract-ok:` marker must pass. The
# checker is a pure function of file paths, so this drives it directly without a git diff.
set -euo pipefail
cd "$(dirname "$0")/../.."

name="expand-contract-migrations-self-test"
checker="$(pwd -P)/scripts/guard/expand-contract-migrations.py"
py=python3
command -v python3 >/dev/null 2>&1 || py=python

fixture="$(mktemp -d)"
cleanup() {
  case "${fixture:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$fixture" ;;
    *) echo "FAIL $name: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

write() { # write <file> <<heredoc
  mkdir -p "$fixture/$(dirname "$1")"
  cat >"$fixture/$1"
}

expect() { # expect pass|<text the refusal must hold> <file...>
  local want="$1"; shift
  local files=() output status=0
  for f in "$@"; do files+=("$fixture/$f"); done
  output="$("$py" "$checker" "$fixture" "${files[@]}" 2>&1)" || status=$?
  if [[ "$want" == pass ]]; then
    ((status == 0)) || { echo "FAIL $name: $* was refused:" >&2; echo "$output" >&2; exit 1; }
  elif ((status == 0)); then
    echo "FAIL $name: $* was accepted" >&2; exit 1
  elif ! grep -qF -- "$want" <<<"$output"; then
    echo "FAIL $name: $* was refused for another reason:" >&2; echo "$output" >&2; exit 1
  fi
}

write m/migrations/20200101000000_add_not_null_bad.sql <<'SQL'
ALTER TABLE demo_widget ADD COLUMN demo_label text NOT NULL;
SQL
write m/migrations/20200101000001_add_not_null_default.sql <<'SQL'
ALTER TABLE demo_widget ADD COLUMN demo_label text NOT NULL DEFAULT '';
SQL
write m/migrations/20200101000002_add_not_null_guarded.sql <<'SQL'
DO $$ BEGIN IF EXISTS (SELECT 1 FROM demo_widget) THEN RAISE EXCEPTION 'not empty'; END IF; END $$;
ALTER TABLE demo_widget ADD COLUMN demo_label text NOT NULL;
SQL
write m/migrations/20200101000003_add_not_null_marker.sql <<'SQL'
-- expand-contract-ok: demo_widget is empty at first deploy
ALTER TABLE demo_widget ADD COLUMN demo_label text NOT NULL;
SQL
write m/migrations/20200101000004_drop_bad.sql <<'SQL'
ALTER TABLE demo_widget DROP COLUMN demo_label;
SQL
write m/migrations/20200101000005_drop_marker.sql <<'SQL'
-- expand-contract-ok: readers stopped reading demo_label two releases ago
ALTER TABLE demo_widget DROP COLUMN demo_label;
SQL
write m/migrations/20200101000006_drop_gated.sql <<'SQL'
DO $$ BEGIN PERFORM current_setting('demo.drop_approved'); END $$;
DROP TABLE demo_widget;
SQL
write m/migrations/20200101000007_benign.sql <<'SQL'
CREATE TABLE demo_widget (id int PRIMARY KEY, demo_label text NOT NULL DEFAULT '');
SQL

expect "is NOT NULL with no DEFAULT" m/migrations/20200101000000_add_not_null_bad.sql
expect pass                          m/migrations/20200101000001_add_not_null_default.sql
expect pass                          m/migrations/20200101000002_add_not_null_guarded.sql
expect pass                          m/migrations/20200101000003_add_not_null_marker.sql
expect "is destructive"              m/migrations/20200101000004_drop_bad.sql
expect pass                          m/migrations/20200101000005_drop_marker.sql
expect pass                          m/migrations/20200101000006_drop_gated.sql
expect pass                          m/migrations/20200101000007_benign.sql

echo "OK $name"
