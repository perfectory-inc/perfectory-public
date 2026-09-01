#!/usr/bin/env bash
# Every schema the database bootstrap grants on must be one it also creates.
#
# What real incident does failing this prevent? On 2026-09-01, rebuilding the runtime database
# from empty stopped at `foundation-bootstrap` with `schema "catalog" does not exist`. The
# bootstrap ran `ALTER SCHEMA catalog OWNER TO ...` and `GRANT ... ON SCHEMA catalog`, but only
# `serving_postgis` was ever created there — `catalog` is created by migration `20260719000001`,
# which compose runs *after* bootstrap. So the stack could not be stood up from an empty
# database at all, and it went unseen for six weeks because the deployment host had been
# restored from a dump that already had the schema.
#
# The property, not the spelling: a schema named by ALTER or GRANT must appear in a CREATE
# SCHEMA in the same file.
set -uo pipefail

root="${1:-$(git rev-parse --show-toplevel)}"
sql="${root}/platforms/foundation-platform/infra/compose/bootstrap-foundation.sql"
name="bootstrap-creates-what-it-grants-on"
failed=0

[[ -f "${sql}" ]] || {
  printf 'FAIL %s: bootstrap SQL is missing: %s\n' "${name}" "${sql}" >&2
  exit 1
}

created="$(grep -oiE 'CREATE SCHEMA (IF NOT EXISTS )?[a-z_][a-z0-9_]*' "${sql}" \
  | awk '{print $NF}' | sort -u)"

# `public` is created by Postgres itself and is the one schema a bootstrap may grant on without
# creating. Naming the exception here rather than skipping quietly keeps it visible.
altered="$(grep -oiE 'ALTER SCHEMA [a-z_][a-z0-9_]*' "${sql}" | awk '{print $NF}' | sort -u)"
# `ON SCHEMA a, b TO role` — take the names between `SCHEMA` and the TO/FROM that ends the list.
# Matching greedily to end of line swallowed `TO foundation_migrator` into the schema name, and
# the self-test caught it by rejecting the real file.
granted="$(sed -nE 's/.*ON SCHEMA ([a-zA-Z0-9_, ]+)[[:space:]]+(TO|FROM)[[:space:]].*/\1/Ip' "${sql}" \
  | tr ',' '\n' | tr -d ' ' | grep -v '^$' | sort -u)"

for schema in $(printf '%s\n%s\n' "${altered}" "${granted}" | sort -u); do
  [[ "${schema}" == "public" ]] && continue
  grep -qxF "${schema}" <<<"${created}" || {
    printf 'FAIL %s: bootstrap grants on schema %s but never creates it\n' "${name}" "${schema}" >&2
    printf '  compose runs bootstrap before the migrations, so on an empty database this is an error\n' >&2
    failed=1
  }
done

if [[ "${failed}" -eq 0 ]]; then
  printf 'OK %s\n' "${name}"
fi
exit "${failed}"
