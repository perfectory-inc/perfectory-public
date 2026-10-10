#!/usr/bin/env bash
# Sourced by every job that connects to the Foundation database over the published loopback port.
# The one place that says which database a runtime environment names (root ADR-0177): until
# 2026-10-10 nine scripts each assembled this address with `/foundation` written into it, so a
# staging run had no way to name another database.
#
#   foundation_database_name           prints the database the runtime environment names
#   foundation_database_url [migrator] prints postgres://<role>:<password>@127.0.0.1:<port>/<database>
#                                      for foundation_admin (default, FOUNDATION_ADMIN_PASSWORD) or
#                                      foundation_migrator (FOUNDATION_MIGRATOR_PASSWORD)
#
#   FOUNDATION_PLATFORM_RUNTIME_ENV  staging names foundation_staging; any other value, or none,
#                                    names foundation, as the R2 client's namespace does (only
#                                    `staging` selects staging), so no production job's address
#                                    changes with this file.
#   FOUNDATION_DB_PORT               the published port (default 15434, docker-compose.yml).
#
# recovery.env holds the passwords, not the address. A URL contains its password: capture it, never
# print it.

foundation_database_name() {
  if [[ "${FOUNDATION_PLATFORM_RUNTIME_ENV:-}" == staging ]]; then
    printf 'foundation_staging\n'
  else
    printf 'foundation\n'
  fi
}

foundation_database_url() {
  local database
  database="$(foundation_database_name)" || return 1
  case "${1:-admin}" in
    admin) : "${FOUNDATION_ADMIN_PASSWORD:?recovery.env must provide FOUNDATION_ADMIN_PASSWORD}" ;;
    # Not a `${…:?}` expansion: the runtime-secrets check would read it as a need of every job that
    # sources this file. Only the staging smoke asks for the migrator, and lists it in its own needs.
    migrator)
      [[ -n "${FOUNDATION_MIGRATOR_PASSWORD:-}" ]] ||
        { printf 'foundation_database_url: FOUNDATION_MIGRATOR_PASSWORD is not set\n' >&2; return 78; }
      ;;
    *) printf 'foundation_database_url: unknown role %s\n' "$1" >&2; return 64 ;;
  esac
  python3 -I - "${1:-admin}" "${database}" <<'PY'
import os
import sys
import urllib.parse

role, database = sys.argv[1], sys.argv[2]
password = os.environ["FOUNDATION_MIGRATOR_PASSWORD" if role == "migrator" else "FOUNDATION_ADMIN_PASSWORD"]
port = os.environ.get("FOUNDATION_DB_PORT", "15434")
print(f"postgres://foundation_{role}:{urllib.parse.quote(password, safe='')}@127.0.0.1:{port}/{database}")
PY
}
