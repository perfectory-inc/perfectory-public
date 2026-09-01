#!/usr/bin/env bash
# Fail when the running database does not carry every migration this release ships.
#
# What this prevents: on 2026-09-01 the runtime on the deployment host had four migrations
# applied and the release tree shipped thirty-three. The gap was six weeks old, every
# `catalog.*` table was empty, `GET /catalog/v1/complexes` answered `[]`, and nothing anywhere
# said so — the release script installs a source tree and moves a symlink, and the migrations
# are compiled into a container image by `sqlx::migrate!`, so deploying source could never move
# them. Three deploys ran that day without noticing.
#
# **It compares versions, not content.** `_sqlx_migrations.checksum` is a SHA-384 over the bytes
# that were applied, and this repository is a published snapshot whose migration bytes do not
# match the private tree the running image was built from — measured 2026-09-01 on the first two
# migrations. A content check here would fail for a reason that has nothing to do with drift.
#
# **It fails when it cannot look.** An unreachable database, a missing container, a missing
# ledger table: each is a state in which the question was not answered, and reporting an
# unanswered question as a pass is the defect this file exists to remove.
set -uo pipefail

release_root="${FOUNDATION_PLATFORM_RELEASE_ROOT:-/opt/foundation-platform}"
release_dir="${1:-${release_root}/current}"
runtime_script="${release_dir}/scripts/deploy/foundation-runtime.sh"

fail() {
  printf 'FAIL runtime-migrations: %s\n' "$1" >&2
  exit 1
}

[[ -d "${release_dir}/migrations" ]] || fail "release has no migrations directory: ${release_dir}"
[[ -x "${runtime_script}" ]] || fail "release has no runtime compose wrapper: ${runtime_script}"

# The release is the authority on what should be applied. Reading the version off the file name
# rather than a hand-kept list means adding a migration cannot forget to update this check.
shipped="$(
  find "${release_dir}/migrations" -maxdepth 1 -name '*.sql' -printf '%f\n' \
    | sed -E 's/^([0-9]+)_.*/\1/' | sort -u
)"
shipped_count="$(printf '%s\n' "${shipped}" | grep -c '[0-9]')"
[[ "${shipped_count}" -gt 0 ]] || fail "release ships no migrations, which cannot be right"

# Two different unanswered questions, and they need different answers from the operator: the
# compose wrapper refusing to run (its env file is root-only) is not the same as the database
# being down. Collapsing them sends someone to restart a container that was never stopped.
wrapper_error="$("${runtime_script}" ps -q postgres 2>&1 >/dev/null)"
wrapper_status=$?
container="$("${runtime_script}" ps -q postgres 2>/dev/null | head -1)"
if [[ "${wrapper_status}" -ne 0 ]]; then
  fail "could not ask the runtime what is running: ${wrapper_error}"
fi
[[ -n "${container}" ]] || fail "runtime postgres container is not running; the schema cannot be read"

applied="$(
  docker exec "${container}" sh -lc \
    'psql -X -q -A -t -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select version from _sqlx_migrations order by version"' \
    2>/dev/null | tr -d '\r' | grep '[0-9]' | sort -u
)"
applied_count="$(printf '%s\n' "${applied}" | grep -c '[0-9]')"
[[ "${applied_count}" -gt 0 ]] || fail "runtime reports no applied migrations; either the ledger is empty or it could not be read"

missing="$(comm -23 <(printf '%s\n' "${shipped}") <(printf '%s\n' "${applied}"))"
unknown="$(comm -13 <(printf '%s\n' "${shipped}") <(printf '%s\n' "${applied}"))"

if [[ -n "${missing}" ]]; then
  printf 'FAIL runtime-migrations: the running database is behind this release by %s migration(s).\n' \
    "$(printf '%s\n' "${missing}" | grep -c '[0-9]')" >&2
  printf '  shipped=%s applied=%s\n' "${shipped_count}" "${applied_count}" >&2
  printf '%s\n' "${missing}" | sed 's/^/  missing /' >&2
  printf '  Deploying source does not move these: they are compiled into the runtime image by\n' >&2
  printf '  sqlx::migrate!. Run `foundation-release.sh migrate` to rebuild and apply.\n' >&2
  exit 1
fi

if [[ -n "${unknown}" ]]; then
  printf 'FAIL runtime-migrations: the running database carries %s migration(s) this release does not ship.\n' \
    "$(printf '%s\n' "${unknown}" | grep -c '[0-9]')" >&2
  printf '%s\n' "${unknown}" | sed 's/^/  unknown /' >&2
  printf '  Either the release is older than the database, or something applied a migration\n' >&2
  printf '  from outside this tree. Neither is a state to deploy on top of.\n' >&2
  exit 1
fi

printf 'OK runtime-migrations: %s shipped, %s applied, none missing\n' "${shipped_count}" "${applied_count}"
