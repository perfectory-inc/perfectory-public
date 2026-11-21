#!/usr/bin/env bash
# one-off: moves the runtime Postgres's data directory from the Docker volume on the data disk to the
# system disk's SSD once (root ADR-0598); it ends when every host has its moved_marker and the cleanup
# pull request that removes the old volume deletes this file and its call in foundation-release.sh.
#
# Sourced by foundation-release.sh `migrate`, before the compose chain runs:
#
#   move_postgres_data <compose wrapper> <the release's config/host-layout.contract.json>
#
# Returns 0 when there is nothing to move (the marker exists, or the volume holds no cluster) or the
# move checked; 1 when it refused or failed, with the old volume untouched and Postgres serving from it
# again. Steps, each checked before the next:
#   1. a few counts the cluster holds (postgres_counts), while it serves;
#   2. Postgres stops with time for its shutdown checkpoint; pg_controldata says "shut down";
#   3. rsync -a --numeric-ids copies the volume to postgres_data.path (owner uid 70 kept), and a second
#      rsync with checksums finds no difference; pg_controldata of the copy equals the original's, and
#      every pg_tblspc link reads the same;
#   4. the marker is written: from now on postgres-data-source.sh binds the copy;
#   5. Postgres starts on the copy and is healthy; every count is at least what step 1 read, the
#      migrations exactly;
#   6. a full pgBackRest backup is asked for (run-postgres-backup.sh reads the request).
# A failure after step 4 removes the marker and starts Postgres on the volume again.
#
# The downtime is from the stop in step 2 to healthy in step 5; the line `postgres-data-move done`
# says it, with the bytes copied.

postgres_counts() {
  # The migrations ledger and every table of the catalog and audit schemas, `<table> <rows>` sorted.
  # Run while the cluster serves: a count after the move may be larger (append-only), never smaller.
  "$1" exec -T postgres psql -X -q -A -t -v ON_ERROR_STOP=1 -U foundation_admin -d foundation <<'SQL'
SELECT 'public._sqlx_migrations ' || count(*) FROM public._sqlx_migrations;
SELECT format('SELECT %L || '' '' || count(*) FROM %I.%I;', n.nspname || '.' || c.relname, n.nspname, c.relname)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND n.nspname IN ('catalog', 'audit') ORDER BY 1
\gexec
SQL
}

# pg_controldata of a stopped cluster's directory, in the postgres service's own image and memory cap.
postgres_controldata() {
  "$1" run --rm -T -u 70:70 -v "$2:/d:ro" --entrypoint pg_controldata postgres /d
}

move_postgres_data() {
  local runtime="$1" contract="$2" layout data marker volume source
  layout="$(python3 -I -c 'import json, sys
entry = json.load(open(sys.argv[1]))["postgres_data"]
print(entry["path"], entry["moved_marker"], entry["volume"])' "${contract}")" || return 1
  read -r data marker volume <<<"${layout}"
  if [[ -e "${marker}" ]]; then
    return 0
  fi
  if ! source="$(docker volume inspect -f '{{.Mountpoint}}' "${volume}" 2>/dev/null)" || [[ ! -s "${source}/PG_VERSION" ]]; then
    printf 'postgres-data-move: %s holds no cluster; nothing to move\n' "${volume}"
    return 0
  fi
  local present=""
  [[ ! -d "${data}" ]] || present="$(find "${data}" -mindepth 1 -maxdepth 1 -print -quit)"
  if [[ -n "${present}" ]]; then
    printf 'postgres-data-move: %s is not empty and %s does not exist; refusing to copy over it\n' "${data}" "${marker}" >&2
    return 1
  fi
  local before after started copied controldata copy_controldata
  before="$(postgres_counts "${runtime}")" || { printf 'postgres-data-move: cannot count the cluster before the move\n' >&2; return 1; }
  started="${SECONDS}"
  # Docker's default 10 s would kill a shutdown checkpoint on the busy data disk: the copy of a cluster
  # that did not shut down cleanly is refused below.
  "${runtime}" stop -t 300 postgres || return 1
  controldata="$(postgres_controldata "${runtime}" "${source}")" || { "${runtime}" up -d --wait postgres; return 1; }
  if ! grep -qx 'Database cluster state: *shut down' <<<"${controldata}"; then
    printf 'postgres-data-move: the cluster did not shut down cleanly; not moved\n' >&2
    "${runtime}" up -d --wait postgres
    return 1
  fi
  install -d -m 0755 "${data%/*}"
  install -d -o 70 -g 70 -m 0700 "${data}"
  local differs="" links_before links_after
  if ! rsync -a --numeric-ids --delete "${source}/" "${data}/" \
    || ! differs="$(rsync -a -n -c --numeric-ids --delete --itemize-changes "${source}/" "${data}/")" \
    || [[ -n "${differs}" ]]; then
    printf 'postgres-data-move: the copy differs from the volume: %s\n' "${differs:-rsync failed}" >&2
    "${runtime}" up -d --wait postgres
    return 1
  fi
  copy_controldata="$(postgres_controldata "${runtime}" "${data}")" || copy_controldata=""
  links_before="$(find "${source}/pg_tblspc" -mindepth 1 -maxdepth 1 -printf '%f %l\n')" || links_before="unreadable"
  links_after="$(find "${data}/pg_tblspc" -mindepth 1 -maxdepth 1 -printf '%f %l\n')" || links_after="unreadable copy"
  links_before="$(sort <<<"${links_before}")"
  links_after="$(sort <<<"${links_after}")"
  if [[ "${copy_controldata}" != "${controldata}" || "${links_after}" != "${links_before}" ]]; then
    printf 'postgres-data-move: the copy'\''s control data or tablespace links differ; not moved\n' >&2
    "${runtime}" up -d --wait postgres
    return 1
  fi
  copied="$(du -sb "${data}")" || copied="unknown"
  copied="${copied%%[[:space:]]*}"

  date -u +%FT%TZ >"${marker}"
  # The wrapper reads the marker on every call: this `up` recreates Postgres on the bind.
  if ! "${runtime}" up -d --wait postgres || ! after="$(postgres_counts "${runtime}")" \
    || ! python3 -I -B - "${before}" "${after}" <<'PY'
import sys
before, after = ({line.rsplit(" ", 1)[0]: int(line.rsplit(" ", 1)[1]) for line in text.splitlines() if line.strip()}
                 for text in sys.argv[1:3])
wrong = [f"{name}: {count} before, {after.get(name)} after" for name, count in before.items()
         if after.get(name) is None or after[name] < count
         or (name == "public._sqlx_migrations" and after[name] != count)]
if wrong:
    print("postgres-data-move: the moved cluster holds less: " + "; ".join(wrong), file=sys.stderr)
    sys.exit(1)
print(f"postgres-data-move: {len(before)} counts hold after the move")
PY
  then
    rm -f "${marker}"
    "${runtime}" stop -t 300 postgres || true
    "${runtime}" up -d --wait postgres
    printf 'postgres-data-move: FAILED after the copy; Postgres serves from %s again and %s is left for a look\n' "${volume}" "${data}" >&2
    return 1
  fi
  printf 'postgres-data-move done: %s -> %s, %s bytes, Postgres down %ss (stop to healthy)\n' \
    "${volume}" "${data}" "${copied}" "$((SECONDS - started))"

  # A full backup of the cluster as it now lives (run-postgres-backup.sh reads the request and removes
  # it when the backup succeeded). It starts in the background and outlives the deploy.
  local evidence=/var/lib/foundation-platform/recovery
  if [[ -d "${evidence}" ]]; then
    install -o foundation-platform -g foundation-platform -m 0644 /dev/null "${evidence}/full-backup-requested" \
      && systemctl start --no-block foundation-postgres-backup.service \
      || printf 'postgres-data-move: could not ask for the full backup; the Sunday full backup takes one\n' >&2
  fi
  return 0
}
