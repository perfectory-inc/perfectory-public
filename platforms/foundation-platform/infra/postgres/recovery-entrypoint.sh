#!/bin/sh
set -eu

prepare_runtime_dirs() {
    install -d -m 0750 -o postgres -g postgres /var/spool/pgbackrest /var/lock/pgbackrest
}

prepare_runtime_dirs

case "${1:-}" in
    pgbackrest-backup)
        shift
        exec gosu postgres pgbackrest --stanza=foundation "$@"
        ;;
    pgbackrest-restore)
        shift
        install -d -m 0700 -o postgres -g postgres /var/lib/postgresql/data
        chown -R postgres:postgres /var/lib/postgresql/data
        # The backup holds the bulk tablespaces too (root ADR-0082); pgBackRest recreates them under
        # this root, which a fresh volume leaves to root: 2026-11-08 the first weekly drill aborted on
        # `unable to create path '/tablespaces/parcel-bulk': Permission denied` (root ADR-0300 K).
        install -d -m 0700 -o postgres -g postgres /tablespaces
        chown postgres:postgres /tablespaces
        exec gosu postgres pgbackrest --stanza=foundation "$@"
        ;;
    *)
        exec docker-entrypoint.sh "$@"
        ;;
esac
