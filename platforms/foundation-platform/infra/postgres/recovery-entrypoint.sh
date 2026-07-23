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
        exec gosu postgres pgbackrest --stanza=foundation "$@"
        ;;
    *)
        exec docker-entrypoint.sh "$@"
        ;;
esac
