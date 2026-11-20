#!/usr/bin/env bash
# Sourced by every compose caller of the runtime project (foundation-runtime.sh and the backup
# scripts under scripts/recovery): where the runtime Postgres keeps its data directory (root ADR-0598).
#
#   postgres_data_source <platform root>
#
# Exports FOUNDATION_PLATFORM_POSTGRES_DATA, the bind docker-compose.yml and compose.recovery.yml mount
# at /var/lib/postgresql/data, once config/host-layout.contract.json postgres_data.moved_marker exists;
# before that it unsets it and compose mounts the named volume `pgdata` as it always has. Only the
# deploy writes the marker, after its copy of the volume checked (postgres-data-move.sh): a bind of an
# empty directory would start an empty cluster, so nothing else decides. The marker sits outside the
# data directory (postgres's own, 0700) so that every account that runs compose can see it.
#
# A restore drill runs its own project with its own volume and never sources this.
postgres_data_source() {
  local root="$1" layout
  unset FOUNDATION_PLATFORM_POSTGRES_DATA
  layout="$(python3 -I -c 'import json, sys
entry = json.load(open(sys.argv[1]))["postgres_data"]
print(entry["path"], entry["moved_marker"])' "${root}/config/host-layout.contract.json" 2>/dev/null)" || return 0
  local data marker
  read -r data marker <<<"${layout}"
  [[ -n "${data}" && -n "${marker}" && -e "${marker}" ]] || return 0
  export FOUNDATION_PLATFORM_POSTGRES_DATA="${data}"
}
