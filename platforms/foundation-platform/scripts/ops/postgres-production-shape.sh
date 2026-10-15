#!/usr/bin/env bash
# Prints config/postgres-production-shape.json for the running database (root ADR-0259 §5): which
# tables hold rows, by name, no value. Read-only (`select exists` per table). Run as root on the
# host, where the runtime compose wrapper can read its environment file, and commit the output:
#   sudo bash scripts/ops/postgres-production-shape.sh > config/postgres-production-shape.json
# The CI seed puts one synthetic row in each of these tables before a pull request's migrations.
set -euo pipefail

runtime="${FOUNDATION_PLATFORM_RELEASE_ROOT:-/opt/foundation-platform}/current/scripts/deploy/foundation-runtime.sh"
# \gexec runs each generated statement; each prints "<schema.table> true|false".
"${runtime}" exec -T postgres sh -c 'psql -X -q -A -t -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL' |
select format('select %L || '' '' || exists (select 1 from %I.%I)', n.nspname || '.' || c.relname, n.nspname, c.relname)
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where c.relkind in ('r', 'p') and not c.relispartition
  and n.nspname not in ('pg_catalog', 'information_schema', 'public', 'topology', 'tiger', 'tiger_data')
order by 1 \gexec
SQL
python3 -c '
import json, sys
from datetime import date
rows = sorted(line.split()[0] for line in sys.stdin if line.split()[-1:] == ["true"])
print(json.dumps({
    "schema_version": "foundation-platform.postgres_production_shape.v1",
    "decision": "root ADR-0259 §5",
    "captured_by": "platforms/foundation-platform/scripts/ops/postgres-production-shape.sh",
    "captured_at": date.today().isoformat(),
    "tables_with_rows": rows,
}, indent=2))'
