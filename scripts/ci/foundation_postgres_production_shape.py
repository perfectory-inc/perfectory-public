#!/usr/bin/env python3
"""A pull request's new migrations run onto a database shaped like production (root ADR-0259 §8).

  postgres_production_shape.py --database-url URL --base SHA --head SHA [--repository DIR]

1. The migrations the pull request adds are the files under migrations/ the base commit lacks. With
   none, nothing runs (a pull request that adds no migration pays a second, not a database).
2. Every other migration is applied in order, each in one transaction, as the production runner
   applies them. That schema is main's, the one production holds after its deploy.
3. Every table production holds rows in (config/postgres-production-shape.json, captured read-only
   by scripts/ops/postgres-production-shape.sh) gets one synthetic row, parents before children: a
   value for each column by its type, a CHECK's own values where it names them, the hints file's
   value where it does not. No row is real: text is `synthetic`, codes are the reserved 99999 band.
4. The new migrations are applied one at a time onto those rows. The first that fails names itself.

On 2026-10-13 and 10-14 two migrations that passed on CI's empty database refused or failed in the
deploy, where the tables held rows; every DAG stayed paused behind them.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

AREA = Path(__file__).resolve().parents[2] / "platforms/foundation-platform"
MIGRATIONS = AREA / "migrations"
SHAPE = AREA / "config/postgres-production-shape.json"
HINTS = AREA / "config/postgres-seed-hints.json"
SHAPE_SCHEMA = "foundation-platform.postgres_production_shape.v1"

# One synthetic value per type, in SQL. Character columns are cut to their length.
TYPE_VALUES = {
    "text": "'synthetic'", "character varying": "'synthetic'", "character": "'9'", "citext": "'synthetic'",
    "smallint": "1", "integer": "1", "bigint": "1", "numeric": "1", "real": "1", "double precision": "1",
    "boolean": "true", "uuid": "'00000000-0000-4000-8000-000000000001'",
    "timestamp with time zone": "'2026-01-01T00:00:00Z'", "timestamp without time zone": "'2026-01-01T00:00:00'",
    "date": "'2026-01-01'", "time without time zone": "'00:00:00'", "interval": "'1 day'",
    "jsonb": "'{}'", "json": "'{}'", "bytea": "'\\x00'", "inet": "'192.0.2.1'", "cidr": "'192.0.2.0/24'",
    "tstzrange": "tstzrange('2026-01-01T00:00:00Z', null)", "daterange": "daterange('2026-01-01', null)",
    "int4range": "int4range(1, null)", "int8range": "int8range(1, null)",
}
# A geometry by its kind, in WGS84 near the synthetic origin; the SRID comes from the column.
GEOMETRY = {
    "POINT": "POINT(127 37)", "MULTIPOINT": "MULTIPOINT((127 37))",
    "LINESTRING": "LINESTRING(127 37,127.001 37.001)",
    "POLYGON": "POLYGON((127 37,127.001 37,127.001 37.001,127 37,127 37))",
    "MULTIPOLYGON": "MULTIPOLYGON(((127 37,127.001 37,127.001 37.001,127 37)))",
    "GEOMETRY": "POLYGON((127 37,127.001 37,127.001 37.001,127 37))",
}


class Database:
    def __init__(self, url: str) -> None:
        self.url = url

    def run(self, sql: str, *, single_transaction: bool = False) -> subprocess.CompletedProcess:
        command = ["psql", self.url, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"]
        if single_transaction:
            command.append("-1")
        return subprocess.run(command, input=sql, capture_output=True, text=True, check=False, timeout=600)

    def rows(self, sql: str) -> list:
        result = self.run(sql)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
        text = result.stdout.strip()
        return json.loads(text) if text else []


COLUMNS = """
select coalesce(json_agg(json_build_object(
  'schema', n.nspname, 'table', c.relname, 'column', a.attname, 'not_null', a.attnotnull,
  'has_default', a.atthasdef or a.attidentity <> '' or a.attgenerated <> '',
  'generated', a.attgenerated::text <> '' or a.attidentity = 'a',
  'type', format_type(a.atttypid, a.atttypmod), 'base', coalesce(bt.typname, t.typname),
  'udt', case when coalesce(bt.typtype, t.typtype) = 'e' then coalesce(bt.oid, t.oid)::regtype::text end,
  'category', format_type(coalesce(nullif(t.typbasetype, 0), a.atttypid), coalesce(nullif(t.typtypmod, -1), a.atttypmod)),
  'element', case when t.typcategory = 'A' then format_type(t.typelem, null) end,
  'length', case when a.atttypmod > 4 and t.typname in ('varchar', 'bpchar') then a.atttypmod - 4 end
) order by n.nspname, c.relname, a.attnum), '[]')
from pg_attribute a join pg_class c on c.oid = a.attrelid join pg_namespace n on n.oid = c.relnamespace
join pg_type t on t.oid = a.atttypid left join pg_type bt on bt.oid = t.typbasetype and t.typtype = 'd'
where a.attnum > 0 and not a.attisdropped and c.relkind in ('r', 'p')
  and n.nspname not in ('pg_catalog', 'information_schema')
"""

FOREIGN_KEYS = """
select coalesce(json_agg(json_build_object(
  'schema', n.nspname, 'table', c.relname,
  'parent_schema', pn.nspname, 'parent', p.relname,
  'columns', (select json_agg(attname order by k.ord) from unnest(con.conkey) with ordinality k(num, ord)
              join pg_attribute on attrelid = con.conrelid and attnum = k.num),
  'parent_columns', (select json_agg(attname order by k.ord) from unnest(con.confkey) with ordinality k(num, ord)
                     join pg_attribute on attrelid = con.confrelid and attnum = k.num))), '[]')
from pg_constraint con join pg_class c on c.oid = con.conrelid join pg_namespace n on n.oid = c.relnamespace
join pg_class p on p.oid = con.confrelid join pg_namespace pn on pn.oid = p.relnamespace
where con.contype = 'f'
"""

CHECKS = """
select coalesce(json_agg(json_build_object(
  'schema', n.nspname, 'table', c.relname, 'name', con.conname, 'definition', pg_get_constraintdef(con.oid),
  'columns', (select json_agg(attname) from unnest(con.conkey) k(num)
              join pg_attribute on attrelid = con.conrelid and attnum = k.num))), '[]')
from pg_constraint con join pg_class c on c.oid = con.conrelid join pg_namespace n on n.oid = c.relnamespace
where con.contype = 'c'
"""

ENUM_FIRST = "select coalesce(json_object_agg(t, l), '{}') from (select enumtypid::regtype::text t, " \
             "(array_agg(enumlabel order by enumsortorder))[1] l from pg_enum group by 1) e"


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def regex_example(pattern: str) -> str | None:
    """A string an anchored regex of literals, classes and counts matches (`^[0-9a-f]{64}$`), or
    None for anything else (alternation, groups): the hints file names that column's value."""
    if not (pattern.startswith("^") and pattern.endswith("$")):
        return None
    out, index, body = [], 0, pattern[1:-1]
    while index < len(body):
        char = body[index]
        if char == "[":
            end = body.index("]", index + 2)
            atom, index = body[index + 1], end + 1
            if atom == "^":
                return None
        elif char == "\\":
            atom = {"d": "9", "w": "a", "s": " "}.get(body[index + 1], body[index + 1])
            index += 2
        elif char in "()|.":
            return None
        else:
            atom, index = char, index + 1
        count = 1
        if index < len(body) and body[index] in "*+?{":
            if body[index] == "{":
                end = body.index("}", index)
                count = max(1, int(body[index + 1:end].split(",")[0] or 1))
                index = end + 1
            else:
                count = 0 if body[index] in "*?" else 1
                index += 1
        out.append(atom * count)
    return "".join(out)


def check_value(column: str, checks: list[dict]) -> str | None:
    """A value the table's CHECKs name for the column itself: the first of `col = ANY (ARRAY[...])`,
    `col = '...'`, or an example of `col ~ '<regex>'`. A comparison inside a function call
    (`jsonb_typeof(col) = 'object'`) says nothing of the column's own value."""
    bare = rf"(?<![\w.]){re.escape(column)}\)*(?:::\w+)?"

    def first(pattern: str, definition: str) -> str | None:
        for found in re.finditer(bare + pattern, definition):
            if not re.search(r"\w\(+$", definition[: found.start()]):
                return found.group(1).replace("''", "'")
        return None

    for check in checks:
        definition = check["definition"]
        value = first(r" = ANY \(+ARRAY\['((?:[^']|'')*)'", definition) or first(r" = '((?:[^']|'')*)'", definition)
        if value is not None:
            return quote(value)
        pattern = first(r" ~ '((?:[^']|'')*)'", definition)
        if pattern is not None and (example := regex_example(pattern)) is not None:
            return quote(example)
    return None


def column_value(column: dict, checks: list[dict], enums: dict, hints: dict) -> str:
    key = f"{column['schema']}.{column['table']}.{column['column']}"
    if key in hints:
        return hints[key]
    from_check = check_value(column["column"], checks)
    if from_check is not None:
        return from_check
    if column["udt"]:
        return quote(enums[column["udt"]])
    if column["element"]:
        return "'{}'"
    kind = column["category"]
    if kind.startswith("geometry"):
        found = re.match(r"geometry\((\w+?)(Z|M|ZM)?(?:,(\d+))?\)", kind.replace(" ", ""))
        shape, srid = (found.group(1).upper(), found.group(3) or "0") if found else ("GEOMETRY", "0")
        geometry = f"ST_GeomFromText('{GEOMETRY.get(shape, GEOMETRY['GEOMETRY'])}', 4326)"
        return geometry if srid in ("0", "4326") else f"ST_Transform({geometry}, {srid})"
    base = re.sub(r"\(.*\)", "", kind).strip()
    value = TYPE_VALUES.get(base)
    if value is None:
        raise LookupError(f"{key}: no synthetic value for type {kind}; add one to {HINTS.name}")
    if column["length"] and value.startswith("'") and base != "character":
        value = quote(value.strip("'")[: column["length"]])
    if base == "character" and column["length"]:
        value = quote("9" * column["length"])
    return value


def seed(db: Database, tables: list[str], hints: dict, settings: dict) -> list[str]:
    """One synthetic row in each of `tables` and in every parent they reference. Returns failures.
    `settings` are the session settings a writer runs under (the publisher's capability flags)."""
    session = "".join(f"set {name} = {quote(value)};\n" for name, value in settings.items())
    columns, keys, checks = db.rows(COLUMNS), db.rows(FOREIGN_KEYS), db.rows(CHECKS)
    enums = db.rows(ENUM_FIRST)
    by_table: dict[str, list[dict]] = {}
    for column in columns:
        by_table.setdefault(f"{column['schema']}.{column['table']}", []).append(column)
    parents: dict[str, list[dict]] = {}
    for key in keys:
        parents.setdefault(f"{key['schema']}.{key['table']}", []).append(key)
    order: list[str] = []

    def visit(table: str, path: tuple[str, ...]) -> None:
        if table in order or table in path:
            return
        for key in parents.get(table, []):
            parent = f"{key['parent_schema']}.{key['parent']}"
            if parent != table:
                visit(parent, (*path, table))
        order.append(table)

    for table in tables:
        if table in by_table:
            visit(table, ())
    failures, seeded = [], set()
    for table in order:
        schema, name = table.split(".", 1)
        table_checks = [check for check in checks if f"{check['schema']}.{check['table']}" == table]
        nullable = {column["column"] for column in by_table[table] if not column["not_null"]}
        generated = {column["column"] for column in by_table[table] if column["generated"]}
        defaulted = {column["column"] for column in by_table[table] if column["has_default"]} - generated
        keyed = {child for key in parents.get(table, []) for child in key["columns"]}
        values: dict[str, str] = {}
        for column in by_table[table]:
            if column["column"] not in generated:
                values[column["column"]] = column_value(column, table_checks, enums, hints)
        for key in parents.get(table, []):
            parent = f"{key['parent_schema']}.{key['parent']}"
            pairs = [(child, parent_column) for child, parent_column in zip(key["columns"], key["parent_columns"])
                     if child not in generated and f"{table}.{child}" not in hints]
            if parent in seeded:
                for child, parent_column in pairs:
                    values[child] = f"(select {parent_column} from {parent} limit 1)"
            elif all(child in nullable for child, _ in pairs):
                # A parent not seeded first (a cycle, or one that failed): a nullable key is left empty.
                for child, _ in pairs:
                    values[child] = "null"
        error = ""
        # A CHECK this cannot read is met by leaving its nullable columns empty, one try per CHECK.
        for _ in range(len(table_checks) + 1):
            insert = (f"insert into {schema}.{name} ({', '.join(values)}) "
                      f"values ({', '.join(values.values())});")
            result = db.run(session + insert)
            if result.returncode == 0:
                error = ""
                seeded.add(table)
                break
            error = result.stderr.strip()
            named = re.search(r'check constraint "([^"]+)"', error)
            emptied = False
            for check in table_checks:
                if not named or check["name"] != named.group(1):
                    continue
                for column in check["columns"] or []:
                    if column in nullable and values.get(column) != "null" and f"{table}.{column}" not in hints:
                        values[column] = "null"
                        emptied = True
            if not emptied and not all(values[column] == "null" for column in nullable
                                       if column in values and f"{table}.{column}" not in hints):
                # A row that begins its life (a run inserted as planned, a release that readdresses
                # nothing) leaves its optional columns empty and its defaulted ones to their default.
                for column in nullable:
                    if column in values and f"{table}.{column}" not in hints:
                        values[column] = "null"
                for column in defaulted - keyed:
                    if f"{table}.{column}" not in hints:
                        values.pop(column, None)
            elif not emptied:
                break
        if error:
            failures.append(f"{table}: {error.splitlines()[0]}")
    return failures




def seed_problems(failures: list[str], not_yet_seeded: dict[str, str], tables: list[str]) -> list[str]:
    """A table the seed cannot fill is a gap only where not_yet_seeded names it; a named table that
    seeds now, or one production no longer holds rows in, is a stale entry. The list only shrinks."""
    failed = {failure.split(":", 1)[0]: failure for failure in failures}
    problems = [f"{failure} (give its column a value in {HINTS.name})"
                for table, failure in sorted(failed.items()) if table not in not_yet_seeded]
    problems += [f"{table} seeds now; remove it from not_yet_seeded"
                 for table in sorted(not_yet_seeded) if table not in failed and table in tables]
    problems += [f"{table} is in not_yet_seeded but production holds no rows in it; remove it"
                 for table in sorted(not_yet_seeded) if table not in tables]
    return problems


def apply(db: Database, path: Path, what: str) -> bool:
    result = db.run(path.read_text(encoding="utf-8"), single_transaction=True)
    if result.returncode != 0:
        print(f"postgres-production-shape: {path.name} {what}:\n{result.stderr.strip()}", file=sys.stderr)
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--base", required=True, help="the commit whose migrations production holds")
    parser.add_argument("--repository", default=str(AREA.parents[1]))
    parser.add_argument("--try", dest="tries", action="append", default=[], type=Path,
                        help="a migration file to apply after the new ones (the self-test plants one)")
    args = parser.parse_args()
    listed = subprocess.run(["git", "-C", args.repository, "ls-tree", "--name-only", args.base,
                             f"{MIGRATIONS.relative_to(AREA.parents[1]).as_posix()}/"],
                            capture_output=True, text=True, check=True).stdout.split()
    at_base = {Path(path).name for path in listed}
    files = sorted(MIGRATIONS.glob("*.sql"))
    new = [path for path in files if path.name not in at_base] + list(args.tries)
    if not new:
        print("postgres-production-shape: no new migration; nothing to apply onto rows")
        return 0
    db = Database(args.database_url)
    for path in files:
        if path not in new and not apply(db, path, "(main's) fails on an empty database"):
            return 1
    shape = json.loads(SHAPE.read_text(encoding="utf-8"))
    if shape.get("schema_version") != SHAPE_SCHEMA:
        raise ValueError(f"{SHAPE.name} schema_version is not {SHAPE_SCHEMA}")
    hints = json.loads(HINTS.read_text(encoding="utf-8"))
    tables = shape["tables_with_rows"]
    failures = seed(db, tables, hints["values"], hints["session_settings"])
    problems = seed_problems(failures, hints["not_yet_seeded"], tables)
    for problem in problems:
        print(f"postgres-production-shape: SEED {problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"postgres-production-shape: {len(tables) - len(failures)} of the {len(tables)} tables production "
          f"holds rows in hold a synthetic row ({len(failures)} not yet: {HINTS.name} not_yet_seeded)")
    for path in new:
        if not apply(db, path, "fails on tables that hold rows, as production's do"):
            return 1
        print(f"postgres-production-shape: {path.name} applied onto rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
