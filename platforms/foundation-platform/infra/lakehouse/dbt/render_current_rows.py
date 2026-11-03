"""Render the dbt macro `foundation_current_rows` from the lakehouse contracts (root ADR-0267, ADR-0275, ADR-0286).

A dbt model reads a Foundation Silver table through `{{ foundation_current_rows('<source name>') }}`:
the table's current rows by its contract, as `platform_contracts.current_rows` selects them for
Spark. The macro holds each source table's rule as the contract states it, so this script writes it
and a test keeps the checked-in macro equal to what it writes. It also writes the dbt test that a
current key is unique wherever the contract says it is (root ADR-0286 §4, dbt's `unique`): the
macro's `rank() = 1` keeps every tied row, which Spark's `current_rows` refuses instead.

    python render_current_rows.py          write the macro and the key test
    python render_current_rows.py --check  exit 1 when either checked-in file differs
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCES = HERE / "models" / "sources" / "foundation_sources.yml"
MACRO = HERE / "macros" / "foundation_current_rows.sql"
KEY_TEST = HERE / "tests" / "foundation_current_keys_are_unique.sql"
sys.path.insert(0, str(HERE.parent / "spark" / "jobs"))

from platform_contracts import (REMOVED, current_row_latest, current_row_predicate, load_lakehouse_contract,  # noqa: E402
                                row_versions)


def foundation_tables(text: str) -> list[tuple[str, str]]:
    """(source name, physical table) of every table of the `foundation` source, in file order."""
    block = text.split("  - name: foundation", 1)[1].split("\n  - name: ", 1)[0]
    found = []
    for match in re.finditer(r"      - name: (\w+)\n(?:        identifier: (\w+)\n)?", block):
        found.append((match.group(1), match.group(2) or match.group(1)))
    return found


def rule_literal(name: str, table: str) -> str:
    contract = load_lakehouse_contract(f"silver.{table}")
    predicate = current_row_predicate(contract)
    latest = current_row_latest(contract)
    versions = row_versions(contract)
    newest_edition = versions["kind"] == "editions" and versions["read"] == "newest"
    if predicate is None and latest is None and not newest_edition:
        return f"    '{name}': none,"
    parts = [f"'predicate': {predicate!r}" if predicate else "'predicate': none"]
    if latest is not None:
        parts += [f"'key': {list(latest.key)!r}", f"'newest_first': {list(latest.newest_first)!r}",
                  f"'removal': {latest.removal_column!r}" if latest.removal_column else "'removal': none"]
    elif newest_edition:
        # An Editions table's newest edition (per group), by each edition's maxima of newest_first.
        parts += ["'key': none", f"'newest_first': {list(versions['newest_first'])!r}", "'removal': none",
                  f"'edition': {versions['edition_column']!r}", f"'per': {list(versions['per'])!r}"]
    else:
        parts += ["'key': none", "'newest_first': none", "'removal': none"]
    return f"    '{name}': {{{', '.join(parts)}}},"


def unique_key(contract: dict) -> list[str] | None:
    """The key a table's current rows hold once each (root ADR-0286 §4), or None when they need not:
    `Replaced` with a key, and `LatestPerKey` that does not keep ties."""
    versions = row_versions(contract)
    if versions["kind"] == "replaced" and versions.get("key"):
        return list(versions["key"])
    latest = current_row_latest(contract)
    return list(latest.key) if latest is not None and not latest.keep_ties else None


def render_key_test(sources: list[tuple[str, str]]) -> str:
    checks = []
    for name, table in sources:
        key = unique_key(load_lakehouse_contract(f"silver.{table}"))
        if key:
            columns = ", ".join(key)
            text_key = " || '/' || ".join(f"cast({column} as varchar)" for column in key)
            checks.append(f"select '{name}' as source, {text_key} as current_key, count(*) as rows_of_key\n"
                          f"from {{{{ foundation_current_rows('{name}') }}}}\ngroup by {columns}\nhaving count(*) > 1")
    body = "\nunion all\n".join(checks) if checks else (
        "-- No foundation source declares a current key that must be unique (each keeps ties or has none).\n"
        "select 'none' as source, '' as current_key, 0 as rows_of_key where false")
    return ("-- Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0286 §4).\n"
            f"{body}\n")


def render() -> str:
    rules = "\n".join(rule_literal(name, table) for name, table in foundation_tables(SOURCES.read_text(encoding="utf-8")))
    return f"""{{# Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0267, ADR-0275, ADR-0286). #}}
{{% macro foundation_current_rows(name) %}}
{{%- set rules = {{
{rules}
}} -%}}
{{%- set relation = source('foundation', name) -%}}
{{%- set rule = rules[name] -%}}
{{%- if rule is none -%}}
{{{{ relation }}}}
{{%- elif rule['edition'] -%}}
{{#- The newest editions as one scalar array: a join to them let Trino build its hash on the table
    (2026-11-04, unit prices: 8 GB per-node limit), a one-row array cannot (9.6 s). `per` columns are
    required contract columns, so the row comparison sees no null. -#}}
{{%- set group = rule['per'] + [rule['edition']] -%}}
(
    select r.* from {{{{ relation }}}} as r
    where contains((
        select array_agg(row({{{{ group | join(', ') }}}})) from (
            select {{{{ group | join(', ') }}}}, rank() over (
                {{% if rule['per'] %}}partition by {{{{ rule['per'] | join(', ') }}}} {{% endif %}}order by {{% for column in rule['newest_first'] %}}max({{{{ column }}}}) desc nulls last{{% if not loop.last %}}, {{% endif %}}{{% endfor %}}
            ) as _edition_rank
            from {{{{ relation }}}}
            group by {{{{ group | join(', ') }}}}
        ) as editions
        where _edition_rank = 1
    ), row({{% for column in group %}}r.{{{{ column }}}}{{% if not loop.last %}}, {{% endif %}}{{% endfor %}}))
    {{% if rule['predicate'] %}}and ({{{{ rule['predicate'] }}}}){{% endif %}}
) as {{{{ name }}}}
{{%- else -%}}
(
    select * from (
        select *{{% if rule['key'] %}}, rank() over (
            partition by {{{{ rule['key'] | join(', ') }}}}
            order by {{% for column in rule['newest_first'] %}}{{{{ column }}}} desc nulls last{{% if not loop.last %}}, {{% endif %}}{{% endfor %}}
        ) as _current_row_rank{{% endif %}}
        from {{{{ relation }}}}
        {{% if rule['predicate'] %}}where {{{{ rule['predicate'] }}}}{{% endif %}}
    ) as ranked
    {{% if rule['key'] %}}where _current_row_rank = 1{{% endif %}}
    {{% if rule['removal'] %}}and ({{{{ rule['removal'] }}}} is null or {{{{ rule['removal'] }}}} <> '{REMOVED}'){{% endif %}}
) as {{{{ name }}}}
{{%- endif -%}}
{{% endmacro %}}
"""


def main(argv: list[str]) -> int:
    rendered = {MACRO: render(), KEY_TEST: render_key_test(foundation_tables(SOURCES.read_text(encoding="utf-8")))}
    if argv[1:] == ["--check"]:
        stale = [path for path, text in rendered.items() if path.read_text(encoding="utf-8") != text]
        for path in stale:
            print(f"{path} is stale: run python render_current_rows.py", file=sys.stderr)
        return 1 if stale else 0
    for path, text in rendered.items():
        path.write_text(text, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
