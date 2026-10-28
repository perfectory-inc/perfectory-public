"""Render the dbt macro `foundation_current_rows` from the lakehouse contracts (root ADR-0267, ADR-0275).

A dbt model reads a Foundation Silver table through `{{ foundation_current_rows('<source name>') }}`:
the table's current rows by its contract, as `platform_contracts.current_rows` selects them for
Spark. The macro holds each source table's rule as the contract states it, so this script writes it
and a test keeps the checked-in macro equal to what it writes.

    python render_current_rows.py          write macros/foundation_current_rows.sql
    python render_current_rows.py --check  exit 1 when the checked-in macro differs
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCES = HERE / "models" / "sources" / "foundation_sources.yml"
MACRO = HERE / "macros" / "foundation_current_rows.sql"
sys.path.insert(0, str(HERE.parent / "spark" / "jobs"))

from platform_contracts import REMOVED, current_row_latest, current_row_predicate, load_lakehouse_contract  # noqa: E402


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
    if predicate is None and latest is None:
        return f"    '{name}': none,"
    parts = [f"'predicate': {predicate!r}" if predicate else "'predicate': none"]
    if latest is None:
        parts += ["'key': none", "'newest_first': none", "'removal': none"]
    else:
        parts += [f"'key': {list(latest.key)!r}", f"'newest_first': {list(latest.newest_first)!r}",
                  f"'removal': {latest.removal_column!r}" if latest.removal_column else "'removal': none"]
    return f"    '{name}': {{{', '.join(parts)}}},"


def render() -> str:
    rules = "\n".join(rule_literal(name, table) for name, table in foundation_tables(SOURCES.read_text(encoding="utf-8")))
    return f"""{{# Rendered by render_current_rows.py from the lakehouse contracts; do not edit (root ADR-0267, ADR-0275). #}}
{{% macro foundation_current_rows(name) %}}
{{%- set rules = {{
{rules}
}} -%}}
{{%- set relation = source('foundation', name) -%}}
{{%- set rule = rules[name] -%}}
{{%- if rule is none -%}}
{{{{ relation }}}}
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
    text = render()
    if argv[1:] == ["--check"]:
        if MACRO.read_text(encoding="utf-8") != text:
            print(f"{MACRO} is stale: run python render_current_rows.py", file=sys.stderr)
            return 1
        return 0
    MACRO.write_text(text, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
