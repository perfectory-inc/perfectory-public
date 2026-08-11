#!/usr/bin/env python3
"""Render the structural indicators that docs/roadmap/foundation-goals.md is judged by.

Only statically derivable numbers belong here. A guard-chain pass count and a lane's executed
test count are facts about a *run*, and the open/closed split of recorded debt is a judgment; both
are owned elsewhere and are deliberately absent. A generator that reported them would be inventing
a number rather than measuring one.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs/roadmap/foundation-baseline.md"
# Bumped by a person when this page's shape changes, never by the generator.
LAST_REVIEWED = "2026-08-06"

MIGRATIONS = ROOT / "platforms/foundation-platform/migrations"
ADR_DIR = ROOT / "docs/adr"
# Where a production writer may live. Seeds and fixtures are excluded on purpose: the question is
# whether the *system* fills a table, and a fixture that fills it is what hides that it does not.
CODE_ROOTS = (
    ROOT / "platforms/foundation-platform/crates",
    ROOT / "platforms/foundation-platform/services",
)

CREATE_TABLE = re.compile(r"^CREATE TABLE (catalog\.[a-z_]+)", re.MULTILINE)
# A CHECK belongs to the table its own statement names. `ALTER TABLE` must be read too: the
# administrative ledger's status check is re-added by an ALTER inside a migration whose nearest
# preceding CREATE TABLE is a different table entirely — attributing by CREATE alone invented a
# status column on a table whose own comment says it deliberately has none.
TABLE_STATEMENT = re.compile(r"^(?:CREATE|ALTER) TABLE ([a-z_]+\.[a-z_]+)", re.MULTILINE)
# The constraint name is optional: `publication_revision`'s status check is written without one.
# Whitespace-tolerant on purpose. A re-declared constraint whose name sits on the previous line
# parsed as unnamed and became a second entry, so one table's single status rule was counted twice.
# Keying by name instead lets a later migration's declaration replace the earlier one, which is what
# an append-only migration set actually means.
STATUS_CHECK = re.compile(
    r"(?:CONSTRAINT\s+([a-z_]+)\s+)?CHECK\s*\([a-z_]*status IN \(([^)]*)\)\)"
)
# How far from a mention of the table a value literal may sit and still count as written to it.
# A status write and its table name live in one SQL statement; this bounds "one statement" without
# parsing SQL. Too small over-reports, which is the direction this measurement chooses.
WRITE_WINDOW = 400
QUOTED = re.compile(r"'([a-z_]+)'")
DEBT_HEADING = re.compile(r"^#{2,4}[ \t]*남은 부채(?:[ \t(].*)?$", re.MULTILINE)
DEBT_ITEM = re.compile(r"^\d+\.[ \t]+\*\*", re.MULTILINE)


def migration_sql() -> dict[Path, str]:
    return {path: path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS.glob("*.sql"))}


def production_sources() -> list[tuple[Path, str]]:
    """Every non-test Rust file, plus the migrations (SQL functions write rows too)."""
    files: list[tuple[Path, str]] = []
    for root in CODE_ROOTS:
        for path in sorted(root.rglob("*.rs")):
            parts = path.parts
            if "tests" in parts or "target" in parts:
                continue
            files.append((path, path.read_text(encoding="utf-8")))
    for path, text in migration_sql().items():
        files.append((path, text))
    return files


def tables_without_producer(sql: dict[Path, str], sources: list[tuple[Path, str]]):
    tables = sorted({match for text in sql.values() for match in CREATE_TABLE.findall(text)})
    missing = []
    for table in tables:
        needle = f"INSERT INTO {table}"
        # Word boundary: `catalog.parcel` must not be satisfied by `catalog.parcel_identifier`.
        pattern = re.compile(re.escape(needle) + r"\b")
        if not any(pattern.search(text) for _, text in sources):
            missing.append(table)
    return tables, missing


def statement_at(text: str, start: int) -> str:
    """The one writing statement beginning at `start`, cut at its own end.

    A fixed-width window crossed statement boundaries and swallowed whatever followed. In one
    migration that was an `ALTER TABLE ... CHECK (status IN (...))` six lines below an `UPDATE`, so
    the constraint's own list of admitted values was read as proof that something writes them.
    Statements end at `;` in SQL and at the closing quote of the Rust string that carries them.
    """
    end = len(text)
    for terminator in (";", '"'):
        found = text.find(terminator, start)
        if found != -1:
            end = min(end, found)
    return text[start : min(end, start + WRITE_WINDOW)]


def strip_sql_comments(text: str) -> str:
    """Removes `--` comments so prose about a value is not read as a write of it.

    Three migration comments discuss `superseded` and `published` by name, including one that says
    outright that neither has a writer. Left in, they would have answered their own question.
    """
    return re.sub(r"--[^\n]*", "", text)


def status_checks(sql: dict[Path, str]) -> dict[str, tuple[str, list[str]]]:
    """Each status CHECK, keyed by the table it belongs to.

    The table matters. Looking for a value anywhere in the tree lets `'failed'` written for the
    collection ledger stand in for `'failed'` on a projection load, which reports every status as
    reachable and is how the first version of this function returned a flattering zero.
    """
    found: dict[str, tuple[str, list[str]]] = {}
    for text in sql.values():
        starts = [(match.start(), match.group(1)) for match in TABLE_STATEMENT.finditer(text)]
        for match in STATUS_CHECK.finditer(text):
            owner = next(
                (name for offset, name in reversed(starts) if offset < match.start()), None
            )
            if owner is None:
                continue
            constraint = match.group(1) or f"{owner}.status"
            found[constraint] = (owner, QUOTED.findall(match.group(2)))
    return found


def unreachable_status_values(sql: dict[Path, str], sources: list[tuple[Path, str]]):
    """Status values a CHECK admits that nothing writes *to that table* outside tests.

    Text matching within a window around each mention of the owning table, not a parse. A value
    written through a variable, or further from its table name than the window, reads as
    unreachable here — the measurement over-reports and is corrected by looking, rather than
    under-reporting and being trusted.
    """
    admitted = status_checks(sql)
    stripped = [(path, strip_sql_comments(text)) for path, text in sources]
    unreachable = []
    for constraint, (owner, values) in sorted(admitted.items()):
        bare = re.escape(owner.split(".", 1)[1])
        # Only a statement that writes the table counts. Searching near the table *name* let the
        # CHECK's own definition — which always spells every admitted value — stand in as evidence
        # that something writes them, so whether a value looked reachable came down to how far its
        # constraint happened to sit from a `CREATE TABLE` line.
        writes = re.compile(rf"(?:INSERT INTO|UPDATE)\s+(?:[a-z_]+\.)?{bare}\b")
        windows = [
            statement_at(text, match.start()) for _, text in stripped for match in writes.finditer(text)
        ]
        for value in values:
            if not any(f"'{value}'" in window or f'"{value}"' in window for window in windows):
                unreachable.append((owner, constraint, value))
    return admitted, unreachable


def recorded_debt() -> tuple[int, int]:
    adrs = sorted(path for path in ADR_DIR.glob("[0-9]*.md"))
    items = 0
    for path in adrs:
        text = path.read_text(encoding="utf-8")
        match = DEBT_HEADING.search(text)
        if not match:
            continue
        items += len(DEBT_ITEM.findall(text[match.end() :]))
    return len(adrs), items


def render() -> str:
    sql = migration_sql()
    sources = production_sources()
    tables, missing = tables_without_producer(sql, sources)
    admitted, unreachable = unreachable_status_values(sql, sources)
    adr_count, debt_items = recorded_debt()

    lines = [
        # A constant, not today's date: the numbers below are regenerated from the tree, but
        # `last_reviewed` means "a human last looked at the shape of this page", and a clock here
        # would make `--check` fail every night for no change anyone made.
        "---",
        "status: current",
        "owner: repository-maintainers",
        "doc_type: catalog",
        f"last_reviewed: {LAST_REVIEWED}",
        "---",
        "",
        "<!-- GENERATED FILE. Do not edit by hand. -->",
        "<!-- Render with: python3 scripts/catalog/render-foundation-baseline.py -->",
        "",
        "# 기반 지표",
        "",
        "> [기반 목표](./foundation-goals.md)가 판정에 쓰는 수치입니다. 목표와 근거는 그 문서가",
        "> 소유하고, 이 파일은 수만 소유합니다.",
        "",
        "정적으로 재생산되는 수만 있습니다. 가드 통과 수와 레인 실행 테스트 수는 **실행해야 나오는**",
        "수이므로 실행 로그가 소유하고, 기록된 부채의 열림/닫힘 구분은 판정이므로 사람이 소유합니다.",
        "",
        "## G1 — 생산자 없는 canonical 표",
        "",
        f"- canonical 표: **{len(tables)}개**",
        f"- 그중 생산자 없음: **{len(missing)}개**",
        "",
        "테스트와 시드를 뺀 `INSERT INTO`가 하나도 없는 표입니다. 시드를 세지 않는 이유는, 표를",
        "채우는 fixture가 바로 시스템이 그 표를 채우지 않는다는 사실을 가리기 때문입니다.",
        "",
        "| 표 |",
        "|---|",
    ]
    lines += [f"| `{table}` |" for table in missing]

    lines += [
        "",
        "## G4 — 쓰이지 않는 상태값",
        "",
        f"- 상태 CHECK: **{len(admitted)}개**",
        f"- 그중 쓰는 경로가 없는 값: **{len(unreachable)}개**",
        "",
        "값 리터럴을 **그 표를 언급하는 자리 근처에서만** 찾습니다. 저장소 전체에서 찾으면 다른",
        "표에 쓰이는 같은 이름의 값이 대신 세어져, 모든 상태가 도달 가능하다는 답이 나옵니다.",
        "변수를 거쳐 쓰이거나 표 이름에서 멀리 떨어진 값은 도달 불가로 읽힙니다 — 과대 보고 쪽으로",
        "틀리며, 그것이 안전한 방향입니다.",
        "",
        "| 표 | 제약 | 값 |",
        "|---|---|---|",
    ]
    lines += [
        f"| `{owner}` | `{constraint}` | `{value}` |" for owner, constraint, value in unreachable
    ]

    lines += [
        "",
        "## 기록 규모",
        "",
        f"- ADR: **{adr_count}개**",
        f"- `남은 부채` 항목: **{debt_items}개**",
        "",
        "항목 수는 남은 일의 수가 아닙니다. 열림/닫힘은",
        "[운영 준비 작업 목록](./production-readiness.md)의 표가 소유합니다.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            print(f"stale foundation baseline: {OUTPUT}", file=sys.stderr)
            return 1
        return 0
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8", newline="\n")
    print(OUTPUT.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
