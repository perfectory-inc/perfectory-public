"""Silver 레인은 읽는 파일을 하나의 읽기로 연다: Bronze 객체(계획이 가져온 것)나 이미지가 쓴 실행 파일은
`duckdb_parquet_stage::line_parts` 의 `FetchedFile` 이고, 그 줄은 `read_line_parts`(조각) 나 `for_each_line`(한 줄씩)
으로만, 바이트는 까닭과 함께 `read_whole`/`open_whole` 로만 읽는다.

막는 위험(ADR-0001): 2026-11-24 21:35Z 경매 Silver 적재가 물건 검색 객체 하나(본문 394 MiB)를 통째로 단계에 올려
DuckDB 의 2 GiB 를 넘겼다. 레인마다 제 손으로 파일을 열면(gzip 을 풀어 문자열 하나로, DuckDB 의 `read_json` 으로) 얼마나
한 번에 싣는지를 레인마다 정한다. 구조로 막는다: 레인이 쥐는 `FetchedFile` 에는 경로를 꺼내는 길이 없다. 그 밖의 길은
여기서 찾는다.

1. gzip 을 풀거나 DuckDB 의 JSON 읽기를 직접 부르는 파일은 공통 읽기(`SHARED`)이거나 `DECLARED` 에 까닭이 있다.
   새 파일이 그렇게 하면 실패하고, 적힌 파일이 더는 그렇게 하지 않아도 실패한다(목록은 줄기만 한다).
2. 통째로 읽는 선언(`LineParts::Whole`, `read_whole`, `open_whole`)은 까닭에 잰 크기를 적는다(또는 Bronze 가 아니라고).
"""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[4]
ROOTS = (
    PLATFORM / "crates" / "publisher" / "foundation-publisher-silver" / "src",
    PLATFORM / "services" / "foundation-outbox-publisher" / "src",
)
SOURCES = (".rs", ".sql")
SKIPPED_DIRS = {"tests", "target"}

# A gzip decoder, or DuckDB's JSON readers in a statement.
OWN_READ = re.compile(
    r"\b(?:Multi)?GzDecoder\b|\bread_ndjson\w*\(|\bread_json_objects\w*\(|\bread_json(?:_auto)?\(\s*[\[']|newline_delimited"
)

# The shared reader.
SHARED = {
    "crates/publisher/foundation-publisher-silver/src/duckdb_parquet_stage/line_parts.rs",
}

# What still reads a file of its own, and why. None of them is a Silver lane's Bronze object.
DECLARED = {
    "crates/publisher/foundation-publisher-silver/src/release_compare_stage.rs":
        "not Bronze: a release compare reads the handoff parts its own run wrote (parquet or JSON Lines), "
        "a part at a time by the run's plan",
    "services/foundation-outbox-publisher/src/postgis_parcel_boundary_mirror_national_rebuild.rs":
        "not Bronze and not a Silver lane: the PostGIS mirror rebuild copies the parcel handoff objects of "
        "ADR-0067 into Postgres, one object at a time",
}

# A reason with a size measured on the lane's own files, or one that says the file is not Bronze.
MEASURED = re.compile(r"\d[\d,.]*\s*(?:MiB|MB|GiB|KiB|lines)\b|not Bronze")
# A Rust string literal; `\` then a line break continues it.
LITERAL = r'"((?:[^"\\]|\\[\s\S])*)"'
WHOLE_LITERAL = re.compile(r"LineParts::Whole\(\s*" + LITERAL)
WHOLE_NAMED = re.compile(r"(?:LineParts::Whole|\.read_whole|\.open_whole)\(\s*([A-Z][A-Z0-9_]*)\s*\)")
WHOLE_ANY = re.compile(r"LineParts::Whole\(|\.read_whole\(|\.open_whole\(")


def is_test(path: Path) -> bool:
    return path.name.endswith("_tests.rs") or path.name in {"tests.rs", "test_support.rs"}


def code_of(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    # A Rust file's own test module builds fixtures; it is not the lane's read.
    return text.split("#[cfg(test)]", 1)[0] if path.suffix == ".rs" else text


def lane_files():
    for root in ROOTS:
        for directory, subdirectories, files in os.walk(root):
            subdirectories[:] = [name for name in subdirectories if name not in SKIPPED_DIRS]
            for name in files:
                path = Path(directory) / name
                if path.suffix in SOURCES and not is_test(path):
                    yield path, code_of(path)


def own_readers() -> set[str]:
    return {path.relative_to(PLATFORM).as_posix() for path, code in lane_files() if OWN_READ.search(code)}


def whole_reasons() -> tuple[list[tuple[str, str]], list[str]]:
    """(file, reason) of every whole read, and the files with a whole read whose reason is not found."""
    found, unresolved = [], []
    for path, code in lane_files():
        relative = path.relative_to(PLATFORM).as_posix()
        if relative in SHARED or not WHOLE_ANY.search(code):
            continue
        reasons = [(relative, match.group(1)) for match in WHOLE_LITERAL.finditer(code)]
        for name in {match.group(1) for match in WHOLE_NAMED.finditer(code)}:
            const = re.search(r"const " + name + r": [^=]+=\s*(?:LineParts::Whole\(\s*)?" + LITERAL, code)
            if const is None:
                unresolved.append(f"{relative}: {name}")
            else:
                reasons.append((relative, const.group(1)))
        calls = len(WHOLE_ANY.findall(code))
        if not reasons or calls > len(WHOLE_LITERAL.findall(code)) + len(WHOLE_NAMED.findall(code)):
            unresolved.append(relative)
        found.extend(reasons)
    return found, unresolved


class SilverLanesReadThroughOneReader(unittest.TestCase):
    def test_a_file_that_reads_a_file_itself_is_the_shared_reader_or_says_why(self):
        found = own_readers()
        self.assertTrue(SHARED <= found, f"the shared reader moved: {sorted(SHARED - found)} reads nothing itself")
        undeclared = sorted(found - SHARED - set(DECLARED))
        self.assertEqual(
            undeclared, [],
            "these files decode gzip or call DuckDB's JSON readers themselves: a Silver lane reads its "
            "FetchedFile through duckdb_parquet_stage (read_line_parts, for_each_line, read_whole) and says "
            "only which lines go together; move the read there, or add the file to DECLARED with its reason",
        )

    def test_the_declared_list_only_shrinks(self):
        found = own_readers()
        stale = sorted(set(DECLARED) - found)
        self.assertEqual(stale, [], "these files no longer read a file themselves: delete their DECLARED lines")
        for path, reason in DECLARED.items():
            self.assertGreater(len(reason), 30, f"{path} is declared without a reason")

    def test_a_whole_read_names_its_measured_size(self):
        found, unresolved = whole_reasons()
        self.assertEqual(
            unresolved, [],
            "a whole read takes its reason as a string literal or a const of the same file",
        )
        self.assertTrue(found, "no whole read found: the search for them is broken")
        vague = sorted(f"{path}: {reason[:80]}" for path, reason in found if not MEASURED.search(reason))
        self.assertEqual(
            vague, [],
            "a whole read says how large the lane's largest file is, measured (MiB or lines), or that "
            "the file is not Bronze",
        )


if __name__ == "__main__":
    unittest.main()
