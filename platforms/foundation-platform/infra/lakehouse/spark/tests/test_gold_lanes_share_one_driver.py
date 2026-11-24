"""Gold 판 레인은 드라이버 하나를 쓴다: 스풀을 가르고 커밋의 차분을 부르는 코드는 공통 드라이버이거나, 까닭과 함께
여기 적혀 있다(root ADR-0615 §7).

막는 위험(ADR-0001): 레인마다 "얼마나 한 번에 싣는가"를 제 코드로 정하는 것. 2026-11-23 에 그림자 넷이 같은 날
멈췄다 — 토지소유 OOM 4/4, 필지 패널 OOM 1/1(시도 하나를 한 문장으로), 실거래 "Too many open files" 9/9(자체 스풀이
시군구마다 디렉터리를 열었다), 인허가 판정 시간 초과. 공통 판 빌드는 시도를 주었고 나누는 일은 레인마다 달랐다:
한 곳(공통 드라이버·자체 드라이버 다섯)마다 다른 답이었다.

그래서 스풀을 파티션으로 쓰는 곳(`PARTITION_BY`)과 커밋의 차분·준비를 부르는 곳(`versions::diff(`,
`versions::stage(`)을 파일마다 찾아, 공통 드라이버(`SHARED`)가 아니면 `DECLARED` 에 까닭이 있어야 한다. 새 파일이
그렇게 하면 실패한다. 적힌 파일이 더는 그렇게 하지 않아도 실패한다: 목록은 줄기만 한다.
"""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[4]
ROOTS = (
    PLATFORM / "crates" / "publisher",
    PLATFORM / "services" / "foundation-outbox-publisher" / "src",
)
SOURCES = (".rs", ".sql")
SKIPPED_DIRS = {"tests", "target"}

# A spool written by partition, or a call of the shared commit's diff or staging.
OWN_SPLIT = re.compile(r"PARTITION_BY|\bversions::diff\(|\bversions::stage\(")

# The shared driver: the one spool by 시도 and the one loop that diffs and stages.
SHARED = {
    "crates/publisher/foundation-publisher-silver/src/kind_versions_stage.rs",
    "services/foundation-outbox-publisher/src/kind_versions_build.rs",
}

# What still does it on its own, and why. Each line goes when its lane moves onto the shared driver.
DECLARED = {
    "crates/publisher/foundation-publisher-silver/src/real_transaction_versions_stage.rs":
        "spool_more of a stage on the shared driver: a deal is keyed by its address code, so the stage "
        "spools it once for every part that reads it (ADR-0615 §7 part_tables); the driver loads and splits it",
    "services/foundation-outbox-publisher/src/lineage_review_queue_versions_build.rs":
        "not a table by PNU: the steward queue is keyed by (unit, subject_code) and partitioned by unit, "
        "and is small (22,068 keys); it has no 시도 to split by",
    "services/foundation-outbox-publisher/src/place_id_versions_build.rs":
        "not a table by PNU: the place id tables are keyed and partitioned by (unit, scope); one read of "
        "the registry, then a region at a time",
}


def is_test(path: Path) -> bool:
    return path.name.endswith("_tests.rs") or path.name == "tests.rs"


def code_of(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    # A Rust file's own test module builds fixtures; it is not the lane's read.
    return text.split("#[cfg(test)]", 1)[0] if path.suffix == ".rs" else text


def own_splits() -> set[str]:
    found = set()
    for root in ROOTS:
        for directory, subdirectories, files in os.walk(root):
            subdirectories[:] = [name for name in subdirectories if name not in SKIPPED_DIRS]
            for name in files:
                path = Path(directory) / name
                if path.suffix in SOURCES and not is_test(path) and OWN_SPLIT.search(code_of(path)):
                    found.add(path.relative_to(PLATFORM).as_posix())
    return found


class GoldLanesShareOneDriver(unittest.TestCase):
    def test_a_file_that_splits_or_diffs_on_its_own_is_the_shared_driver_or_says_why(self):
        found = own_splits()
        self.assertTrue(SHARED <= found, f"the shared driver moved: {sorted(SHARED - found)} no longer spool or diff")
        undeclared = sorted(found - SHARED - set(DECLARED))
        self.assertEqual(
            undeclared, [],
            "these files partition a spool or call the commit's diff themselves: a lane supplies SQL and "
            "declarations and the shared driver (kind_versions_build, kind_versions_stage) splits, loops and "
            "commits (root ADR-0615 §7); move the lane onto it, or add the file to DECLARED with its reason",
        )

    def test_the_declared_list_only_shrinks(self):
        found = own_splits()
        stale = sorted(set(DECLARED) - found)
        self.assertEqual(stale, [], "these files no longer split or diff on their own: delete their DECLARED lines")
        for path, reason in DECLARED.items():
            self.assertGreater(len(reason), 30, f"{path} is declared without a reason")


if __name__ == "__main__":
    unittest.main()
