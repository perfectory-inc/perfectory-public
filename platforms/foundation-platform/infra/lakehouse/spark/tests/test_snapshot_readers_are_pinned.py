"""스냅숏을 id 로 읽는 코드는 모두 계약에 있고, 그 스냅숏을 무엇이 지키는지 말한다(root ADR-0338).

막는 위험(ADR-0001): 태그 없이 옛 스냅숏을 읽는 독자. 2026-10-29 감사에서 by-PNU 굽기의 고정 스냅숏
(ADR-0310)과 FLOOR 증인 스냅숏을 읽는 코드가 어떤 태그로도 지켜지지 않고, 적재 기록 만료 보호에 기대어
우연히 살아 있었다. 그 보호가 표 29개에서 30일 넘은 스냅숏 161개(데이터 약 24 GB)를 붙들고 있었다.
보호를 걷고 30일 보존을 되살리면, 태그 없는 독자는 다음 만료에 읽던 스냅숏을 잃는다.

그래서 스냅숏을 id 로 읽는 곳(`snapshot-id` 읽기 옵션, `VERSION AS OF`, Rust 의
`load_snapshot_manifest_list`)을 파일마다 세어, 계약 `lakehouse-maintenance.contract.json`
`snapshot_expiry.reader_pins.readers` 의 수와 맞춘다. 새로 읽는 곳은 그 목록에 들어가며 무엇이 그
스냅숏을 지키는지 적어야 한다: 독자 태그, 같은 실행의 머리(`head`), 또는 지켜지지 않는 이유.
"""

from __future__ import annotations

import os
import re
import sys
import unittest
from collections import Counter
from pathlib import Path

JOBS = Path(__file__).resolve().parents[1] / "jobs"
PLATFORM = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(JOBS))

from lakehouse_maintenance import load_maintenance_contract  # noqa: E402

# A read of one snapshot by id or time, in Spark, Trino or the Rust catalog client. `AS OF VERSION`
# (a tag or branch being made) is not a read.
READ = re.compile(
    r'option\(\s*"(?:start-|end-)?snapshot-id"'
    r"|VERSION AS OF"
    r"|TIMESTAMP AS OF"
    r"|as-of-timestamp"
    r"|\.load_snapshot_manifest_list\("
)
SOURCES = (".py", ".rs", ".sh")
SKIPPED_DIRS = {"tests", "target", "node_modules", "__pycache__", ".venv"}


def is_test(path: Path) -> bool:
    return path.name.startswith("test_") or path.name.endswith("_tests.rs") or path.name == "tests.rs"


def code_of(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    # A Rust file's own test module reads fixtures, not the lakehouse.
    return text.split("#[cfg(test)]", 1)[0] if path.suffix == ".rs" else text


def read_sites() -> Counter:
    found: Counter = Counter()
    for directory, subdirectories, files in os.walk(PLATFORM):
        subdirectories[:] = [name for name in subdirectories if name not in SKIPPED_DIRS and not name.startswith(".")]
        for name in files:
            path = Path(directory) / name
            if path.suffix not in SOURCES or is_test(path):
                continue
            count = len(READ.findall(code_of(path)))
            if count:
                found[path.relative_to(PLATFORM).as_posix()] = count
    return found


class SnapshotReadersArePinned(unittest.TestCase):
    def setUp(self) -> None:
        self.pins = load_maintenance_contract()["snapshot_expiry"]["reader_pins"]

    def test_every_snapshot_read_is_declared_with_what_keeps_it(self) -> None:
        declared = {reader["path"]: reader["reads"] for reader in self.pins["readers"]}
        found = read_sites()
        undeclared = {path: count for path, count in found.items() if declared.get(path) != count}
        self.assertEqual(
            undeclared, {},
            "these files read a snapshot by id and the contract does not say what keeps it "
            "(lakehouse-maintenance.contract.json snapshot_expiry.reader_pins.readers): pin it with a "
            "reader tag, read the head, or state why nothing has to keep it")
        gone = sorted(set(declared) - set(found))
        self.assertEqual(gone, [], "the contract names readers that no longer read a snapshot by id")

    def test_the_contract_names_the_tags_the_code_makes(self) -> None:
        import building_register_floor_history
        import gold_incremental

        tags = self.pins["tags"]
        self.assertIn(gold_incremental.INPUT_TAG_PREFIX, tags)
        self.assertIn(gold_incremental.BUILD_TAG, tags)
        self.assertIn(building_register_floor_history.WITNESS_TAG, tags)
        pins = (PLATFORM / "crates/publisher/foundation-publisher-by-pnu/src/by_pnu_serving_pins.rs").read_text(
            encoding="utf-8")
        for prefix in ("served-", "baking-"):
            self.assertIn(prefix, tags)
            self.assertIn(f'format!("{prefix}{{}}-"', pins, f"the Rust pins no longer make {prefix} tags")

    def test_the_scan_sees_each_kind_of_read(self) -> None:
        for line in ('spark.read.format("iceberg").option("snapshot-id", s)',
                     "SELECT * FROM t VERSION AS OF 1", "r2.t FOR VERSION AS OF 1",
                     "catalog.load_snapshot_manifest_list(table, id)",
                     '.option( "start-snapshot-id", a)'):
            self.assertTrue(READ.search(line), line)
        for line in ("ALTER TABLE t CREATE TAG `x` AS OF VERSION 1", '{"snapshot-id": head}'):
            self.assertFalse(READ.search(line), line)


if __name__ == "__main__":
    unittest.main()
