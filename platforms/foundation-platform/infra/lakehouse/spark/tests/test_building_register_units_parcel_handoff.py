"""단위 핸드오프 내보내기가 계약을 읽고, 매니페스트를 마지막에 쓰는지 고정한다.

이 잡의 독자는 Rust 적재기다. 둘의 합의 전부(접두사·매니페스트 키·칸 목록)는
`building-unit-handoff.json` 한 곳에 있고, 여기 시험은 잡이 그 합의를 자기 파일에 다시
적지 않았는지를 본다 — 같은 문자열이 세 곳에 있다가 하나가 빗나갔던 것이 접두사 사건이다
(ADR-0072, 가드 the-contract-names-where-its-objects-live 의 필지 사례).

이 파일은 `unittest.TestCase` 여야 한다 — 러너가 unittest discover 라서, pytest 형식의
모듈 함수는 0개 수집으로 초록이 된다.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
CONTRACT_PATH = JOBS_DIR.parents[1] / "contracts" / "building-unit-handoff.json"
sys.path.insert(0, str(JOBS_DIR))

# 이 import 가 곧 검사다: 잡이 pyspark 를 모듈 최상단에서 부르면 여기서 터진다.
from building_register_units_parcel_handoff import (  # noqa: E402
    JOB_NAME,
    MANIFEST_SCHEMA_VERSION,
    PNU_PATTERN,
    load_handoff_contract,
)


def job_source() -> str:
    return (JOBS_DIR / "building_register_units_parcel_handoff.py").read_text(encoding="utf-8")


class ContractIsTheOnlyAgreementTest(unittest.TestCase):
    def test_the_job_reads_the_real_contract(self) -> None:
        contract = load_handoff_contract()

        self.assertEqual(contract["schema_version"], 1)
        self.assertIn("register_pk", contract["columns"])
        self.assertIn("pnu", contract["columns"])
        self.assertIn("building_register_pk", contract["columns"])
        self.assertTrue(contract["manifest_object"].startswith(contract["handoff_prefix"]))

    def test_the_job_does_not_restate_the_prefix(self) -> None:
        """접두사를 잡에 다시 적으면 계약과 갈라진다 — 필지 사건 그대로."""
        contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        source = job_source()

        self.assertNotIn(
            f'"{contract["handoff_prefix"]}"',
            source,
            "the prefix belongs to the contract; the job must read it, not repeat it",
        )

    def test_the_manifest_version_names_this_dataset(self) -> None:
        self.assertIn("building_unit_handoff_manifest", MANIFEST_SCHEMA_VERSION)
        self.assertIn(JOB_NAME, ("building_register_units_parcel_handoff",))

    def test_the_pnu_rule_is_nineteen_digits(self) -> None:
        import re

        pattern = re.compile(PNU_PATTERN)
        self.assertIsNotNone(pattern.fullmatch("9999900000100000001"))
        self.assertIsNone(pattern.fullmatch("123"))
        self.assertIsNone(pattern.fullmatch("999990000010000000a"))


class ManifestIsWrittenLastTest(unittest.TestCase):
    def test_objects_are_written_before_the_manifest(self) -> None:
        """매니페스트는 약속이다: 그것이 있으면 객체들도 있다.

        먼저 쓰면 내보내기가 중간에 죽어도 적재기는 완전한 데이터가 있다고 믿는다. 소스
        본문에서 객체 쓰기가 매니페스트 쓰기보다 앞서는 것을 본다 — 실행 순서는 텍스트
        순서를 따르는 단일 함수다.
        """
        source = job_source()
        main_body = source[source.index("def main(") :]

        self.assertLess(
            main_body.index("write_objects("),
            main_body.index('contract["manifest_object"]'),
            "the manifest must be written after every object it names",
        )

    def test_left_behind_rows_reach_the_manifest(self) -> None:
        """세지 않고 떨어뜨린 행은 아무도 모르는 행이다 (ADR-0072 §3)."""
        source = job_source()

        for field in ("null_pnu_row_count", "invalid_pnu_row_count", "exported_row_count"):
            self.assertIn(field, source, f"the manifest must carry {field}")


class ExclusiveAreasOnlyTest(unittest.TestCase):
    def test_only_exclusive_areas_describe_a_unit(self) -> None:
        """공용면적을 합치면 호의 면적이 아니라 건물 지분이 된다."""
        source = job_source()

        self.assertIn('F.col("area_kind") == F.lit("exclusive")', source)
        self.assertNotIn('F.lit("common")', source)


if __name__ == "__main__":
    unittest.main()
