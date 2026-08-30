"""나누지 않는 표가 만드는 DDL 이 실행 가능한지 고정한다 (root ADR-0066).

작은 표는 나누지 않는 것이 맞다. 산업단지 경계는 1,343행 8 MB 를 371칸으로 나눠 20 KB 짜리
파일 371개가 됐고, 합치기는 칸을 못 넘으므로 고칠 수 없었다.

그런데 나누기를 비우면 `PARTITIONED BY ()` 라는 문법 오류가 나온다. 조각 목록을 이어붙이는
쪽은 그것을 알 수 없고, 표를 만들려는 순간에야 터진다. 절을 통째로 만들어 주는 함수가
그 자리를 맡는다.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from platform_contracts import (  # noqa: E402
    load_lakehouse_artifact,
    partition_clause_sql,
    partition_spec,
)


class PartitionClauseTest(unittest.TestCase):
    def test_an_unpartitioned_contract_produces_no_clause(self) -> None:
        """빈 나누기는 절이 아예 없어야 한다. 빈 괄호는 문법 오류다."""
        self.assertEqual(partition_clause_sql({"partition_spec": []}), "")

    def test_a_partitioned_contract_produces_the_clause(self) -> None:
        self.assertEqual(
            partition_clause_sql({"partition_spec": ["sigungu_code"]}),
            "PARTITIONED BY (sigungu_code)",
        )
        self.assertEqual(
            partition_clause_sql({"partition_spec": ["a", "bucket(32, b)"]}),
            "PARTITIONED BY (a, bucket(32, b))",
        )

    def test_no_contract_would_emit_empty_parentheses(self) -> None:
        """실물 계약 전부에 대해 확인한다.

        표 하나를 나누지 않기로 바꿀 때 그 잡이 절을 조립하는 옛 방식으로 남아 있으면 이
        검사가 잡는다. 목록을 여기 적지 않고 계약에서 모으는 것도 같은 이유다 — 적어 둔
        목록은 새 표가 생겨도 늘지 않는다.
        """
        contracts = load_lakehouse_artifact()["contracts"]
        for table_name, contract in contracts.items():
            with self.subTest(table=table_name):
                clause = partition_clause_sql(contract)
                self.assertNotIn("()", clause, f"{table_name} 이 빈 괄호를 만든다")
                if partition_spec(contract):
                    self.assertTrue(clause.startswith("PARTITIONED BY ("))
                else:
                    self.assertEqual(clause, "")

    def test_every_job_builds_the_clause_instead_of_wrapping_the_field_list(self) -> None:
        """잡이 절을 직접 조립하면 빈 나누기에서 문법 오류를 만든다.

        `PARTITIONED BY ({partition_spec_sql(...)})` 는 조각이 있을 때만 맞는 문장이다.
        나누지 않는 표가 생기는 순간 그 잡은 표를 못 만든다.
        """
        offenders = []
        for path in sorted(JOBS_DIR.glob("*.py")):
            source = path.read_text(encoding="utf-8")
            if "PARTITIONED BY ({partition_spec_sql(" in source:
                offenders.append(path.name)

        self.assertEqual(
            offenders,
            [],
            "절은 partition_clause_sql() 이 만든다 — 조각 목록을 괄호로 감싸지 마라",
        )


if __name__ == "__main__":
    unittest.main()
