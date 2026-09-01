"""붙인 사실이 붙인 커밋 안에 남는지를 고정한다 (ADR-0062).

잡이 붙이고 나서 확인하는 동안, 행은 커밋됐는데 붙였다고 말해 주는 것은 아직 없다.
그 틈에서 죽은 실행은 적재기에게 "안 들어갔다"로 보이고, 재시도가 같은 것을 또 붙인다.
2026-08-27 에 그 틈이 세 번 열려 필지 1,865,891건이 표에 세 벌 쌓였다.

고치는 방법은 재시도를 똑똑하게 만드는 것이 아니라, 붙인 기록을 붙인 커밋 밖에 두지 않는
것이다. Iceberg 는 스냅숏 요약을 데이터 파일과 같은 커밋에 쓴다.

이 파일은 `unittest.TestCase` 여야 한다. 이 디렉터리의 러너는
`python3 -m unittest discover` 이고, 모듈 수준의 `def test_*` 함수는 수집되지 않는다.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

# 이 import 가 곧 검사다. 공용 모듈이 PySpark 를 최상단에서 부르면 여기서 터지고, 이 레인에는
# PySpark 가 없으므로 아래 검사가 전부 조용히 건너뛰어진다.
from lakehouse_ingest import (  # noqa: E402
    INGEST_BATCH_OBJECTS_KEY,
    INGEST_BATCH_TOKEN_KEY,
    SNAPSHOT_PROPERTY_PREFIX,
    append_batch_once,
    decide_whether_to_append,
    ingest_batch_token,
    read_ingested_objects,
    snapshot_property_options,
    table_holds_rows,
)


class FakeRow:
    def __init__(self, snapshot_id: int, summary: dict[str, str] | None) -> None:
        self.snapshot_id = snapshot_id
        self.summary = summary


class FakeSpark:
    """Enough of a SparkSession to answer the one question `read_ingested_objects` asks.

    A stand-in rather than a real session because the lane running these checks has no
    PySpark, and the behaviour under test is which summary key the answer comes from — not
    anything Spark computes.
    """

    def __init__(self, rows: list[FakeRow], table_exists: bool = True) -> None:
        self._rows = rows
        self._table_exists = table_exists
        self.catalog = self
        self.queries: list[str] = []

    def tableExists(self, name: str) -> bool:  # noqa: N802 - Spark's spelling
        return self._table_exists

    def sql(self, query: str) -> FakeSpark:
        self.queries.append(query)
        return self

    def collect(self) -> list[FakeRow]:
        return self._rows


class FakeRegistryRow:
    """A row of the batch's own source ids, as `frame.select(col).distinct()` yields them."""

    def __init__(self, value: str, column: str) -> None:
        setattr(self, column, value)


class FakeBatchFrame:
    """Enough of a DataFrame for `append_batch_once` to identify a batch and try to write it.

    Recording the write rather than performing one, because what is under test is whether the
    write is attempted at all.
    """

    def __init__(self, record_ids: list[str], column: str = "source_record_id") -> None:
        self._record_ids = record_ids
        self._column = column
        self.appended = False

    def select(self, *_columns: str) -> "FakeBatchFrame":
        return self

    def distinct(self) -> "FakeBatchFrame":
        return self

    def limit(self, _n: int) -> "FakeBatchFrame":
        return self

    def collect(self) -> list[FakeRegistryRow]:
        return [FakeRegistryRow(value, self._column) for value in self._record_ids]

    def writeTo(self, _table: str) -> "FakeBatchFrame":  # noqa: N802 - Spark's spelling
        return self

    def option(self, _key: str, _value: str) -> "FakeBatchFrame":
        return self

    def append(self) -> None:
        self.appended = True

    def overwritePartitions(self) -> None:  # noqa: N802 - Spark's spelling
        self.appended = True


class FakeAnsweringSpark:
    """Answers the registry query and the row query differently, the way Spark does.

    The single-answer fake above cannot express the state this guard exists for — rows present,
    registry empty — because that state is precisely two different answers.
    """

    def __init__(self, registry: list[FakeRow], total_records: int | None) -> None:
        self._registry = registry
        self._total = total_records
        self.catalog = self
        self._pending: list[FakeRow] = []

    def tableExists(self, _name: str) -> bool:  # noqa: N802 - Spark's spelling
        return True

    def sql(self, query: str) -> "FakeAnsweringSpark":
        if "refs" in query:
            self._pending = (
                [] if self._total is None else [FakeRow(1, {"total-records": str(self._total)})]
            )
        else:
            self._pending = self._registry
        return self

    def collect(self) -> list[FakeRow]:
        return self._pending


class EmptyRegistryTest(unittest.TestCase):
    """행이 있는데 기록이 없으면, 그것은 빈 표가 아니다.

    2026-09-01 실측: 표 6개 중 5개가 그 상태였고 합쳐서 133,583,046 행이었다. 안전장치가
    생기기 전에 실린 표들이라 기록이 없었고, 없는 기록은 "아무것도 안 실렸다"로 읽혔다.
    그대로 다시 돌리면 전부 한 벌 더 들어간다 (root ADR-0069).
    """

    def test_rows_without_a_registry_stop_the_append(self) -> None:
        spark = FakeAnsweringSpark(registry=[], total_records=113_813_264)
        frame = FakeBatchFrame(["a.zip"])

        with self.assertRaises(ValueError) as caught:
            append_batch_once(spark, frame, ["source_record_id"], "`c`.`s`.`t`")

        self.assertIn("records no ingested objects", str(caught.exception))
        self.assertFalse(frame.appended, "거절했다면 쓰기를 시도하지 않았어야 한다")

    def test_an_empty_table_is_not_refused(self) -> None:
        """처음 싣는 표에는 기록이 없는 것이 정상이다. 그것까지 막으면 아무것도 못 싣는다."""
        spark = FakeAnsweringSpark(registry=[], total_records=0)
        frame = FakeBatchFrame(["a.zip"])

        result = append_batch_once(spark, frame, ["source_record_id"], "`c`.`s`.`t`")

        self.assertTrue(result["appended"])
        self.assertTrue(frame.appended)

    def test_overwrite_is_not_refused(self) -> None:
        """덮어쓰기는 자기가 쓰는 것을 대체한다. 두 번 들어가는 물음이 생기지 않는다."""
        spark = FakeAnsweringSpark(registry=[], total_records=1_442)
        frame = FakeBatchFrame(["a.zip"])

        result = append_batch_once(
            spark, frame, ["source_record_id"], "`c`.`s`.`t`", write_mode="overwrite"
        )

        self.assertTrue(result["appended"])

    def test_the_row_count_comes_from_the_summary_not_from_a_count(self) -> None:
        """1억 행을 세려면 모든 파일을 연다. 물음은 "비었나" 하나뿐이다."""
        spark = FakeAnsweringSpark(registry=[], total_records=5)

        self.assertTrue(table_holds_rows(spark, "`c`.`s`.`t`", "c.s.t"))

        empty = FakeAnsweringSpark(registry=[], total_records=0)
        self.assertFalse(table_holds_rows(empty, "`c`.`s`.`t`", "c.s.t"))

        unknown = FakeAnsweringSpark(registry=[], total_records=None)
        self.assertFalse(table_holds_rows(unknown, "`c`.`s`.`t`", "c.s.t"))


class BatchIdentityTest(unittest.TestCase):
    def test_the_token_comes_from_content_not_from_order_or_time(self) -> None:
        """같은 객체를 담은 묶음은 언제 어떤 순서로 돌아도 같은 토큰이어야 한다.

        순번이었다면 재개한 적재기가 세 번째 실행에 "3"을 주는데, 두 번째가 들어갔는지에
        따라 그 "3"이 가리키는 객체가 달라진다.
        """
        self.assertEqual(ingest_batch_token(["a", "b", "c"]), ingest_batch_token(["a", "b", "c"]))
        self.assertEqual(
            ingest_batch_token(["c", "a", "b"]),
            ingest_batch_token(["a", "b", "c"]),
            "묶음의 정체는 파일을 훑은 순서가 아니라 담긴 객체다",
        )
        self.assertNotEqual(
            ingest_batch_token(["a", "b"]),
            ingest_batch_token(["a", "b", "c"]),
            "객체가 다른 묶음이 같은 토큰을 받으면 넣지 않은 것을 넣었다고 본다",
        )


class WriteOptionTest(unittest.TestCase):
    def test_the_options_carry_the_record_into_the_write_commit(self) -> None:
        """Iceberg 가 요약에 실어 주는 접두사로 나가야 한다.

        접두사를 틀리면 Iceberg 가 모르는 옵션이 되어 조용히 무시된다. 쓰기는 성공하고
        요약은 비고, 다음 실행은 아무것도 못 본 채 같은 것을 또 붙인다.
        """
        options = snapshot_property_options(["b.zip", "a.zip"], "deadbeef")

        self.assertEqual(
            set(options),
            {
                f"{SNAPSHOT_PROPERTY_PREFIX}{INGEST_BATCH_TOKEN_KEY}",
                f"{SNAPSHOT_PROPERTY_PREFIX}{INGEST_BATCH_OBJECTS_KEY}",
            },
        )
        self.assertEqual(
            options[f"{SNAPSHOT_PROPERTY_PREFIX}{INGEST_BATCH_OBJECTS_KEY}"],
            "a.zip,b.zip",
            "객체 목록은 정렬된 채 실려야 사람이 두 스냅숏을 눈으로 비교할 수 있다",
        )
        self.assertTrue(
            SNAPSHOT_PROPERTY_PREFIX.endswith("."),
            "접두사에 점이 빠지면 Iceberg 가 키를 잘라내지 못한다",
        )

    def test_an_object_name_holding_the_separator_is_refused(self) -> None:
        """구분자가 든 이름은 요약 안에서 두 개로 쪼개진다.

        쪼개지면 다음 실행이 있지도 않은 객체 둘을 이미 들어간 것으로 보고, 정작 진짜
        객체는 못 알아본다. 쓰기 전에 거절한다.
        """
        with self.assertRaises(ValueError) as caught:
            snapshot_property_options(["a,b.zip"], "deadbeef")

        self.assertIn("a,b.zip", str(caught.exception), "어느 이름이 문제인지 말해야 한다")


class ReadIngestedObjectsTest(unittest.TestCase):
    def test_the_answer_comes_from_the_object_list_not_from_the_batch_token(self) -> None:
        """요약에서 읽는 것은 객체 목록이어야 한다.

        토큰을 읽으면 묶는 크기를 바꾼 순간 같은 객체가 처음 보는 것이 되어 다시 들어간다.
        목록을 읽으면 묶는 법과 무관하게 그 객체가 이미 들어갔음을 알아본다.
        """
        token_only = FakeSpark([FakeRow(7, {INGEST_BATCH_TOKEN_KEY: "abc123"})])
        self.assertEqual(
            read_ingested_objects(token_only, "`c`.`n`.`t`", "c.n.t"),
            {},
            "토큰만 있는 스냅숏은 어떤 객체가 들어갔는지 말해 주지 않는다",
        )

        with_objects = FakeSpark(
            [
                FakeRow(7, {INGEST_BATCH_TOKEN_KEY: "abc", INGEST_BATCH_OBJECTS_KEY: "a.zip,b.zip"}),
                FakeRow(9, {INGEST_BATCH_OBJECTS_KEY: "c.zip"}),
            ]
        )
        self.assertEqual(
            read_ingested_objects(with_objects, "`c`.`n`.`t`", "c.n.t"),
            {"a.zip": 7, "b.zip": 7, "c.zip": 9},
            "객체마다 그것을 넣은 스냅숏이 나와야 사람이 짚어 갈 수 있다",
        )

    def test_summaries_without_our_record_are_passed_over(self) -> None:
        """우리가 남기지 않은 스냅숏도 표에 있다.

        표를 만든 커밋, 스키마를 바꾼 커밋, 이 규칙 이전의 적재가 모두 스냅숏을 남긴다.
        요약이 비었다고 터지면 기존 표에는 이 관문을 붙일 수 없다.
        """
        spark = FakeSpark([FakeRow(1, None), FakeRow(2, {}), FakeRow(3, {"other": "x"})])
        self.assertEqual(read_ingested_objects(spark, "`c`.`n`.`t`", "c.n.t"), {})

    def test_a_table_that_does_not_exist_yet_has_ingested_nothing(self) -> None:
        """첫 적재는 물을 표가 없다. 없는 표를 조회하면 터진다."""
        spark = FakeSpark([], table_exists=False)
        self.assertEqual(read_ingested_objects(spark, "`c`.`n`.`t`", "c.n.t"), {})
        self.assertEqual(spark.queries, [], "없는 표에는 질의를 보내지 않는다")


class AppendDecisionTest(unittest.TestCase):
    def test_the_decision_is_keyed_on_the_object_not_on_the_batch(self) -> None:
        """묶는 크기를 바꿔도 같은 객체가 두 번 들어가면 안 된다.

        묶음은 그날 적재기가 파일을 몇 개씩 묶었는지일 뿐 데이터의 성질이 아니다. 묶음
        단위로만 판단하면 같은 객체를 다르게 묶은 순간 표가 처음 보는 토큰이 되어 다시
        들어간다. 2026-08-28 에 2개짜리 증명 묶음 위로 8개짜리 적재 묶음이 올 뻔했다.
        """
        ingested = {"a.zip": 111, "b.zip": 111}

        self.assertEqual(decide_whether_to_append(ingested, ["a.zip", "b.zip"]), [])
        self.assertEqual(
            decide_whether_to_append({}, ["a.zip", "b.zip"]),
            ["a.zip", "b.zip"],
            "하나도 안 들어간 묶음은 전부 붙여야 한다",
        )
        self.assertEqual(
            decide_whether_to_append(ingested, ["c.zip"]),
            ["c.zip"],
            "겹치지 않는 묶음은 앞선 적재와 무관하다",
        )

    def test_a_batch_that_is_half_in_is_refused_rather_than_half_appended(self) -> None:
        """일부만 들어간 묶음은 재개가 아니라 사고다.

        적재기가 도중에 묶는 법을 바꿨거나 두 적재기가 같이 돌고 있다는 뜻이다. 남은 것만
        붙이면 그 실행이 쓴 행을 적재 후 읽기가 가둘 수 없다.
        """
        with self.assertRaises(ValueError) as caught:
            decide_whether_to_append({"a.zip": 111}, ["a.zip", "b.zip"])

        message = str(caught.exception)
        self.assertIn("a.zip", message, "이미 들어간 객체를 이름으로 말해야 한다")
        self.assertIn("b.zip", message, "아직 안 들어간 객체를 이름으로 말해야 한다")


class OneImplementationTest(unittest.TestCase):
    def test_no_job_spells_the_summary_keys_for_itself(self) -> None:
        """요약 키는 공용 모듈 한 곳에서만 적힌다.

        같은 문자열이 두 잡에 적히면 한쪽만 고쳐질 수 있고, 그때 두 잡은 같은 표를 놓고
        서로 다른 질문을 하게 된다. 감시자를 붙일 게 아니라 사본이 안 생기게 한다.

        목록을 여기 적지 않고 디렉터리에서 모으는 것도 같은 이유다 — 적어 둔 목록은
        새 잡이 생겨도 늘지 않는다.
        """
        offenders = []
        for path in sorted(JOBS_DIR.glob("*.py")):
            if path.name == "lakehouse_ingest.py":
                continue
            source = path.read_text(encoding="utf-8")
            if INGEST_BATCH_TOKEN_KEY in source or INGEST_BATCH_OBJECTS_KEY in source:
                offenders.append(path.name)

        self.assertEqual(
            offenders,
            [],
            "요약 키의 철자는 lakehouse_ingest.py 에만 있어야 한다",
        )

    def test_no_job_builds_the_snapshot_property_prefix_by_hand(self) -> None:
        """접두사도 한 곳에서만 적힌다.

        `snapshot-property.` 를 직접 이어 붙인 잡은 오타 하나로 옵션이 조용히 무시되고,
        쓰기는 성공한 채 요약만 비게 된다.
        """
        offenders = []
        for path in sorted(JOBS_DIR.glob("*.py")):
            if path.name == "lakehouse_ingest.py":
                continue
            if "snapshot-property." in path.read_text(encoding="utf-8"):
                offenders.append(path.name)

        self.assertEqual(
            offenders,
            [],
            "옵션 이름은 snapshot_property_options() 가 조립한다. "
            "이 검사는 글자만 보므로 주석이나 docstring 에 적어도 걸린다 — "
            "형태를 적지 말고 lakehouse_ingest 를 가리켜라",
        )


class OneEngineVersionTest(unittest.TestCase):
    """Iceberg 판 번호는 계약 파일 한 곳에만 적힌다 (root ADR-0064).

    열두 곳에 적혀 있었고, 그래서 올리려면 열두 곳을 고쳐야 했고, 그래서 아무도 안 올렸다.
    다섯 판이 지나가는 동안 1.6.1 에 머물렀고 그중 하나에 우리를 이틀 잡아먹은 수정이
    들어 있었다. 감시할 것은 "낡았는가"가 아니라 "사본이 생겼는가"다 — 새 판이 나오는 것은
    우리가 통제하지 못하지만, 사본이 생기는 것은 우리가 통제한다.
    """

    PATTERN = re.compile(r"iceberg-(?:spark-runtime-[\d.]+_[\d.]+|aws-bundle):\d")

    def test_only_the_contract_spells_the_iceberg_version(self) -> None:
        root = Path(__file__).resolve().parents[3]
        allowed = {
            "lakehouse-engine.contract.json",  # 정본
            "lakehouse_engine_contract.rs",  # 정본을 읽는 Rust
            "test_lakehouse_ingest.py",  # 이 검사 자신
        }
        offenders = []
        for path in root.rglob("*"):
            if not path.is_file() or path.name in allowed:
                continue
            if path.suffix not in {".py", ".rs", ".md", ".json", ".yml", ".yaml", ".sh", ".example"}:
                continue
            if any(part in {"target", "__pycache__", "node_modules", ".git"} for part in path.parts):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            if self.PATTERN.search(text):
                offenders.append(str(path.relative_to(root)))

        self.assertEqual(
            sorted(offenders),
            [],
            "Iceberg 판 번호는 lakehouse-engine.contract.json 에만 적는다. "
            "여기에 뜨는 파일은 iceberg_packages() 를 부르도록 바꿔라",
        )


if __name__ == "__main__":
    unittest.main()
