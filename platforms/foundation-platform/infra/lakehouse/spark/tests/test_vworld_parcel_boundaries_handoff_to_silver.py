"""필지 핸드오프 잡이 다시 돌아도 같은 결과가 되게 하는 두 가지를 고정한다.

**적재 후 읽기 범위.** 이 잡은 쓰고 나서 표를 다시 읽어 확인한다. 읽기 범위가 이번
실행보다 넓으면 앞선 실행이 넣은 행까지 딸려 와 유일성 검사가 "같은 PNU 가 여러 개"라고
보고한다. 제대로 쓴 실행이 실패로 보고되고, 재시도가 같은 파일을 두 번 넣어 **진짜**
중복을 만든다. 2026-08-27 에 실제로 그렇게 됐다.

**이미 넣었다는 표시가 있는 곳.** 위를 고쳐도, 표시가 데이터와 다른 곳에 커밋되면
그 사이에서 죽은 실행이 둘을 어긋나게 둔다. 같은 날 그 틈으로 세 번 들어가 필지
1,865,891건이 표에 세 벌 쌓였다. 표시는 데이터와 같은 커밋에 있어야 한다 — Iceberg 의
Flink 싱크가 `flink.max-committed-checkpoint-id` 를 스냅숏 요약에 넣는 이유와 같다.

이 파일은 `unittest.TestCase` 여야 한다. 이 디렉터리의 러너는 xtask 의
`python3 -m unittest discover -s infra/lakehouse/spark/tests` 이고, unittest 는 모듈
수준의 `def test_*` 함수를 수집하지 않는다. 앞선 판(pytest 형식)은 CI 에서 0개가 수집돼
검사가 하나도 없는 채로 초록이었다.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
JOB_PATH = JOBS_DIR / "vworld_parcel_boundaries_handoff_to_silver.py"
sys.path.insert(0, str(JOBS_DIR))

# 이 import 가 곧 검사다. 잡이 pyspark 를 모듈 최상단에서 부르면 여기서 터지고, CI 레인에는
# pyspark 가 없으므로 아래 검사가 전부 조용히 건너뛰어진다. 앞선 판이 정확히 그랬다.
from vworld_parcel_boundaries_handoff_to_silver import (  # noqa: E402
    MAX_READBACK_SOURCE_RECORDS,
)


def job_source() -> str:
    return JOB_PATH.read_text(encoding="utf-8")


def function_body(source: str, name: str) -> str:
    """Return one top-level function's text, including the last one in the file."""
    start = source.index(f"def {name}")
    end = source.find("\ndef ", start + 1)
    return source[start:] if end == -1 else source[start:end]


class ReadBackScopeTest(unittest.TestCase):
    def test_readback_filters_on_the_source_object_not_the_provider_snapshot(self) -> None:
        """읽기는 Bronze 객체 단위여야 한다.

        `source_snapshot_id` 는 제공자 스냅숏 이름이라 한 시기의 전국 추출 255개가 모두 같다.
        그것으로 거르면 두 번째 적재가 첫 번째 적재의 행을 함께 읽는다.
        `source_record_id` 는 행이 온 Bronze 객체를 가리키므로 실행끼리 겹치지 않는다.
        """
        body = function_body(job_source(), "read_iceberg_snapshot_for_batch")

        self.assertIn(
            'F.col("source_record_id").isin(',
            body,
            "적재 후 읽기는 source_record_id 로 걸러야 한다",
        )
        self.assertNotIn(
            'F.col("source_snapshot_id").isin(',
            body,
            "source_snapshot_id 로 거르면 앞선 적재의 행까지 읽는다",
        )

    def test_readback_bounds_how_many_objects_one_run_may_append(self) -> None:
        """한 실행이 담을 수 있는 Bronze 객체 수에 상한이 있어야 한다.

        상한이 없으면 읽기 조건이 무한정 길어진다. 전국 필지는 255개 객체라 한 실행에
        다 담기지 않으며, 이 상한은 묶음 하나를 제한하는 것이지 데이터셋을 제한하지 않는다.
        """
        limit = MAX_READBACK_SOURCE_RECORDS

        self.assertIsInstance(limit, int, "상한이 정수로 선언돼 있어야 한다")
        self.assertGreater(limit, 0, "상한이 정수로 선언돼 있어야 한다")
        self.assertLess(
            limit,
            255,
            "상한이 전국 객체 수 이상이면 한 실행에 다 넣으라는 뜻이 되어 상한의 의미가 없다",
        )


class RetryIsIdempotentTest(unittest.TestCase):
    def test_the_already_ingested_marker_rides_in_the_write_commit(self) -> None:
        """표시는 데이터와 같은 커밋에 실려야 한다.

        Iceberg 는 `snapshot-property.<키>` 로 준 값을 그 쓰기가 만드는 스냅숏 요약에 함께
        커밋한다. 이 옵션 대신 파일이나 다른 표에 표시를 남기면 커밋이 둘로 갈라지고,
        그 사이에서 죽은 실행이 재시도 때 중복을 만든다.
        """
        body = function_body(job_source(), "write_silver_iceberg")

        self.assertIn(
            "snapshot_property_options(",
            body,
            "쓰기가 스냅숏 요약에 기록을 같이 커밋해야 한다",
        )
        self.assertIn(
            ".writeTo(",
            body,
            "INSERT INTO 는 snapshot-property 를 받지 못한다 — DataFrame writer 여야 한다",
        )
        self.assertNotIn(
            "INSERT INTO",
            body,
            "SQL 로 붙이면 옵션을 실을 자리가 없어 기록이 커밋 밖으로 나간다",
        )

    def test_the_write_path_asks_before_it_appends(self) -> None:
        """쓰기 앞에 확인이 있어야 한다.

        확인 없이 붙이면 토큰은 "무엇이 들어갔는지"를 기록만 할 뿐 두 번째 적재를 막지
        못한다. 막는 것은 붙이기 전에 묻는 이 순서다.
        """
        source = job_source()
        body = function_body(source, "main")

        self.assertIn("read_ingested_objects(", body, "붙이기 전에 물어야 한다")
        self.assertIn("decide_whether_to_append(", body, "답을 판단에 써야 한다")
        self.assertLess(
            body.index("decide_whether_to_append("),
            body.index("write_silver_iceberg("),
            "확인이 쓰기보다 먼저 와야 막을 수 있다",
        )


class ReadableAfterWriteTest(unittest.TestCase):
    """이 표는 쓰는 것으로 끝이 아니라 읽혀야 한다.

    이 잡은 쓴 것을 되읽어 검사하고, 이미 들어간 묶음을 건너뛸 때도 행을 센다. 둘 다 파일을
    여는 일이므로 표가 준비되기 전에 일어나면 안 된다.

    2026-08-28~30 에는 여기에 벡터화 읽기를 끄는 검사도 있었다. Iceberg 1.6.1 의 결함
    (root ADR-0064) 때문이었고, 1.11.0 으로 올려 이유가 사라져 함께 지웠다 (root ADR-0065).
    """

    def test_the_table_is_prepared_before_anything_reads_it(self) -> None:
        """건너뛰는 경로도 행을 읽는다.

        이미 들어간 묶음이면 잡은 붙이지 않고 행을 세는데, 그것도 파일을 읽는 일이다.
        준비가 쓰기 직전에만 있으면 그 읽기가 보호 없이 먼저 일어난다 — 실제로 그 자리에서
        세 번 죽었다.
        """
        body = function_body(job_source(), "main")

        self.assertIn("create_iceberg_table_if_missing(", body, "main 이 표를 준비해야 한다")
        self.assertLess(
            body.index("create_iceberg_table_if_missing("),
            body.index("read_iceberg_snapshot_for_batch("),
            "준비가 어떤 읽기보다도 먼저 와야 한다",
        )


if __name__ == "__main__":
    unittest.main()
