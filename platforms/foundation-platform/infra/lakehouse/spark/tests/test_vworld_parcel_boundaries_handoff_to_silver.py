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
    INGEST_BATCH_TOKEN_KEY,
    MAX_READBACK_SOURCE_RECORDS,
    ingest_batch_token,
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
    def test_batch_token_is_derived_from_content_not_from_order_or_time(self) -> None:
        """같은 객체를 담은 묶음은 언제 어떤 순서로 돌아도 같은 토큰이어야 한다.

        순번이었다면 재개한 적재기가 세 번째 실행에 "3"을 주는데, 두 번째가 들어갔는지에
        따라 그 "3"이 가리키는 객체가 달라진다. 내용에서 뽑으면 그런 어긋남이 안 생긴다.
        """
        token = ingest_batch_token

        self.assertEqual(token(["a", "b", "c"]), token(["a", "b", "c"]))
        self.assertEqual(
            token(["c", "a", "b"]),
            token(["a", "b", "c"]),
            "묶음의 정체는 파일을 훑은 순서가 아니라 담긴 객체다",
        )
        self.assertNotEqual(
            token(["a", "b"]),
            token(["a", "b", "c"]),
            "객체가 다른 묶음이 같은 토큰을 받으면 넣지 않은 것을 넣었다고 본다",
        )

    def test_the_already_ingested_marker_rides_in_the_write_commit(self) -> None:
        """표시는 데이터와 같은 커밋에 실려야 한다.

        Iceberg 는 `snapshot-property.<키>` 로 준 값을 그 쓰기가 만드는 스냅숏 요약에 함께
        커밋한다. 이 옵션 대신 파일이나 다른 표에 표시를 남기면 커밋이 둘로 갈라지고,
        그 사이에서 죽은 실행이 재시도 때 중복을 만든다.
        """
        body = function_body(job_source(), "write_silver_iceberg")

        self.assertIn(
            'f"snapshot-property.{INGEST_BATCH_TOKEN_KEY}"',
            body,
            "쓰기가 스냅숏 요약에 토큰을 같이 커밋해야 한다",
        )
        self.assertIn(
            ".writeTo(",
            body,
            "INSERT INTO 는 snapshot-property 를 받지 못한다 — DataFrame writer 여야 한다",
        )

    def test_the_skip_decision_reads_the_table_not_a_file_beside_it(self) -> None:
        """이미 넣었는지는 표의 커밋 기록에서 답이 나와야 한다.

        적재기 옆의 마커 파일은 표와 다른 매체에 따로 커밋된 두 번째 사실이라, 실행이 그
        사이에서 죽으면 둘이 어긋난다. 표의 스냅숏 요약을 읽으면 데이터와 같은 곳을 본다.
        """
        body = function_body(job_source(), "find_committed_batch_snapshot")

        self.assertIn(".snapshots", body, "판단 근거는 표 자신의 스냅숏 요약이어야 한다")
        self.assertIn("summary", body, "판단 근거는 표 자신의 스냅숏 요약이어야 한다")
        self.assertNotIn(
            "open(",
            body,
            "표시를 파일에서 읽으면 데이터와 다른 곳에 커밋된 사실을 믿는 것이다",
        )
        self.assertTrue(
            INGEST_BATCH_TOKEN_KEY.startswith("foundation."),
            "요약 키는 엔진 예약어와 섞이지 않게 우리 이름공간에 둔다",
        )

    def test_the_write_path_asks_before_it_appends(self) -> None:
        """쓰기 앞에 확인이 있어야 한다.

        확인 없이 붙이면 토큰은 "무엇이 들어갔는지"를 기록만 할 뿐 두 번째 적재를 막지
        못한다. 막는 것은 붙이기 전에 묻는 이 순서다.
        """
        source = job_source()
        body = function_body(source, "main")

        self.assertIn("find_committed_batch_snapshot(", body, "붙이기 전에 물어야 한다")
        self.assertLess(
            body.index("find_committed_batch_snapshot("),
            body.index("write_silver_iceberg("),
            "확인이 쓰기보다 먼저 와야 막을 수 있다",
        )


if __name__ == "__main__":
    unittest.main()
