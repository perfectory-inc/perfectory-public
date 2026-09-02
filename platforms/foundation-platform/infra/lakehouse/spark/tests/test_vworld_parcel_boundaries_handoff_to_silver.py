"""핸드오프 잡이 다시 돌아도 같은 결과가 되게 하는 두 가지를 고정한다.

**적재 후 읽기 범위.** 이 잡들은 쓰고 나서 표를 다시 읽어 확인한다. 읽기 범위가 이번
실행보다 넓으면 앞선 실행이 넣은 행까지 딸려 와 유일성 검사가 "같은 PNU 가 여러 개"라고
보고한다. 제대로 쓴 실행이 실패로 보고되고, 재시도가 같은 파일을 두 번 넣어 **진짜**
중복을 만든다. 2026-08-27 에 실제로 그렇게 됐다.

**이미 넣었다는 표시가 있는 곳.** 위를 고쳐도, 표시가 데이터와 다른 곳에 커밋되면
그 사이에서 죽은 실행이 둘을 어긋나게 둔다. 같은 날 그 틈으로 세 번 들어가 필지
1,865,891건이 표에 세 벌 쌓였다. 표시는 데이터와 같은 커밋에 있어야 한다 — Iceberg 의
Flink 싱크가 `flink.max-committed-checkpoint-id` 를 스냅숏 요약에 넣는 이유와 같다.

**2026-09-02 부터 이 규칙들은 `spatial_silver_handoff` 에 있다.** 그전에는 필지 잡과 산단
잡이 각자 사본을 갖고 있었고, 이름이 같은 함수 서른 개 중 글자까지 같은 것은 다섯 개뿐이었다.
그래서 이 파일은 잡의 본문 대신 공용 모듈의 본문을 읽는다 — 검사가 약해지는 게 아니라
**두 잡을 한 번에 덮는다.** 잡이 자기 사본을 되만드는 경우는 아래에서 따로 막는다.

이 파일은 `unittest.TestCase` 여야 한다. 이 디렉터리의 러너는 xtask 의
`python3 -m unittest discover -s infra/lakehouse/spark/tests` 이고, unittest 는 모듈
수준의 `def test_*` 함수를 수집하지 않는다. 앞선 판(pytest 형식)은 CI 에서 0개가 수집돼
검사가 하나도 없는 채로 초록이었다.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

# 잡이 찍는 줄을 읽는 쪽. 둘을 잇는 검사가 없어서 줄 모양을 바꿨다가 성공한 적재가
# 아무 결과도 아닌 것으로 읽힐 뻔했다.
LOADER_SCRIPT = Path(__file__).resolve().parents[4] / "scripts/load/lakehouse-batch-load.sh"

# 이 import 가 곧 검사다. 잡이 pyspark 를 모듈 최상단에서 부르면 여기서 터지고, CI 레인에는
# pyspark 가 없으므로 아래 검사가 전부 조용히 건너뛰어진다. 앞선 판이 정확히 그랬다.
from vworld_parcel_boundaries_handoff_to_silver import (  # noqa: E402
    SILVER_COLUMNS,
)
from spatial_silver_handoff import HandoffLabels, outcome_line  # noqa: E402

# 적재 규칙은 잡이 아니라 공용 모듈이 정본이다(2026-08-31). 잡마다 같은 규칙을 다시 적으면
# 잡마다 달라지고, 실제로 네 잡이 SQL 로 붙이면서 기록을 남길 자리조차 없었다.
from lakehouse_ingest import MAX_BATCH_SOURCE_RECORDS  # noqa: E402

# 두 핸드오프 잡이 공유하는 규칙이 사는 곳. 잡 두 개가 각각 통과하는 것보다, 규칙 하나가
# 통과하는 것이 지키려는 것에 가깝다.
JOB_PATHS = {
    "parcel": JOBS_DIR / "vworld_parcel_boundaries_handoff_to_silver.py",
    "complex": JOBS_DIR / "industrial_complex_boundaries_handoff_to_silver.py",
}
INGEST_PATH = JOBS_DIR / "lakehouse_ingest.py"
SHARED_PATH = JOBS_DIR / "spatial_silver_handoff.py"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


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
        body = function_body(read(SHARED_PATH), "read_iceberg_snapshot_for_batch")

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
        limit = MAX_BATCH_SOURCE_RECORDS

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
        body = function_body(read(INGEST_PATH), "append_batch_once")

        self.assertIn(
            "snapshot_property_options(",
            body,
            "쓰기가 스냅숏 요약에 기록을 같이 커밋해야 한다",
        )
        self.assertIn(
            ".writeTo(",
            body,
            "SQL 의 INSERT 는 snapshot-property 를 받지 못한다 — DataFrame writer 여야 한다",
        )

    def test_the_write_path_asks_before_it_appends(self) -> None:
        """쓰기 앞에 확인이 있어야 한다.

        확인 없이 붙이면 토큰은 "무엇이 들어갔는지"를 기록만 할 뿐 두 번째 적재를 막지
        못한다. 막는 것은 붙이기 전에 묻는 이 순서다.
        """
        body = function_body(read(INGEST_PATH), "append_batch_once")

        self.assertIn("read_ingested_objects(", body, "붙이기 전에 물어야 한다")
        self.assertIn("decide_whether_to_append(", body, "답을 판단에 써야 한다")
        self.assertLess(
            body.index("decide_whether_to_append("),
            body.index(".writeTo("),
            "확인이 쓰기보다 먼저 와야 막을 수 있다",
        )

    def test_the_shared_write_path_uses_the_common_decision(self) -> None:
        """붙이는 결정은 공용 함수 하나여야 한다.

        2026-08-31 에 필지 잡만 이 규칙을 갖고 있었고 나머지 넷은 SQL 로 붙어서 기록을 실을
        자리조차 없었다.
        """
        body = function_body(read(SHARED_PATH), "append_and_read_back")

        self.assertIn("append_batch_once(", body, "공용 결정 함수를 써야 한다")

    def test_no_job_keeps_its_own_copy_of_the_rule(self) -> None:
        """잡이 사본을 되만들면 규칙이 다시 갈라진다.

        두 잡을 다 본다. 한 잡만 검사하면 다른 잡이 사본을 가진 채 초록이고, 그것이 바로
        이 두 잡이 서로 갈라진 방식이다.
        """
        for label, path in JOB_PATHS.items():
            source = read(path)
            with self.subTest(job=label):
                self.assertIn(
                    "shared.append_and_read_back(",
                    source,
                    "쓰기·되읽기는 공용 함수에 맡겨야 한다",
                )
                self.assertNotIn(
                    "snapshot_property_options(",
                    source,
                    "옵션 조립을 잡에서 다시 하면 정본이 둘이 된다",
                )
                self.assertNotIn(
                    "def batch_source_record_ids",
                    source,
                    "배치 식별을 잡에서 다시 정의하면 정본이 둘이 된다",
                )
                self.assertNotIn(
                    'F.col("source_record_id").isin(',
                    source,
                    "되읽기 술어를 잡에 다시 적으면 정본이 둘이 된다",
                )


class ReadableAfterWriteTest(unittest.TestCase):
    """이 표는 쓰는 것으로 끝이 아니라 읽혀야 한다.

    이 잡들은 쓴 것을 되읽어 검사하고, 이미 들어간 묶음을 건너뛸 때도 행을 센다. 둘 다 파일을
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
        body = function_body(read(SHARED_PATH), "append_and_read_back")

        self.assertIn(
            "create_iceberg_table_if_missing(", body, "쓰기·되읽기 경로가 표를 준비해야 한다"
        )
        self.assertLess(
            body.index("create_iceberg_table_if_missing("),
            body.index("read_iceberg_snapshot_for_batch("),
            "준비가 어떤 읽기보다도 먼저 와야 한다",
        )


class SharedGateCoversBothJobsTest(unittest.TestCase):
    """품질 판정은 두 잡이 같은 구현을 써야 한다.

    2026-09-02 실측: 두 잡의 `assert_quality_metrics` 는 75줄 대 72줄이었고 41줄이 달랐다.
    같은 이름의 서로 다른 검사가 두 데이터셋을 판정하고 있었다.
    """

    def test_neither_job_defines_the_geometry_rules_itself(self) -> None:
        for label, path in JOB_PATHS.items():
            source = read(path)
            with self.subTest(job=label):
                for rule in (
                    "def geometry_wkb_is_invalid",
                    "def geometry_wkb_hex_is_invalid",
                    "def bbox_is_invalid",
                    "def checksum_is_invalid",
                    "def is_invalid_double",
                ):
                    self.assertNotIn(
                        rule,
                        source,
                        f"{rule} 는 공용 모듈에 있어야 한다 — 잡에 두면 잡마다 달라진다",
                    )

    def test_the_shared_module_holds_them(self) -> None:
        source = read(SHARED_PATH)

        for rule in (
            "def geometry_wkb_is_invalid",
            "def geometry_wkb_hex_is_invalid",
            "def bbox_is_invalid",
            "def checksum_is_invalid",
            "def is_invalid_double",
            "def assert_geometry",
        ):
            self.assertIn(rule, source, f"{rule} 가 공용 모듈에서 사라지면 잡들이 사본을 만든다")

    def test_failure_samples_never_carry_the_geometry_blobs(self) -> None:
        """실패 표본에 WKB 를 실으면 무엇이 실패했는지가 안 보인다.

        경계 하나의 WKB 는 수십 KB 의 hex 다. 산단 잡은 이걸 배웠고 필지 잡은 못 배웠는데,
        행이 3,986만인 쪽은 필지였다. 사본이 갈라지면 고침이 한쪽에만 남는다.
        """
        source = read(SHARED_PATH)
        body = function_body(source, "sample_invalid_rows")

        self.assertIn("SAMPLE_SUPPRESSED_COLUMNS", body, "표본은 기하 칸을 빼고 찍어야 한다")
        for column in ("geometry_wkb", "geometry_wkb_hex", "_geometry_wkb_hex"):
            self.assertIn(f'"{column}"', source, f"{column} 이 표본에서 빠져야 한다")


class TheLoaderCanReadWhatTheJobPrintsTest(unittest.TestCase):
    """적재기가 로그에서 결과를 읽는다. 잡이 줄 모양을 바꾸면 그 읽기가 조용히 빗나간다.

    `lakehouse-batch-load.sh` 는 `<라벨> rows=<숫자>` 를 정규식으로 찾아 한 묶음이 들어갔는지
    건너뛰었는지 판단한다. 2026-09-02 리팩터링에서 `rows=` 를 뒤로 옮겼다가, 성공한 적재가
    아무 결과도 아닌 것으로 읽히게 만들었다 — 그때 둘을 잇는 검사가 없었다.

    그래서 이 검사는 스크립트에서 정규식을 **읽어서** 잡이 실제로 만드는 줄에 걸어 본다.
    여기에 정규식을 베껴 적으면 사본이 하나 더 생길 뿐이다.
    """

    def test_the_script_pattern_matches_every_line_the_job_prints(self) -> None:
        script = LOADER_SCRIPT.read_text(encoding="utf-8")
        match = re.search(r'grep -aoE "([^"]+)"', script)
        self.assertIsNotNone(match, "적재기에서 결과를 찾는 정규식을 못 찾았다")
        pattern = re.compile(match.group(1))

        labels = HandoffLabels("silver-parcel-boundaries")
        for label in (labels.validate_ok, labels.iceberg_write_ok, labels.already_ingested):
            line = outcome_line(label, 1344, "table=silver.parcel_boundaries")
            with self.subTest(label=label):
                self.assertRegex(
                    line, pattern, f"적재기가 이 줄에서 결과를 읽지 못한다: {line!r}"
                )

    def test_the_pattern_would_reject_a_line_with_the_count_moved(self) -> None:
        """이 검사가 무엇이든 통과시키는 것이 아님을 보인다."""
        script = LOADER_SCRIPT.read_text(encoding="utf-8")
        pattern = re.compile(re.search(r'grep -aoE "([^"]+)"', script).group(1))
        moved = "silver-parcel-boundaries-iceberg-write-ok target=table=x rows=1344"

        self.assertNotRegex(
            moved, pattern, "순서를 바꾼 줄도 통과하면 이 검사는 아무것도 지키지 않는다"
        )


class ContractShapeTest(unittest.TestCase):
    def test_the_job_reads_its_columns_off_the_contract(self) -> None:
        """칸 목록은 계약에서 온다. 잡에 적으면 계약과 갈라진다."""
        self.assertIn("pnu", SILVER_COLUMNS)
        self.assertIn("geometry_wkb", SILVER_COLUMNS)
        self.assertIn("source_record_id", SILVER_COLUMNS)


if __name__ == "__main__":
    unittest.main()
