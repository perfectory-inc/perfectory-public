"""백필이 지켜야 할 것.

기록 없는 표에 안전장치를 되살리는 조작이다. 행을 더하지 않고 커밋 하나만 남긴다.
잘못 돌면 1억 3천만 행짜리 표에 틀린 기록이 붙고, 그 뒤로는 안전장치가 틀린 것을 믿는다.

`python3 -m unittest discover` 로 수집되므로 `unittest.TestCase` 여야 한다.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

from lakehouse_engine import catalog_settings, required_catalog_env  # noqa: E402
from lakehouse_registry_backfill import backfill, identities_in_table, parse_args  # noqa: E402


class FakeRow(dict):
    """Spark 의 Row 는 `row["x"]` 와 `row.x` 를 둘 다 받는다.

    코드가 어느 쪽을 쓰는지 흉내가 정하면 안 된다. 둘 중 하나만 되게 만들면, 코드가 다른
    쪽으로 바뀔 때 시험이 실물과 다른 것을 검사하게 된다.
    """

    def __getattr__(self, name: str):  # noqa: ANN204
        try:
            return self[name]
        except KeyError as error:
            raise AttributeError(name) from error


class FakeFrame:
    def __init__(self, values: list[str], column: str) -> None:
        self._values = values
        self._column = column
        self.appended = False
        self.options: dict[str, str] = {}

    def select(self, *_columns: str) -> "FakeFrame":
        return self

    def distinct(self) -> "FakeFrame":
        return self

    def limit(self, _n: int) -> "FakeFrame":
        return self

    def collect(self) -> list[FakeRow]:
        return [FakeRow({self._column: value}) for value in self._values]

    def writeTo(self, _table: str) -> "FakeFrame":  # noqa: N802 - Spark 의 철자
        return self

    def option(self, key: str, value: str) -> "FakeFrame":
        self.options[key] = value
        return self

    def append(self) -> None:
        self.appended = True


class FakeSpark:
    """행 존재 여부와 기록을 따로 답한다. 이 둘이 다른 답이라는 것이 백필의 전제다."""

    def __init__(self, frame: FakeFrame, total_records: int, registry: dict[str, int]) -> None:
        self._frame = frame
        self._total = total_records
        self._registry = registry
        self.catalog = self
        self._pending: list = []
        self.recorded_after_write: dict[str, int] | None = None

    def tableExists(self, _name: str) -> bool:  # noqa: N802 - Spark 의 철자
        return True

    def table(self, _name: str) -> FakeFrame:
        return self._frame

    def sql(self, query: str):  # noqa: ANN201
        if "refs" in query:
            self._pending = [FakeRow({"summary": {"total-records": str(self._total)}})]
        else:
            registry = self._registry if not self._frame.appended else self._after_write()
            self._pending = [
                FakeRow({"snapshot_id": 1, "summary": {"foundation.ingest-batch-objects": names}})
                for names in ([",".join(registry)] if registry else [])
            ]
        return self

    def _after_write(self) -> dict[str, int]:
        written = self._frame.options.get(
            "snapshot-property.foundation.ingest-batch-objects", ""
        )
        return {name: 1 for name in written.split(",") if name}

    def collect(self) -> list[FakeRow]:
        return self._pending


def make_args(table: str, apply: bool) -> object:
    argv = ["--table", table]
    if apply:
        argv.append("--apply")
    return parse_args(argv)


class BackfillTest(unittest.TestCase):
    def test_it_records_what_the_table_holds_without_adding_a_row(self) -> None:
        """기록은 표에서 읽어 온다. 여기 손으로 적으면 그것이 세 번째 사실이 된다."""
        frame = FakeFrame(
            [
                "foundation-platform:bronze:bronze/source=vworldkr__sandan_profile/x.zip#1",
                "foundation-platform:bronze:bronze/source=vworldkr__sandan_profile/x.zip#2",
            ],
            "source_record_id",
        )
        spark = FakeSpark(frame, total_records=1_442, registry={})

        result = backfill(spark, make_args("silver.industrial_complexes", apply=True))

        self.assertTrue(result["recorded"])
        # 1,442개 값이 객체 하나로 줄어야 한다. 안 줄면 한 묶음 상한을 넘겨 거부된다.
        self.assertEqual(
            result["identities"], ["bronze/source=vworldkr__sandan_profile/x.zip"]
        )
        self.assertTrue(frame.appended)

    def test_it_refuses_a_table_that_already_records_something(self) -> None:
        """두 번 적으면 어느 쪽이 진짜인지 아무도 모른다."""
        frame = FakeFrame(["a.zip"], "source_record_id")
        spark = FakeSpark(frame, total_records=5, registry={"a.zip": 1})

        with self.assertRaisesRegex(ValueError, "already records"):
            backfill(spark, make_args("silver.parcel_boundaries", apply=True))

        self.assertFalse(frame.appended)

    def test_it_refuses_an_empty_table(self) -> None:
        """행이 없으면 기록할 사실이 없다. 빈 기록을 남기면 다음 적재가 그것을 믿는다."""
        frame = FakeFrame(["a.zip"], "source_record_id")
        spark = FakeSpark(frame, total_records=0, registry={})

        with self.assertRaisesRegex(ValueError, "holds no rows"):
            backfill(spark, make_args("silver.parcel_boundaries", apply=True))

    def test_it_refuses_a_derived_table(self) -> None:
        """파생 표는 덮어쓴다. 비교할 것이 없는 표에 기록을 남기면 뜻이 없다."""
        frame = FakeFrame(["x"], "source_snapshot_id")
        spark = FakeSpark(frame, total_records=1_442, registry={})

        with self.assertRaisesRegex(ValueError, "derived"):
            backfill(spark, make_args("gold.complex_catalog", apply=True))

    def test_without_apply_it_writes_nothing(self) -> None:
        """1억 행짜리 표를 건드리기 전에 무엇을 적을지 볼 수 있어야 한다."""
        frame = FakeFrame(["a.zip"], "source_record_id")
        spark = FakeSpark(frame, total_records=5, registry={})

        result = backfill(spark, make_args("silver.parcel_boundaries", apply=False))

        self.assertFalse(result["recorded"])
        self.assertFalse(frame.appended)

    def test_the_empty_frame_is_the_table_not_the_contract(self) -> None:
        """빈 프레임은 표의 스키마를 그대로 써야 한다.

        `silver.building_register_unit_areas` 는 칸이 31개이고 계약은 32개다 —
        `source_record_id` 가 선언돼 있지만 아직 표에 없다. 계약의 칸을 고르면 없는 칸을
        고르다 죽고, 죽는 자리는 1억 3천만 행짜리 표를 열고 난 뒤다.
        """
        source = (
            SPARK_DIR / "jobs" / "lakehouse_registry_backfill.py"
        ).read_text(encoding="utf-8")
        body = source.split("def backfill(", 1)[1]

        self.assertIn("empty = frame.limit(0)", body)
        self.assertNotIn(
            "column_names(", body, "계약의 칸을 고르면 표에 없는 칸을 고르게 된다"
        )

    def test_more_identities_than_one_record_can_carry_is_an_error(self) -> None:
        """상한을 넘겨 잘리면, 기록은 적재보다 적은 객체를 말하게 된다."""
        frame = FakeFrame([f"{n}.zip" for n in range(200)], "source_record_id")

        with self.assertRaisesRegex(ValueError, "at most"):
            identities_in_table(frame, "silver.parcel_boundaries", "source_record_id")


class CatalogSettingsTest(unittest.TestCase):
    """카탈로그 설정은 한 곳에서 온다.

    2026-09-01 실측: 여덟 개 job 이 같은 설정을 각자 들고 있었고, 두 곳이 나머지와 달랐다.
    한 곳(`spatial_tile_publication_wap`)은 `oauth2-server-uri` 를 일부러 뺀 것이었고 그것이
    맞았다 — 실물에서 그 설정 없이 카탈로그도 데이터 파일도 열린다. 다른 한 곳
    (`industrial_complex_boundaries_silver_to_postgis_handoff`)은 `s3.remote-signing-enabled`
    가 빠져 있었고 그것을 설명하는 것은 아무 데도 없었다.
    """

    ENV = {
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI": "https://example.invalid/acct/wh",
        "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": "acct_wh",
        "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN": "not-a-real-token",
    }

    def test_it_carries_every_setting_a_job_needs(self) -> None:
        with patch.dict(os.environ, self.ENV, clear=False):
            settings = catalog_settings("lakehouse")

        for key in (
            "spark.sql.extensions",
            "spark.sql.catalog.lakehouse",
            "spark.sql.catalog.lakehouse.type",
            "spark.sql.catalog.lakehouse.uri",
            "spark.sql.catalog.lakehouse.warehouse",
            "spark.sql.catalog.lakehouse.token",
            "spark.sql.catalog.lakehouse.header.X-Iceberg-Access-Delegation",
            "spark.sql.catalog.lakehouse.s3.remote-signing-enabled",
        ):
            with self.subTest(setting=key):
                self.assertIn(key, settings)

    def test_the_token_endpoint_is_only_set_when_configured(self) -> None:
        """정적 토큰 방식에는 발급처가 필요 없다. 2026-09-01 실물 확인.

        발급처를 빼고 세션을 만들어 `silver.industrial_complex_boundaries` 의 스냅숏 2개를
        읽고 데이터 파일에서 행 1개를 꺼냈다. 여섯 개 job 이 카탈로그 주소에서 만들어 넣고
        있었는데, REST 클라이언트가 정적 토큰이 있으면 그 주소를 부르지 않아 무해했을 뿐이다.
        """
        with patch.dict(os.environ, self.ENV, clear=True):
            settings = catalog_settings("lakehouse")
        self.assertNotIn("spark.sql.catalog.lakehouse.oauth2-server-uri", settings)

        with patch.dict(
            os.environ,
            {**self.ENV, "FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI": "https://other/tok"},
            clear=True,
        ):
            override = catalog_settings("lakehouse")[
                "spark.sql.catalog.lakehouse.oauth2-server-uri"
            ]
        self.assertEqual(override, "https://other/tok")

    def test_the_precondition_list_is_asked_of_the_settings(self) -> None:
        """사전 점검 목록은 설정을 읽어서 만든다. 옆에 적으면 그것이 두 번째 사실이 된다.

        2026-09-01 실측: 일곱 개 job 이 이 목록을 손으로 들고 있었다. 설정이 하나 늘어도
        일곱 곳 중 어디도 그 사실을 알지 못한다.
        """
        names = required_catalog_env("lakehouse")

        with patch.dict(os.environ, self.ENV, clear=True):
            settings = catalog_settings("lakehouse")
        for name in names:
            with self.subTest(name=name):
                self.assertTrue(
                    any(self.ENV[name] == value for value in settings.values()),
                    f"{name} 은 필수라면서 설정 어디에도 안 쓰인다",
                )
        self.assertNotIn(
            "FOUNDATION_PLATFORM_LAKEHOUSE_OAUTH2_SERVER_URI",
            names,
            "선택 변수는 필수 목록에 들어가면 안 된다",
        )

    def test_a_missing_variable_is_named(self) -> None:
        """비면 그 자리에서 이름을 말해야 한다. 빈 문자열로 붙으면 실패가 훨씬 뒤에서 난다."""
        with patch.dict(os.environ, {**self.ENV, "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE": ""}):
            with self.assertRaisesRegex(ValueError, "WAREHOUSE"):
                catalog_settings("lakehouse")


if __name__ == "__main__":
    unittest.main()
