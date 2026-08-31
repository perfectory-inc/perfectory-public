"""`s3a://` 입력을 열기 위한 설정이 지켜야 할 것.

이 설정들은 틀려도 대부분 조용하다. 잘못된 자격증명은 접근 거부로, 빠진 경로 방식은 DNS 오류로
나타나며, 둘 다 "왜 안 되는지"를 말해 주지 않는다. 그래서 값을 여기서 못 박는다.

`python3 -m unittest discover` 로 수집되므로 `unittest.TestCase` 여야 한다. 모듈 수준의
`def test_*` 함수는 수집되지 않고, 수집되지 않은 검사는 통과한 검사와 같은 초록을 보고한다.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from lakehouse_object_store import (  # noqa: E402
    ACCESS_KEY_ENV,
    ENDPOINT_ENV,
    SECRET_KEY_ENV,
    is_object_store_path,
    object_store_settings,
)


def fake_lookup(**overrides: str):
    values = {
        ENDPOINT_ENV: "https://example.r2.cloudflarestorage.test",
        ACCESS_KEY_ENV: "fixture-access-key",
        SECRET_KEY_ENV: "fixture-secret-key",
    }
    values.update(overrides)
    return values.get


class ObjectStorePathTest(unittest.TestCase):
    def test_only_an_s3a_input_asks_for_object_store_settings(self) -> None:
        """로컬 경로 실행이 버킷 자격증명을 요구하면 안 된다.

        요구하면, 버킷을 건드리지도 않는 실행이 없는 환경변수 때문에 실패한다.
        """
        self.assertTrue(is_object_store_path("s3a://bucket/handoff/*.jsonl"))
        self.assertFalse(is_object_store_path("/workspace/target/lakehouse/batch-0/*.jsonl"))
        self.assertFalse(is_object_store_path("s3://bucket/x.jsonl"))


class ObjectStoreSettingsTest(unittest.TestCase):
    def test_r2_needs_path_style_addressing(self) -> None:
        """기본값(하위 도메인 방식)이면 없는 호스트로 요청이 가고 DNS 오류가 난다."""
        settings = object_store_settings(fake_lookup())

        self.assertEqual(settings["spark.hadoop.fs.s3a.path.style.access"], "true")

    def test_the_region_is_fixed_because_there_is_nothing_to_choose(self) -> None:
        """R2 는 지역이 하나이고, 다른 지역으로 계산한 서명을 거부한다."""
        settings = object_store_settings(fake_lookup())

        self.assertEqual(settings["spark.hadoop.fs.s3a.endpoint.region"], "auto")

    def test_it_uses_the_reader_credentials(self) -> None:
        """읽기 경로에 쓰기 권한을 주지 않는다.

        이 통로로 여는 것은 적재의 입력뿐이고, 같은 버킷에 모든 표가 들어 있다.
        """
        self.assertIn("READER", ACCESS_KEY_ENV)
        self.assertIn("READER", SECRET_KEY_ENV)
        self.assertNotIn("WRITER", ACCESS_KEY_ENV)
        self.assertNotIn("WRITER", SECRET_KEY_ENV)

    def test_a_missing_credential_names_itself(self) -> None:
        """없는 값은 접근 거부가 아니라 이름을 말하며 실패해야 한다.

        Spark 가 나중에 던지는 오류는 변수 이름도 버킷 이름도 말해 주지 않는다.
        """
        for missing in (ENDPOINT_ENV, ACCESS_KEY_ENV, SECRET_KEY_ENV):
            with self.subTest(missing=missing):
                with self.assertRaises(ValueError) as caught:
                    object_store_settings(fake_lookup(**{missing: ""}))
                self.assertIn(missing, str(caught.exception))

    def test_every_setting_is_a_spark_hadoop_key(self) -> None:
        """`fs.s3a.*` 는 Hadoop 설정이라 `spark.hadoop.` 접두사가 있어야 전달된다.

        접두사가 없으면 Spark 는 그 값을 자기 설정으로 받아 두고 Hadoop 에 넘기지 않는다.
        조용히 무시되고, 실행은 자격증명 없이 시도한다.
        """
        for key in object_store_settings(fake_lookup()):
            self.assertTrue(key.startswith("spark.hadoop.fs.s3a."), key)


if __name__ == "__main__":
    unittest.main()
