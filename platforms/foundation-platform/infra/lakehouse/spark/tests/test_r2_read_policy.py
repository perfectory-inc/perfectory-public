"""R2 읽기 재시도는 엔진 계약의 `r2_read` 한 곳에서 나온다 (루트 ADR-0102).

Iceberg S3FileIO 카탈로그 설정, Hadoop s3a 설정, Spark 마스터 URL 이 같은 숫자를 쓴다. 숫자를
어느 한쪽에 다시 적으면 그쪽만 바뀌어 한 경로가 다시 약해진다.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import lakehouse_engine  # noqa: E402
import lakehouse_object_store  # noqa: E402

CATALOG_ENV = {
    lakehouse_engine.CATALOG_URI_ENV: "https://catalog.example.invalid",
    lakehouse_engine.CATALOG_WAREHOUSE_ENV: "warehouse",
    lakehouse_engine.CATALOG_TOKEN_ENV: "token",
}
OBJECT_STORE_ENV = {
    lakehouse_object_store.ENDPOINT_ENV: "https://r2.example.invalid",
    lakehouse_object_store.ACCESS_KEY_ENV: "reader",
    lakehouse_object_store.SECRET_KEY_ENV: "secret",
}


class OnePolicy(unittest.TestCase):
    def setUp(self):
        self.policy = lakehouse_engine.r2_read_policy()

    def test_the_catalog_settings_carry_the_contract_numbers(self):
        settings = lakehouse_engine.catalog_settings("r2", CATALOG_ENV.get)
        self.assertEqual(settings["spark.sql.catalog.r2.s3.retry.num-retries"], str(self.policy["num_retries"]))
        self.assertEqual(settings["spark.sql.catalog.r2.s3.retry.min-wait-ms"], str(self.policy["min_wait_ms"]))
        self.assertEqual(settings["spark.sql.catalog.r2.s3.retry.max-wait-ms"], str(self.policy["max_wait_ms"]))
        self.assertEqual(settings["spark.sql.catalog.r2.http-client.apache.connection-time-to-live-ms"],
                         str(self.policy["connection_ttl_ms"]))

    def test_an_s3a_read_retries_as_the_catalog_does(self):
        """s3a 읽기(Silver 넘김 JSONL)는 2026-10-16 까지 Hadoop 기본값으로 돌았다."""
        settings = lakehouse_object_store.object_store_settings(OBJECT_STORE_ENV.get)
        self.assertEqual(settings["spark.hadoop.fs.s3a.retry.limit"], str(self.policy["num_retries"]))
        self.assertEqual(settings["spark.hadoop.fs.s3a.attempts.maximum"], str(self.policy["num_retries"]))
        self.assertEqual(settings["spark.hadoop.fs.s3a.retry.interval"], f"{self.policy['min_wait_ms']}ms")

    def test_the_master_lets_a_failed_task_run_again(self):
        self.assertEqual(lakehouse_engine.spark_master(8), f"local[8,{self.policy['task_max_failures']}]")
        self.assertRegex(lakehouse_engine.spark_master("*"), r"^local\[\*,\d+\]$")

    def test_a_contract_without_task_retries_is_refused(self):
        contract = json.loads(Path(lakehouse_engine.DEFAULT_CONTRACT_PATH).read_text(encoding="utf-8"))
        contract["r2_read"]["task_max_failures"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "engine.json"
            path.write_text(json.dumps(contract), encoding="utf-8")
            with mock.patch.dict(os.environ, {lakehouse_engine.CONTRACT_PATH_ENV: str(path)}):
                with self.assertRaises(ValueError):
                    lakehouse_engine.r2_read_policy()


if __name__ == "__main__":
    unittest.main()
