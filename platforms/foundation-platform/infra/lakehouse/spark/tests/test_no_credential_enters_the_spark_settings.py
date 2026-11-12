"""자격증명은 Spark 설정에 값으로 들어가지 않는다. 설정은 그것이 있는 환경변수의 이름만 댄다.

2026-11-11 23:34Z..23:38Z, 실거래 Gold 의 run 로그에 카탈로그 쓰기 토큰이 일곱 번 찍혔다.
Spark 는 `spark.*` SQL 설정을 모든 작업의 속성으로 복사하고(SQLExecution.withSQLConfPropagated),
리스너가 작업 시작 이벤트를 1초 넘게 처리하면 그 속성을 가리지 않고 로그에 쓴다(ListenerBus 는
환경 갱신 이벤트만 가린다). 그 파일은 호스트의 모든 계정이 읽을 수 있었다.

로그를 가리는 것은 찍는 곳마다 따로 막아야 한다. 값이 설정에 없으면 찍을 것이 없다:
Iceberg REST 카탈로그는 `env:NAME` 을(RESTSessionCatalog.initialize → EnvironmentUtil.resolveAll),
Hadoop 은 `${env.NAME}` 을(Configuration.substituteVars) JVM 환경에서 읽는다.

`python3 -m unittest discover` 로 수집되므로 `unittest.TestCase` 여야 한다.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import lakehouse_engine  # noqa: E402
import lakehouse_object_store  # noqa: E402

# 이름이 자격증명처럼 생긴 변수(scripts/deploy/deploy_steps.py SECRET_NAME 과 같은 어휘).
CREDENTIAL_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|PASSPHRASE|CREDENTIAL|_KEY(_|$)|ACCESS_KEY")


class Recorder:
    """환경을 대신한다: 물어 온 이름마다 그 이름으로만 알아볼 수 있는 값을 준다."""

    def __init__(self) -> None:
        self.asked: dict[str, str] = {}

    def __call__(self, name: str) -> str:
        return self.asked.setdefault(name, f"value-of-{name}-{len(self.asked)}")


def assembled() -> list[tuple[str, dict[str, str], Recorder]]:
    catalog, store = Recorder(), Recorder()
    return [
        ("catalog_settings", lakehouse_engine.catalog_settings("r2", catalog), catalog),
        ("object_store_settings", lakehouse_object_store.object_store_settings(store), store),
    ]


class NoCredentialEntersTheSparkSettings(unittest.TestCase):
    def test_no_setting_carries_the_value_of_a_credential(self) -> None:
        for owner, settings, environment in assembled():
            credentials = {name: value for name, value in environment.asked.items()
                           if CREDENTIAL_NAME.search(name)}
            self.assertTrue(credentials, f"{owner} asked for no credential; this test checks nothing")
            for name, value in credentials.items():
                carriers = [key for key, setting in settings.items() if value in setting]
                self.assertEqual(carriers, [], f"{owner}: {carriers} carry the value of {name}")

    def test_the_catalog_token_is_named_the_way_iceberg_resolves_it(self) -> None:
        settings = lakehouse_engine.catalog_settings("r2", Recorder())
        self.assertEqual(settings["spark.sql.catalog.r2.token"], f"env:{lakehouse_engine.CATALOG_TOKEN_ENV}")

    def test_the_s3a_key_pair_is_named_the_way_hadoop_resolves_it(self) -> None:
        settings = lakehouse_object_store.object_store_settings(Recorder())
        self.assertEqual(settings["spark.hadoop.fs.s3a.access.key"],
                         "${env." + lakehouse_object_store.ACCESS_KEY_ENV + "}")
        self.assertEqual(settings["spark.hadoop.fs.s3a.secret.key"],
                         "${env." + lakehouse_object_store.SECRET_KEY_ENV + "}")

    def test_a_named_credential_must_still_be_set(self) -> None:
        """이름만 대더라도, 없는 변수는 Spark 가 뜨기 전에 그 이름으로 실패한다."""
        with self.assertRaises(ValueError) as caught:
            lakehouse_engine.catalog_settings(
                "r2", lambda name: "" if name == lakehouse_engine.CATALOG_TOKEN_ENV else "set")
        self.assertIn(lakehouse_engine.CATALOG_TOKEN_ENV, str(caught.exception))

    def test_no_job_sets_a_credential_on_its_builder(self) -> None:
        """두 모듈 밖에서 자격증명 이름의 설정 키를 적는 job 은 값을 직접 싣는 것이다."""
        spelled = re.compile(r"""["'](spark\.[A-Za-z0-9_.{}-]*(?:token|secret|password|access\.key)[A-Za-z0-9_.-]*)["']""")
        owners = {"lakehouse_engine.py", "lakehouse_object_store.py"}
        offenders = [f"{path.name}: {match}" for path in sorted(JOBS_DIR.glob("*.py")) if path.name not in owners
                     for match in spelled.findall(path.read_text(encoding="utf-8"))]
        self.assertEqual(offenders, [], "Spark 설정에 자격증명을 싣는 곳은 두 모듈뿐이고, 둘은 이름만 댄다")


if __name__ == "__main__":
    unittest.main()
